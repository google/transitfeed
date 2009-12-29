#!/usr/bin/python2.4

# Copyright (C) 2008 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Imports Zurich timetables, converting them from DIVA export format
to Google Transit format."""

# This was written before transitfeed.py and we haven't yet found the
# motivation to port it. Please see the examples directory for better
# examples.

import cStringIO
import csv
import datetime
import optparse
import sys
import urllib
import zipfile


# Zurich tram lines
TRAM_LINES = {'2':['FF3300','FFFFFF'],
              '3':['009933','FFFFFF'],
              '4':['333399','FFFFFF'],
              '5':['996600','FFFFFF'],
              '6':['CC9933','FFFFFF'],
              '7':['000000','FFFFFF'],
              '8':['99CC00','000000'],
              '9':['333399','FFFFFF'],
              '10':['FF6699','FFFFFF'],
              '11':['009933','FFFFFF'],
              '12':['FFFFFF','000000'],
              '13':['FFCC33','000000'],
              '14':['3399CC','FFFFFF'],
              '15':['FF3300','FFFFFF']}


# Terms that indicate points of interest.  Used to split station names
# to (name, city).
POI_TERMS = {'Bahnhof':1, 'Dorfzentrum':1, 'Schiffstation':1,
             'Station':1, u'Zentrum':1,
             'Dorfplatz':1, 'Zentrum/Bahnhof':1, 'Dorf':1}


# Maps station names to (name, city). Used as exception list where our
# simple heuristcs doesn't work.
SPECIAL_NAMES = {
  'Freienbach SOB, Bahnhof': ('Freienbach SOB', 'Freienbach'),
  'Herrliberg-Feldmeilen,Bhf West': ('Bahnhof West', 'Herrliberg-Feldmeilen'),
  'Neue Forch': ('Neue Forch', u'Z\u00fcrich'),
  'Oberrieden Dorf Bahnhof': ('Oberrieden Dorf', 'Oberrieden'),
  'Spital Zollikerberg': ('Spital', 'Zollikerberg'),
  'Triemli': ('Triemli', u'Z\u00fcrich'),
  'Zentrum Glatt': ('Zentrum Glatt', 'Wallisellen'),
}


# Cities whose names we want to prettify/correct at import time.
SPECIAL_CITIES = {
  'Affoltern a. A.': 'Affoltern am Albis',
  'Wangen b. D.': 'Wangen'
}


def ReadCSV(s, cols):
  csv_dialect = csv.Sniffer().sniff(s[0])
  reader = csv.reader(s, csv_dialect)
  header = reader.next()
  col_index = [-1] * len(cols)
  for i in range(len(cols)):
    if cols[i] in header:
      col_index[i] = header.index(cols[i])

  for row in reader:
    result = [None] * len(cols)
    for i in range(len(cols)):
      ci = col_index[i]
      if ci >= 0:
        result[i] = row[ci].decode('iso-8859-1').strip()
    yield result


def ConvertCH1903(x, y):
  "Converts coordinates from the 1903 Swiss national grid system to WGS-84."
  yb = (x - 600000.0) / 1e6;
  xb = (y - 200000.0) / 1e6;
  lam = 2.6779094 \
     + 4.728982 * yb \
     + 0.791484 * yb * xb \
     + 0.1306 * yb * xb * xb \
     - 0.0436 * yb * yb * yb
  phi = 16.9023892 \
     + 3.238372 * xb \
     - 0.270978 * yb * yb \
     - 0.002582 * xb * xb \
     - 0.0447 * yb * yb * xb \
     - 0.0140 * xb * xb * xb
  return (phi * 100.0 / 36.0, lam * 100.0 / 36.0)


def EncodeForCSV(x):
  "Encodes one value for CSV."
  k = x.encode('utf-8')
  if ',' in k or '"' in k:
    return '"%s"' % k.replace('"', '""')
  else:
    return k


def WriteRow(stream, values):
  "Writes one row of comma-separated values to stream."
  stream.write(','.join([EncodeForCSV(val) for val in values]))
  stream.write('\n')


class Station:
  pass


class Route:
  pass


class Pattern:
  pass


class Trip:
  pass


# http://code.google.com/transit/spec/transit_feed_specification.htm
TYPE_TRAM = 0
TYPE_BUS = 3


class DivaImporter:
  def __init__(self, coord_converter, drop_unadvertised_lines):
    self.coord_converter = coord_converter
    self.stations = {}   # id --> Station
    self.routes = {}   # id --> Route
    self.patterns = {}   # id --> Pattern
    self.services = {}  # id --> [date, date, ...] (sorted)
    self.pickup_type = {}  # (trip_id, stop_seq) --> '0'=normal/'1'=no pickup
    self.drop_off_type = {}  # (trip_id, stop_seq) --> '0'/'1', '1'=no drop-off
    self.trips = {}  # id --> Trip
    self.goodTrips = {}
    self._drop_unadvertised_lines = drop_unadvertised_lines

  def DemangleName(self, name):
    "Applies some simple heuristics to split names into (city, name)."

    # Handle special cases where our heuristcs doesn't work.
    # Example:"Triemli" --> ("Triemli", "Zurich").
    if name in SPECIAL_NAMES:
      return SPECIAL_NAMES[name]

    # Expand abbreviations.
    for abbrev, expanded in [('str.', 'strasse'),
                             ('Schiffst.', 'Schiffstation')]:
      suffix_pos = name.rfind(abbrev)
      if suffix_pos > 0:
        name = name[:suffix_pos] + expanded
    #end for

    names = name.split(", ", 1)
    if len(names) == 2:
      if names[1] in POI_TERMS:
        nam = u'%s %s' % (names[0], names[1])
      else:
        nam = names[1]
      city = names[0]
    else:
      # "Zurich Enge": First word of station name designates the city
      nam = names[0]
      city = nam.split(' ')[0]
    return (nam, SPECIAL_CITIES.get(city, city))

  def Import(self, inpath):
    inzip = zipfile.ZipFile(inpath, mode="r")
    read = lambda name, prefix="": (prefix + inzip.read(name)).splitlines()
    # The advertised lines file has no column headers.
    self.ImportStations(read('rec_ort.mdv'), read('bedienendeLinien_google.csv',
                                                  "ORT_NR;LI_NR;;;;"))
    self.ImportRoutes(read('rec_lin_ber.mdv'))
    self.ImportPatterns(read('lid_verlauf.mdv'))
    self.ImportServices(read('tagesart_merkmal.mdv'),
                        read('firmenkalender.mdv'))
    self.ImportTrafficRestrictions(read('vb_regio.mdv'))
    self.ImportBoarding(read('bedverb.mdv'))
    self.ImportStopTimes(read('lid_fahrzeitart.mdv'))
    self.ImportTrips(read('rec_frt.mdv'))

  def ImportStations(self, station_file, adv_file):
    "Imports the rec_ort.mdv file."
    for id, name, x, y, uic_code in  \
    ReadCSV(station_file, ['ORT_NR', 'ORT_NAME',
                'ORT_POS_X', 'ORT_POS_Y', 'ORT_NR_NATIONAL']):
      station = Station()
      station.id = id
      station.position = self.coord_converter(float(x), float(y))
      station.uic_code = ''
      if uic_code and len(uic_code) == 7 and uic_code[:2] == '85':
        station.uic_code = uic_code
      station.name, station.city = self.DemangleName(name)
      station.country = 'CH'
      station.url = 'http://fahrplan.zvv.ch/?to.0=' + \
                    urllib.quote(name.encode('iso-8859-1'))
      station.advertised_lines = set()
      self.stations[id] = station
    for station_id, line_id in ReadCSV(adv_file, ['ORT_NR', 'LI_NR']):
      if station_id in self.stations:
        # Line ids in this file have leading zeroes, remove.
        self.stations[station_id].advertised_lines.add(line_id.lstrip("0"))
      else:
        print "Warning, advertised lines file references " \
              "unknown station, id " + station_id

  def ImportRoutes(self, s):
    "Imports the rec_lin_ber.mdv file."
    # the line id is really qualified with an area_id (BEREICH_NR), but the
    # table of advertised lines does not include area. Fortunately, it seems
    # that line ids are unique across all areas, so we can just throw it away.
    for line_id, name in \
    ReadCSV(s, ['LI_NR', 'LINIEN_BEZ_DRUCK']):
      route = Route()
      route.id = line_id
      route.name = name
      route.color = "FFFFFF"
      route.color_text = "000000"
      if TRAM_LINES.has_key(name):
        route.type = TYPE_TRAM
        route.color = TRAM_LINES[name][0]
        route.color_text = TRAM_LINES[name][1]
      else:
        route.type = TYPE_BUS
        if route.name[0:1]=="N":
          route.color = "000000"
          route.color_text = "FFFF00"
      self.routes[route.id] = route

  def ImportPatterns(self, s):
    "Imports the lid_verlauf.mdv file."
    for line, strli, direction, seq, station_id in  \
    ReadCSV(s, ['LI_NR', 'STR_LI_VAR', 'LI_RI_NR', 'LI_LFD_NR', 'ORT_NR']):
      pattern_id = u'Pat.%s.%s.%s' % (line, strli, direction)
      pattern = self.patterns.get(pattern_id, None)
      if not pattern:
        pattern = Pattern()
        pattern.id = pattern_id
        pattern.stops = []
        pattern.stoptimes = {}
        self.patterns[pattern_id] = pattern
      seq = int(seq) - 1
      if len(pattern.stops) <= seq:
        pattern.stops.extend([None] * (seq - len(pattern.stops)  + 1))
      pattern.stops[seq] = station_id

  def ImportBoarding(self, drop_off_file):
    "Reads the bedverb.mdv file."
    for trip_id, seq, code in \
    ReadCSV(drop_off_file, ['FRT_FID', 'LI_LFD_NR', 'BEDVERB_CODE']):
      key = (trip_id, int(seq) - 1)
      if code == 'A':
        self.pickup_type[key] = '1'  # '1' = no pick-up
      elif code == 'E':
        self.drop_off_type[key] = '1'  # '1' = no drop-off
      elif code == 'B' :
        # 'B' just means that rider needs to push a button to have the driver
        # stop. We don't encode this for now.
        pass
      else:
        raise ValueError('Unexpected code in bedverb.mdv; '
                         'FRT_FID=%s BEDVERB_CODE=%s' % (trip_id, code))

  def ImportServices(self, daytype_file, days_file):
    daytypes = {}   # 'j06' --> {20060713:1, 20060714:1, ...}
    schedules = {}  # {'j06':1, 'p27':1}
    for schedule, daytype, date in  \
    ReadCSV(days_file, ['FPL_KUERZEL', 'TAGESART_NR', 'BETRIEBSTAG']):
      schedule = schedule.strip()
      daytypes.setdefault('%s.%s' % (schedule, daytype), {})[int(date)] = 1
      schedules[schedule] = 1
    schedules = schedules.keys()

    service_days = {}  # 'Cj06.H9' --> {20060713:1, 20060714:1, ...}
    for daytype, service_id in \
    ReadCSV(daytype_file, ['TAGESART_NR', 'TAGESMERKMAL_NR']):
      for schedule in schedules:
        service = 'C%s.%s' % (schedule, service_id)
        for date in daytypes['%s.%s' % (schedule, daytype)].iterkeys():
          service_days.setdefault(service, {})[date] = 1
    for k in service_days.iterkeys():
      self.services[k] = service_days[k].keys()
      self.services[k].sort()

  def ImportTrafficRestrictions(self, restrictions_file):
    "Reads the vb_regio.mdv file."
    ParseDate = lambda x: datetime.date(int(x[:4]), int(x[4:6]), int(x[6:8]))
    MonthNr = lambda x: int(x[:4]) * 12 + int(x[4:6])
    for schedule, id, bitmask, start_date, end_date in  \
    ReadCSV(restrictions_file,
            ['FPL_KUERZEL', 'VB', 'VB_DATUM', 'DATUM_VON', 'DATUM_BIS']):
      id = u"VB%s.%s" % (schedule, id)
      bitmask = bitmask.strip()
      dates = {}

      # This is ugly as hell, I know. I briefly explain what I do:
      # 8 characters in the bitmask equal a month ( 8 * 4bits = 32, no month has
      # more than 31 days, so it's ok).
      # Then I check if the current day of the month is in the bitmask (by
      # shifting the bit by x days and comparing it to the bitmask).
      # If so I calculate back what year month and actual day I am in
      # (very disgusting) and mark that date...
      for i in range(MonthNr(end_date) - MonthNr(start_date)+1):
        mask=int(bitmask[i*8:i*8+8], 16)
        for d in range(32):
          if 1 << d & mask:
            year=int(start_date[0:4])+ ((int(start_date[4:6]) + i -1 )) / 12
            month=((int(start_date[4:6]) + i-1 ) % 12) +1
            day=d+1
            cur_date = str(year)+("0"+str(month))[-2:]+("0"+str(day))[-2:]
            dates[int(cur_date)] = 1
      self.services[id] = dates.keys()
      self.services[id].sort()

  def ImportStopTimes(self, stoptimes_file):
    "Imports the lid_fahrzeitart.mdv file."
    for line, strli, direction, seq, stoptime_id, drive_secs, wait_secs in \
    ReadCSV(stoptimes_file,
            ['LI_NR', 'STR_LI_VAR', 'LI_RI_NR', 'LI_LFD_NR',
             'FGR_NR', 'FZT_REL', 'HZEIT']):
      pattern = self.patterns[u'Pat.%s.%s.%s' % (line, strli, direction)]
      stoptimes = pattern.stoptimes.setdefault(stoptime_id, [])
      seq = int(seq) - 1
      drive_secs = int(drive_secs)
      wait_secs = int(wait_secs)
      assert len(stoptimes) == seq  # fails if seq not in order
      stoptimes.append((drive_secs, wait_secs))

  def ImportTrips(self, trips_file):
    "Imports the rec_frt.mdv file."
    for trip_id, trip_starttime, line, strli, direction,  \
        stoptime_id, schedule_id, daytype_id, restriction_id,  \
        dest_station_id, dest_stop_id, trip_type in  \
        ReadCSV(trips_file,
            ['FRT_FID', 'FRT_START', 'LI_NR', 'STR_LI_VAR', 'LI_RI_NR',
             'FGR_NR', 'FPL_KUERZEL', 'TAGESMERKMAL_NR', 'VB',
             'FRT_HP_AUS', 'HALTEPUNKT_NR_ZIEL', 'FAHRTART_NR']):
      if trip_type != '1':
        print "skipping Trip ", trip_id, line, direction, \
              dest_station_id, trip_type
        continue  # 1=normal, 2=empty, 3=from depot, 4=to depot, 5=other
      trip = Trip()
      #The trip_id (FRT_FID) field is not unique in the vbz data, as of Dec 2009
      # to prevent overwritingimported trips when we key them by trip.id
      # we should make trip.id unique, by combining trip_id and line
      trip.id = ("%s_%s") % (trip_id, line)
      trip.starttime = int(trip_starttime)
      trip.route = self.routes[line]
      dest_station = self.stations[dest_station_id]
      pattern_id = u'Pat.%s.%s.%s' % (line, strli, direction)
      trip.pattern = self.patterns[pattern_id]
      trip.stoptimes = trip.pattern.stoptimes[stoptime_id]
      if restriction_id:
        service_id = u'VB%s.%s' % (schedule_id, restriction_id)
      else:
        service_id = u'C%s.%s' % (schedule_id, daytype_id)
      trip.service_id = service_id
      assert len(self.services[service_id]) > 0
      assert not trip.id in self.trips
      self.trips[trip.id] = trip

  def Write(self, outpath):
    "Writes a .zip file in Google Transit format."
    out = zipfile.ZipFile(outpath, mode="w", compression=zipfile.ZIP_DEFLATED)
    for filename, func in [('agency.txt', self.WriteAgency),
                           ('calendar.txt', self.WriteCalendar),
                           ('calendar_dates.txt', self.WriteCalendarDates),
                           ('routes.txt', self.WriteRoutes),
                           ('trips.txt', self.WriteTrips),
                           ('stops.txt', self.WriteStations),
                           ('stop_times.txt', self.WriteStopTimes)]:
      s = cStringIO.StringIO()
      func(s)
      out.writestr(filename, s.getvalue())
    out.close()

  def WriteAgency(self, out):
    out.write('agency_name,agency_url,agency_lang,agency_timezone\n')
    out.write('VBZ,http://www.vbz.ch/,de,Europe/Zurich\n')

  def WriteRoutes(self, out):
    out.write('route_id,route_short_name,route_long_name,route_type,'
              'route_color,route_text_color\n')
    k = [(r.id, r) for r in self.routes.itervalues()]
    k.sort()
    for id, route in k:
      name = EncodeForCSV(route.name)
      out.write('%s,%s,%s,%s,%s,%s\n' % (
          id, name, name, route.type,route.color,route.color_text))

  def WriteStations(self, out):
    out.write('stop_id,stop_uic_code,stop_name,stop_city,stop_country,'
              'stop_lat,stop_lon,stop_url\n')
    stations = [(s.id, s) for s in self.stations.itervalues()]
    stations.sort()
    for id, s in stations:
      WriteRow(out,
               [id, s.uic_code, s.name, s.city, s.country,
                str(s.position[0]), str(s.position[1]), s.url])

  def WriteCalendar(self, out):
    out.write('service_id,monday,tuesday,wednesday,thursday,'
              'friday,saturday,sunday,start_date,end_date\n')
    for service_id, service in self.services.iteritems():
      out.write('%s,0,0,0,0,0,0,0,%d,%d\n' %
               (EncodeForCSV(service_id), service[0], service[-1]))

  def WriteCalendarDates(self, out):
    out.write('service_id,date,exception_type\n')
    for service_id, service in self.services.iteritems():
      encoded_service_id = EncodeForCSV(service_id)
      for date in service:
        out.write('%s,%d,1\n' % (encoded_service_id, date))

  def WriteTrips(self, out):
    out.write('trip_id,route_id,service_id,trip_headsign\n')
    trips = [(t.id, t) for t in self.trips.itervalues()]
    trips.sort()
    for (trip_id, trip) in trips:
      if (not len(trip.pattern.stops)) or (None in trip.pattern.stops):
        print "*** Skipping bad trip: ", [trip.id]
        continue
      self.goodTrips[trip_id] = True
      headsign = self.stations[trip.pattern.stops[-1]].name
      WriteRow(out, [trip.id, trip.route.id, trip.service_id, headsign])

  def FormatTime(self, t):
    return "%02d:%02d:%02d" % (t / 3600, (t % 3600)/60, t % 60)

  def WriteStopTimes(self, out):
    out.write('trip_id,stop_sequence,stop_id,arrival_time,departure_time,'
              'pickup_type,drop_off_type\n')
    trips = [(t.id, t) for t in self.trips.itervalues()]
    trips.sort()
    for (trip_id, trip) in trips:
      if trip_id not in self.goodTrips:
        continue
      assert len(trip.stoptimes) == len(trip.pattern.stops)
      time = trip.starttime
      for seq in range(len(trip.stoptimes)):
        drive_time, wait_time = trip.stoptimes[seq]
        time += drive_time
        station = self.stations[trip.pattern.stops[seq]]
        if not self._drop_unadvertised_lines or \
           trip.route.id in station.advertised_lines:
          WriteRow(out, [trip.id, str(seq + 1), station.id,
                         self.FormatTime(time),
                         self.FormatTime(time + wait_time),
                         self.pickup_type.get((trip.id, seq), '0'),
                         self.drop_off_type.get((trip.id, seq), '0')])
        time += wait_time


def main(argv):
  # It's hard to replicate the old behavior of --drop_unadvertised_lines, so we
  # don't. Instead, there are only two options without arguments:
  #   nothing                                drop
  #   --nodrop_unadvertised_lines            do not drop
  #   --drop_unadvertised_lines              drop
  opt_parser = optparse.OptionParser()
  # drop_unadvertised_lines: Only export the departures of lines that
  # are advertised at the station in question.  This is used to remove
  # depot trips etc, to not confuse the data in schedule bubbles. Use
  # --nodrop_unadvertised_lines to disable that.
  opt_parser.add_option('--drop_unadvertised_lines', action='store_true',
                        dest='drop_unadvertised_lines', default=True)
  opt_parser.add_option('--nodrop_unadvertised_lines', action='store_false',
                        dest='drop_unadvertised_lines')
  opt_parser.add_option('--in_file', action='store', type='string')
  opt_parser.add_option('--out_file', action='store', type='string')
  options, unused_arguments = opt_parser.parse_args(argv[1:])

  if options.in_file is None:
    raise SystemExit('Please provide a value to the --in_file flag.')
  if options.out_file is None:
    raise SystemExit('Please provide a value to the --out_file flag.')

  importer = DivaImporter(ConvertCH1903, options.drop_unadvertised_lines)
  importer.Import(options.in_file)
  importer.Write(options.out_file)
  print 'Wrote output to', options.out_file


if __name__ == '__main__':
  main(sys.argv)
