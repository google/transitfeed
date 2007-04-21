#!/usr/bin/python2.4

# Copyright (C) 2007 Google Inc.
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

# An example script that hopefully demonstrates that programattically making a
# Google Transit Feed file can be very easy.

import transitfeed
from optparse import OptionParser
import re

stops = {}

# table is a list of lists in this form
# [ ['Short Name', 'Long Name'],
#   ['Stop 1', 'Stop 2', ...]
#   [time_at_1, time_at_2, ...]  # times for trip 1
#   [time_at_1, time_at_2, ...]  # times for trip 2
#   ... ]
def AddRouteToSchedule(schedule, table):
  if len(table) >= 2:
    r = schedule.AddRoute(short_name=table[0][0], long_name=table[0][1], type='Bus')
    for trip in table[2:]:
      if len(trip) > len(table[1]):
        print "ignoring %s" % trip[len(table[1]):]
        trip = trip[0:len(table[1])]
      t = r.AddTrip(headsign='My headsign')
      trip_stops = []  # Build a list of (time, stopname) tuples
      for i in range(0, len(trip)):
        if re.search(r'\S', trip[i]):
          trip_stops.append( (transitfeed.TimeToSecondsSinceMidnight(trip[i]), table[1][i]) )
      trip_stops.sort()  # Sort by time
      for (time, stopname) in trip_stops:
        t.AddStopTime(stop=stops[stopname.lower()], time_arr=time, time_dep=time)

def TransposeTable(table):
  """Transpose a list of lists, using None to extend all input lists to the
  same length.

  For example:
  >>> TransposeTable(
  [ [11,   12,   13],
    [21,   22],
    [31,   32,   33,   34]])

  [ [11,   21,   31],
    [12,   22,   32],
    [13,   None, 33],
    [None, None, 34]]
  """
  transposed = []
  rows = len(table)
  cols = max(len(row) for row in table)
  for x in range(cols):
    transposed.append([])
    for y in range(rows):
      if x < len(table[y]):
        transposed[x].append(table[y][x])
      else:
        transposed[x].append(None)
  return transposed

def ProcessOptions(schedule, table):
  service_period = schedule.GetActiveServicePeriod()
  agency_name, agency_url, agency_timezone = (None, None, None)

  for row in table[1:]:
    command = row[0].lower()
    if command == 'weekday':
      service_period.SetWeekdayService()
    elif command == 'start_date':
      service_period.SetStartDate(row[1])
    elif command == 'end_date':
      service_period.SetEndDate(row[1])
    elif command == 'add_date':
      service_period.SetDateHasService(date=row[1])
    elif command == 'remove_date':
      service_period.SetDateHasService(date=row[1], has_service=False)
    elif command == 'agency_name':
      agency_name = row[1]
    elif command == 'agency_url':
      agency_url = row[1]
    elif command == 'agency_timezone':
      agency_timezone = row[1]

  if not (agency_name and agency_url and agency_timezone):
    print "You must provide agency information"

  schedule.SetAgency(name=agency_name, url=agency_url, timezone=agency_timezone)


def AddStops(schedule, table):
  for name, lat_str, lng_str in table[1:]:
    stop = schedule.AddStop(lat=float(lat_str), lng=float(lng_str), name=name)
    stops[name.lower()] = stop


def ProcessTable(schedule, table):
  if table[0][0].lower() == 'options':
    ProcessOptions(schedule, table)
  elif table[0][0].lower() == 'stops':
    AddStops(schedule, table)
  else:
    # AddRouteToSchedule expects the table[0] to be the route name, table[1] to
    # be stop names and each list in table[2:] to be the times for a trip. This
    # input contains the stop names in table[x][0], x >= 2 with trips found in
    # columns, so we need to transpose table[1:]
    transposed = [table[0]]
    transposed.extend(TransposeTable(table[1:]))
    AddRouteToSchedule(schedule, transposed)


def main():
  parser = OptionParser()
  parser.add_option('--input', dest='input',
                    help='Path of input file')
  parser.add_option('--output', dest='output',
                    help='Path of output file, should end in .zip')
  parser.set_defaults(output='feed.zip')
  (options, args) = parser.parse_args()

  schedule = transitfeed.Schedule()

  table = []
  for line in open(options.input):
    line = line.rstrip()
    if not line:
      ProcessTable(schedule, table)
      table = []
    else:
      table.append(line.split('\t'))

  ProcessTable(schedule, table)

  schedule.WriteGoogleTransitFeed(options.output)


if __name__ == '__main__':
  main()
