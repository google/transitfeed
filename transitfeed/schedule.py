#!/usr/bin/python2.5

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

import bisect
import cStringIO as StringIO
import datetime
import itertools
import os
try:
  import sqlite3 as sqlite
except ImportError:
  from pysqlite2 import dbapi2 as sqlite
import tempfile
import time
import warnings
# Objects in a schedule (Route, Trip, etc) should not keep a strong reference
# to the Schedule object to avoid a reference cycle. Schedule needs to use
# __del__ to cleanup its temporary file. The garbage collector can't handle
# reference cycles containing objects with custom cleanup code.
import weakref
import zipfile

import gtfsfactory
import problems as problems_module
from transitfeed.util import defaultdict
import util

class Schedule:
  """Represents a Schedule, a collection of stops, routes, trips and
  an agency.  This is the main class for this module."""

  def __init__(self, problem_reporter=None,
               memory_db=True, check_duplicate_trips=False,
               gtfs_factory=None):
    if gtfs_factory is None:
      gtfs_factory = gtfsfactory.GetGtfsFactory()
    self._gtfs_factory = gtfs_factory

    # Map from table name to list of columns present in this schedule
    self._table_columns = {}

    self._agencies = {}
    self.stops = {}
    self.routes = {}
    self.trips = {}
    self.service_periods = {}
    self.fares = {}
    self.fare_zones = {}  # represents the set of all known fare zones
    self._shapes = {}  # shape_id to Shape
    # A map from transfer._ID() to a list of transfers. A list is used so
    # there can be more than one transfer with each ID. Once GTFS explicitly
    # prohibits duplicate IDs this might be changed to a simple dict of
    # Transfers.
    self._transfers = defaultdict(lambda: [])
    self._default_service_period = None
    self._default_agency = None
    if problem_reporter is None:
      self.problem_reporter = problems_module.default_problem_reporter
    else:
      self.problem_reporter = problem_reporter
    self._check_duplicate_trips = check_duplicate_trips
    self.ConnectDb(memory_db)

  def AddTableColumn(self, table, column):
    """Add column to table if it is not already there."""
    if column not in self._table_columns[table]:
      self._table_columns[table].append(column)

  def AddTableColumns(self, table, columns):
    """Add columns to table if they are not already there.

    Args:
      table: table name as a string
      columns: an iterable of column names"""
    table_columns = self._table_columns.setdefault(table, [])
    for attr in columns:
      if attr not in table_columns:
        table_columns.append(attr)

  def GetTableColumns(self, table):
    """Return list of columns in a table."""
    return self._table_columns[table]

  def __del__(self):
    self._connection.cursor().close()
    self._connection.close()
    if hasattr(self, '_temp_db_filename'):
      os.remove(self._temp_db_filename)

  def ConnectDb(self, memory_db):
    if memory_db:
      self._connection = sqlite.connect(":memory:")
    else:
      try:
        self._temp_db_file = tempfile.NamedTemporaryFile()
        self._connection = sqlite.connect(self._temp_db_file.name)
      except sqlite.OperationalError:
        # Windows won't let a file be opened twice. mkstemp does not remove the
        # file when all handles to it are closed.
        self._temp_db_file = None
        (fd, self._temp_db_filename) = tempfile.mkstemp(".db")
        os.close(fd)
        self._connection = sqlite.connect(self._temp_db_filename)

    cursor = self._connection.cursor()
    cursor.execute("""CREATE TABLE stop_times (
                                           trip_id CHAR(50),
                                           arrival_secs INTEGER,
                                           departure_secs INTEGER,
                                           stop_id CHAR(50),
                                           stop_sequence INTEGER,
                                           stop_headsign VAR CHAR(100),
                                           pickup_type INTEGER,
                                           drop_off_type INTEGER,
                                           shape_dist_traveled FLOAT);""")
    cursor.execute("""CREATE INDEX trip_index ON stop_times (trip_id);""")
    cursor.execute("""CREATE INDEX stop_index ON stop_times (stop_id);""")

  def GetStopBoundingBox(self):
    return (min(s.stop_lat for s in self.stops.values()),
            min(s.stop_lon for s in self.stops.values()),
            max(s.stop_lat for s in self.stops.values()),
            max(s.stop_lon for s in self.stops.values()),
           )

  def AddAgency(self, name, url, timezone, agency_id=None):
    """Adds an agency to this schedule."""
    agency = self._gtfs_factory.Agency(name, url, timezone, agency_id)
    self.AddAgencyObject(agency)
    return agency

  def AddAgencyObject(self, agency, problem_reporter=None, validate=False):
    assert agency._schedule is None

    if not problem_reporter:
      problem_reporter = self.problem_reporter

    if agency.agency_id in self._agencies:
      problem_reporter.DuplicateID('agency_id', agency.agency_id)
      return

    self.AddTableColumns('agency', agency._ColumnNames())
    agency._schedule = weakref.proxy(self)

    if validate:
      agency.Validate(problem_reporter)
    self._agencies[agency.agency_id] = agency

  def GetAgency(self, agency_id):
    """Return Agency with agency_id or throw a KeyError"""
    return self._agencies[agency_id]

  def GetDefaultAgency(self):
    """Return the default Agency. If no default Agency has been set select the
    default depending on how many Agency objects are in the Schedule. If there
    are 0 make a new Agency the default, if there is 1 it becomes the default,
    if there is more than 1 then return None.
    """
    if not self._default_agency:
      if len(self._agencies) == 0:
        self.NewDefaultAgency()
      elif len(self._agencies) == 1:
        self._default_agency = self._agencies.values()[0]
    return self._default_agency

  def NewDefaultAgency(self, **kwargs):
    """Create a new Agency object and make it the default agency for this Schedule"""
    agency = self._gtfs_factory.Agency(**kwargs)
    if not agency.agency_id:
      agency.agency_id = util.FindUniqueId(self._agencies)
    self._default_agency = agency
    self.SetDefaultAgency(agency, validate=False)  # Blank agency won't validate
    return agency

  def SetDefaultAgency(self, agency, validate=True):
    """Make agency the default and add it to the schedule if not already added"""
    assert isinstance(agency, self._gtfs_factory.Agency)
    self._default_agency = agency
    if agency.agency_id not in self._agencies:
      self.AddAgencyObject(agency, validate=validate)

  def GetAgencyList(self):
    """Returns the list of Agency objects known to this Schedule."""
    return self._agencies.values()

  def GetServicePeriod(self, service_id):
    """Returns the ServicePeriod object with the given ID."""
    return self.service_periods[service_id]

  def GetDefaultServicePeriod(self):
    """Return the default ServicePeriod. If no default ServicePeriod has been
    set select the default depending on how many ServicePeriod objects are in
    the Schedule. If there are 0 make a new ServicePeriod the default, if there
    is 1 it becomes the default, if there is more than 1 then return None.
    """
    if not self._default_service_period:
      if len(self.service_periods) == 0:
        self.NewDefaultServicePeriod()
      elif len(self.service_periods) == 1:
        self._default_service_period = self.service_periods.values()[0]
    return self._default_service_period

  def NewDefaultServicePeriod(self):
    """Create a new ServicePeriod object, make it the default service period and
    return it. The default service period is used when you create a trip without
    providing an explict service period. """
    service_period = self._gtfs_factory.ServicePeriod()
    service_period.service_id = util.FindUniqueId(self.service_periods)
    # blank service won't validate in AddServicePeriodObject
    self.SetDefaultServicePeriod(service_period, validate=False)
    return service_period

  def SetDefaultServicePeriod(self, service_period, validate=True):
    assert isinstance(service_period, self._gtfs_factory.ServicePeriod)
    self._default_service_period = service_period
    if service_period.service_id not in self.service_periods:
      self.AddServicePeriodObject(service_period, validate=validate)

  def AddServicePeriodObject(self, service_period, problem_reporter=None,
                             validate=True):
    if not problem_reporter:
      problem_reporter = self.problem_reporter

    if service_period.service_id in self.service_periods:
      problem_reporter.DuplicateID('service_id', service_period.service_id)
      return

    if validate:
      service_period.Validate(problem_reporter)
    self.service_periods[service_period.service_id] = service_period

  def GetServicePeriodList(self):
    return self.service_periods.values()

  def GetDateRange(self):
    """Returns a tuple of (earliest, latest) dates on which the service
    periods in the schedule define service, in YYYYMMDD form."""

    ranges = [period.GetDateRange() for period in self.GetServicePeriodList()]
    starts = filter(lambda x: x, [item[0] for item in ranges])
    ends = filter(lambda x: x, [item[1] for item in ranges])

    if not starts or not ends:
      return (None, None)

    return (min(starts), max(ends))

  def GetServicePeriodsActiveEachDate(self, date_start, date_end):
    """Return a list of tuples (date, [period1, period2, ...]).

    For each date in the range [date_start, date_end) make list of each
    ServicePeriod object which is active.

    Args:
      date_start: The first date in the list, a date object
      date_end: The first date after the list, a date object

    Returns:
      A list of tuples. Each tuple contains a date object and a list of zero or
      more ServicePeriod objects.
    """
    date_it = date_start
    one_day = datetime.timedelta(days=1)
    date_service_period_list = []
    while date_it < date_end:
      periods_today = []
      date_it_string = date_it.strftime("%Y%m%d")
      for service in self.GetServicePeriodList():
        if service.IsActiveOn(date_it_string, date_it):
          periods_today.append(service)
      date_service_period_list.append((date_it, periods_today))
      date_it += one_day
    return date_service_period_list


  def AddStop(self, lat, lng, name, stop_id=None):
    """Add a stop to this schedule.

    Args:
      lat: Latitude of the stop as a float or string
      lng: Longitude of the stop as a float or string
      name: Name of the stop, which will appear in the feed
      stop_id: stop_id of the stop or None, in which case a unique id is picked

    Returns:
      A new Stop object
    """
    if stop_id is None:
      stop_id = util.FindUniqueId(self.stops)
    stop = self._gtfs_factory.Stop(stop_id=stop_id, lat=lat, lng=lng, name=name)
    self.AddStopObject(stop)
    return stop

  def AddStopObject(self, stop, problem_reporter=None):
    """Add Stop object to this schedule if stop_id is non-blank."""
    assert stop._schedule is None
    if not problem_reporter:
      problem_reporter = self.problem_reporter

    if not stop.stop_id:
      return

    if stop.stop_id in self.stops:
      problem_reporter.DuplicateID('stop_id', stop.stop_id)
      return

    stop._schedule = weakref.proxy(self)
    self.AddTableColumns('stops', stop._ColumnNames())
    self.stops[stop.stop_id] = stop
    if hasattr(stop, 'zone_id') and stop.zone_id:
      self.fare_zones[stop.zone_id] = True

  def GetStopList(self):
    return self.stops.values()

  def AddRoute(self, short_name, long_name, route_type, route_id=None):
    """Add a route to this schedule.

    Args:
      short_name: Short name of the route, such as "71L"
      long_name: Full name of the route, such as "NW 21st Ave/St Helens Rd"
      route_type: A type such as "Tram", "Subway" or "Bus"
      route_id: id of the route or None, in which case a unique id is picked
    Returns:
      A new Route object
    """
    if route_id is None:
      route_id = util.FindUniqueId(self.routes)
    route = self._gtfs_factory.Route(short_name=short_name, long_name=long_name,
                        route_type=route_type, route_id=route_id)
    route.agency_id = self.GetDefaultAgency().agency_id
    self.AddRouteObject(route)
    return route

  def AddRouteObject(self, route, problem_reporter=None):
    if not problem_reporter:
      problem_reporter = self.problem_reporter

    if route.route_id in self.routes:
      problem_reporter.DuplicateID('route_id', route.route_id)
      return

    if route.agency_id not in self._agencies:
      if not route.agency_id and len(self._agencies) == 1:
        # we'll just assume that the route applies to the only agency
        pass
      else:
        problem_reporter.InvalidValue('agency_id', route.agency_id,
                                      'Route uses an unknown agency_id.')
        return

    self.AddTableColumns('routes', route._ColumnNames())
    route._schedule = weakref.proxy(self)
    self.routes[route.route_id] = route

  def GetRouteList(self):
    return self.routes.values()

  def GetRoute(self, route_id):
    return self.routes[route_id]

  def AddShapeObject(self, shape, problem_reporter=None):
    if not problem_reporter:
      problem_reporter = self.problem_reporter

    shape.Validate(problem_reporter)

    if shape.shape_id in self._shapes:
      problem_reporter.DuplicateID('shape_id', shape.shape_id)
      return

    self._shapes[shape.shape_id] = shape

  def GetShapeList(self):
    return self._shapes.values()

  def GetShape(self, shape_id):
    return self._shapes[shape_id]

  def AddTripObject(self, trip, problem_reporter=None, validate=False):
    if not problem_reporter:
      problem_reporter = self.problem_reporter

    if trip.trip_id in self.trips:
      problem_reporter.DuplicateID('trip_id', trip.trip_id)
      return

    self.AddTableColumns('trips', trip._ColumnNames())
    trip._schedule = weakref.proxy(self)
    self.trips[trip.trip_id] = trip

    # Call Trip.Validate after setting trip._schedule so that references
    # are checked. trip.ValidateChildren will be called directly by
    # schedule.Validate, after stop_times has been loaded.
    if validate:
      if not problem_reporter:
        problem_reporter = self.problem_reporter
      trip.Validate(problem_reporter, validate_children=False)
    try:
      self.routes[trip.route_id]._AddTripObject(trip)
    except KeyError:
      # Invalid route_id was reported in the Trip.Validate call above
      pass

  def GetTripList(self):
    return self.trips.values()

  def GetTrip(self, trip_id):
    return self.trips[trip_id]

  def AddFareObject(self, fare, problem_reporter=None):
    """Deprecated. Please use AddFareAttributeObject."""
    warnings.warn("No longer supported. The Fare class was renamed to "
                  "FareAttribute, and all related functions were renamed "
                  "accordingly.", DeprecationWarning)
    self.AddFareAttributeObject(fare, problem_reporter)

  def AddFareAttributeObject(self, fare, problem_reporter=None):
    if not problem_reporter:
      problem_reporter = self.problem_reporter
    fare.Validate(problem_reporter)

    if fare.fare_id in self.fares:
      problem_reporter.DuplicateID('fare_id', fare.fare_id)
      return

    self.fares[fare.fare_id] = fare

  def GetFareList(self):
    """Deprecated. Please use GetFareAttributeList instead"""
    warnings.warn("No longer supported. The Fare class was renamed to "
                  "FareAttribute, and all related functions were renamed "
                  "accordingly.", DeprecationWarning)
    return self.GetFareAttributeList()

  def GetFareAttributeList(self):
    return self.fares.values()

  def GetFare(self, fare_id):
    """Deprecated. Please use GetFareAttribute instead"""
    warnings.warn("No longer supported. The Fare class was renamed to "
                  "FareAttribute, and all related functions were renamed "
                  "accordingly.", DeprecationWarning)
    return self.GetFareAttribute(fare_id)

  def GetFareAttribute(self, fare_id):
    return self.fares[fare_id]

  def AddFareRuleObject(self, rule, problem_reporter=None):
    if not problem_reporter:
      problem_reporter = self.problem_reporter

    if util.IsEmpty(rule.fare_id):
      problem_reporter.MissingValue('fare_id')
      return

    if rule.route_id and rule.route_id not in self.routes:
      problem_reporter.InvalidValue('route_id', rule.route_id)
    if rule.origin_id and rule.origin_id not in self.fare_zones:
      problem_reporter.InvalidValue('origin_id', rule.origin_id)
    if rule.destination_id and rule.destination_id not in self.fare_zones:
      problem_reporter.InvalidValue('destination_id', rule.destination_id)
    if rule.contains_id and rule.contains_id not in self.fare_zones:
      problem_reporter.InvalidValue('contains_id', rule.contains_id)

    if rule.fare_id in self.fares:
      self.GetFareAttribute(rule.fare_id).rules.append(rule)
    else:
      problem_reporter.InvalidValue('fare_id', rule.fare_id,
                                    '(This fare_id doesn\'t correspond to any '
                                    'of the IDs defined in the '
                                    'fare attributes.)')

  def AddTransferObject(self, transfer, problem_reporter=None):
    assert transfer._schedule is None, "only add Transfer to a schedule once"
    if not problem_reporter:
      problem_reporter = self.problem_reporter

    transfer_id = transfer._ID()

    if transfer_id in self._transfers:
      self.problem_reporter.DuplicateID(self._gtfs_factory.Transfer._ID_COLUMNS,
                                        transfer_id,
                                        type=problems_module.TYPE_WARNING)
      # Duplicates are still added, while not prohibited by GTFS.

    transfer._schedule = weakref.proxy(self)  # See weakref comment at top
    self.AddTableColumns('transfers', transfer._ColumnNames())
    self._transfers[transfer_id].append(transfer)

  def GetTransferIter(self):
    """Return an iterator for all Transfer objects in this schedule."""
    return itertools.chain(*self._transfers.values())

  def GetTransferList(self):
    """Return a list containing all Transfer objects in this schedule."""
    return list(self.GetTransferIter())

  def GetStop(self, id):
    return self.stops[id]

  def GetFareZones(self):
    """Returns the list of all fare zones that have been identified by
    the stops that have been added."""
    return self.fare_zones.keys()

  def GetNearestStops(self, lat, lon, n=1):
    """Return the n nearest stops to lat,lon"""
    dist_stop_list = []
    for s in self.stops.values():
      # TODO: Use util.ApproximateDistanceBetweenStops?
      dist = (s.stop_lat - lat)**2 + (s.stop_lon - lon)**2
      if len(dist_stop_list) < n:
        bisect.insort(dist_stop_list, (dist, s))
      elif dist < dist_stop_list[-1][0]:
        bisect.insort(dist_stop_list, (dist, s))
        dist_stop_list.pop()  # Remove stop with greatest distance
    return [stop for dist, stop in dist_stop_list]

  def GetStopsInBoundingBox(self, north, east, south, west, n):
    """Return a sample of up to n stops in a bounding box"""
    stop_list = []
    for s in self.stops.values():
      if (s.stop_lat <= north and s.stop_lat >= south and
          s.stop_lon <= east and s.stop_lon >= west):
        stop_list.append(s)
        if len(stop_list) == n:
          break
    return stop_list

  def Load(self, feed_path, extra_validation=False):
    loader = self._gtfs_factory.Loader(feed_path,
                                       self, problems=self.problem_reporter,
                                       extra_validation=extra_validation)
    loader.Load()

  def _WriteArchiveString(self, archive, filename, stringio):
    zi = zipfile.ZipInfo(filename)
    # See
    # http://stackoverflow.com/questions/434641/how-do-i-set-permissions-attributes-on-a-file-in-a-zip-file-using-pythons-zipf
    zi.external_attr = 0666 << 16L  # Set unix permissions to -rw-rw-rw
    # ZIP_DEFLATED requires zlib. zlib comes with Python 2.4 and 2.5
    zi.compress_type = zipfile.ZIP_DEFLATED
    archive.writestr(zi, stringio.getvalue())

  def WriteGoogleTransitFeed(self, file):
    """Output this schedule as a Google Transit Feed in file_name.

    Args:
      file: path of new feed file (a string) or a file-like object

    Returns:
      None
    """
    # Compression type given when adding each file
    archive = zipfile.ZipFile(file, 'w')

    if 'agency' in self._table_columns:
      agency_string = StringIO.StringIO()
      writer = util.CsvUnicodeWriter(agency_string)
      columns = self.GetTableColumns('agency')
      writer.writerow(columns)
      for a in self._agencies.values():
        writer.writerow([util.EncodeUnicode(a[c]) for c in columns])
      self._WriteArchiveString(archive, 'agency.txt', agency_string)

    calendar_dates_string = StringIO.StringIO()
    writer = util.CsvUnicodeWriter(calendar_dates_string)
    writer.writerow(
        self._gtfs_factory.ServicePeriod._FIELD_NAMES_CALENDAR_DATES)
    has_data = False
    for period in self.service_periods.values():
      for row in period.GenerateCalendarDatesFieldValuesTuples():
        has_data = True
        writer.writerow(row)
    wrote_calendar_dates = False
    if has_data:
      wrote_calendar_dates = True
      self._WriteArchiveString(archive, 'calendar_dates.txt',
                               calendar_dates_string)

    calendar_string = StringIO.StringIO()
    writer = util.CsvUnicodeWriter(calendar_string)
    writer.writerow(self._gtfs_factory.ServicePeriod._FIELD_NAMES)
    has_data = False
    for s in self.service_periods.values():
      row = s.GetCalendarFieldValuesTuple()
      if row:
        has_data = True
        writer.writerow(row)
    if has_data or not wrote_calendar_dates:
      self._WriteArchiveString(archive, 'calendar.txt', calendar_string)

    if 'stops' in self._table_columns:
      stop_string = StringIO.StringIO()
      writer = util.CsvUnicodeWriter(stop_string)
      columns = self.GetTableColumns('stops')
      writer.writerow(columns)
      for s in self.stops.values():
        writer.writerow([util.EncodeUnicode(s[c]) for c in columns])
      self._WriteArchiveString(archive, 'stops.txt', stop_string)

    if 'routes' in self._table_columns:
      route_string = StringIO.StringIO()
      writer = util.CsvUnicodeWriter(route_string)
      columns = self.GetTableColumns('routes')
      writer.writerow(columns)
      for r in self.routes.values():
        writer.writerow([util.EncodeUnicode(r[c]) for c in columns])
      self._WriteArchiveString(archive, 'routes.txt', route_string)

    if 'trips' in self._table_columns:
      trips_string = StringIO.StringIO()
      writer = util.CsvUnicodeWriter(trips_string)
      columns = self.GetTableColumns('trips')
      writer.writerow(columns)
      for t in self.trips.values():
        writer.writerow([util.EncodeUnicode(t[c]) for c in columns])
      self._WriteArchiveString(archive, 'trips.txt', trips_string)

    # write frequencies.txt (if applicable)
    headway_rows = []
    for trip in self.GetTripList():
      headway_rows += trip.GetFrequencyOutputTuples()
    if headway_rows:
      headway_string = StringIO.StringIO()
      writer = util.CsvUnicodeWriter(headway_string)
      writer.writerow(self._gtfs_factory.Frequency._FIELD_NAMES)
      writer.writerows(headway_rows)
      self._WriteArchiveString(archive, 'frequencies.txt', headway_string)

    # write fares (if applicable)
    if self.GetFareAttributeList():
      fare_string = StringIO.StringIO()
      writer = util.CsvUnicodeWriter(fare_string)
      writer.writerow(self._gtfs_factory.FareAttribute._FIELD_NAMES)
      writer.writerows(
          f.GetFieldValuesTuple() for f in self.GetFareAttributeList())
      self._WriteArchiveString(archive, 'fare_attributes.txt', fare_string)

    # write fare rules (if applicable)
    rule_rows = []
    for fare in self.GetFareAttributeList():
      for rule in fare.GetFareRuleList():
        rule_rows.append(rule.GetFieldValuesTuple())
    if rule_rows:
      rule_string = StringIO.StringIO()
      writer = util.CsvUnicodeWriter(rule_string)
      writer.writerow(self._gtfs_factory.FareRule._FIELD_NAMES)
      writer.writerows(rule_rows)
      self._WriteArchiveString(archive, 'fare_rules.txt', rule_string)
    stop_times_string = StringIO.StringIO()
    writer = util.CsvUnicodeWriter(stop_times_string)
    writer.writerow(self._gtfs_factory.StopTime._FIELD_NAMES)
    for t in self.trips.values():
      writer.writerows(t._GenerateStopTimesTuples())
    self._WriteArchiveString(archive, 'stop_times.txt', stop_times_string)

    # write shapes (if applicable)
    shape_rows = []
    for shape in self.GetShapeList():
      seq = 1
      for (lat, lon, dist) in shape.points:
        shape_rows.append((shape.shape_id, lat, lon, seq, dist))
        seq += 1
    if shape_rows:
      shape_string = StringIO.StringIO()
      writer = util.CsvUnicodeWriter(shape_string)
      writer.writerow(self._gtfs_factory.Shape._FIELD_NAMES)
      writer.writerows(shape_rows)
      self._WriteArchiveString(archive, 'shapes.txt', shape_string)

    if 'transfers' in self._table_columns:
      transfer_string = StringIO.StringIO()
      writer = util.CsvUnicodeWriter(transfer_string)
      columns = self.GetTableColumns('transfers')
      writer.writerow(columns)
      for t in self.GetTransferIter():
        writer.writerow([util.EncodeUnicode(t[c]) for c in columns])
      self._WriteArchiveString(archive, 'transfers.txt', transfer_string)

    archive.close()

  def GenerateDateTripsDeparturesList(self, date_start, date_end):
    """Return a list of (date object, number of trips, number of departures).

    The list is generated for dates in the range [date_start, date_end).

    Args:
      date_start: The first date in the list, a date object
      date_end: The first date after the list, a date object

    Returns:
      a list of (date object, number of trips, number of departures) tuples
    """

    service_id_to_trips = defaultdict(lambda: 0)
    service_id_to_departures = defaultdict(lambda: 0)
    for trip in self.GetTripList():
      headway_start_times = trip.GetFrequencyStartTimes()
      if headway_start_times:
        trip_runs = len(headway_start_times)
      else:
        trip_runs = 1

      service_id_to_trips[trip.service_id] += trip_runs
      service_id_to_departures[trip.service_id] += (
          (trip.GetCountStopTimes() - 1) * trip_runs)

    date_services = self.GetServicePeriodsActiveEachDate(date_start, date_end)
    date_trips = []

    for date, services in date_services:
      day_trips = sum(service_id_to_trips[s.service_id] for s in services)
      day_departures = sum(
          service_id_to_departures[s.service_id] for s in services)
      date_trips.append((date, day_trips, day_departures))
    return date_trips

  def ValidateFeedStartAndExpirationDates(self,
                                          problems,
                                          first_date,
                                          last_date,
                                          today):
    """Validate the start and expiration dates of the feed.
       Issue a warning if it only starts in the future, or if
       it expires within 60 days.

    Args:
      problems: The problem reporter object
      first_date: A date object representing the first day the feed is active
      last_date: A date object representing the last day the feed is active
      today: A date object representing the date the validation is being run on

    Returns:
      None
    """
    warning_cutoff = today + datetime.timedelta(days=60)
    if last_date < warning_cutoff:
        problems.ExpirationDate(time.mktime(last_date.timetuple()))

    if first_date > today:
      problems.FutureService(time.mktime(first_date.timetuple()))

  def ValidateServiceGaps(self,
                          problems,
                          validation_start_date,
                          validation_end_date,
                          service_gap_interval):
    """Validate consecutive dates without service in the feed.
       Issue a warning if it finds service gaps of at least
       "service_gap_interval" consecutive days in the date range
       [validation_start_date, last_service_date)

    Args:
      problems: The problem reporter object
      validation_start_date: A date object representing the date from which the
                             validation should take place
      validation_end_date: A date object representing the first day the feed is
                        active
      service_gap_interval: An integer indicating how many consecutive days the
                            service gaps need to have for a warning to be issued

    Returns:
      None
    """
    if service_gap_interval is None:
      return

    departures = self.GenerateDateTripsDeparturesList(validation_start_date,
                                                      validation_end_date)

    # The first day without service of the _current_ gap
    first_day_without_service = validation_start_date
    # The last day without service of the _current_ gap
    last_day_without_service = validation_start_date

    consecutive_days_without_service = 0

    for day_date, day_trips, _ in departures:
      if day_trips == 0:
        if consecutive_days_without_service == 0:
            first_day_without_service = day_date
        consecutive_days_without_service += 1
        last_day_without_service = day_date
      else:
        if consecutive_days_without_service >= service_gap_interval:
            problems.TooManyDaysWithoutService(first_day_without_service,
                                               last_day_without_service,
                                               consecutive_days_without_service)

        consecutive_days_without_service = 0

    # We have to check if there is a gap at the end of the specified date range
    if consecutive_days_without_service >= service_gap_interval:
      problems.TooManyDaysWithoutService(first_day_without_service,
                                         last_day_without_service,
                                         consecutive_days_without_service)

  def ValidateServiceExceptions(self,
                                problems,
                                first_service_day,
                                last_service_day):
    # good enough approximation
    six_months = datetime.timedelta(days=182)
    service_span = last_service_day - first_service_day
    if service_span < six_months:
      # We don't check for exceptions because the feed is
      # active for less than six months
      return

    for period in self.GetServicePeriodList():
      # If at least one ServicePeriod has service exceptions we don't issue the
      # warning, so we can stop looking at the list of ServicePeriods.
      if period.HasExceptions():
        return
    problems.NoServiceExceptions(start=first_service_day,
                                 end=last_service_day)

  def ValidateServiceRangeAndExceptions(self, problems, today,
                                        service_gap_interval):
    if today is None:
      today = datetime.date.today()
    (start_date, end_date) = self.GetDateRange()
    if not end_date or not start_date:
      problems.OtherProblem('This feed has no effective service dates!',
                            type=problems_module.TYPE_WARNING)
    else:
        try:
          last_service_day = datetime.datetime(
              *(time.strptime(end_date, "%Y%m%d")[0:6])).date()
          first_service_day = datetime.datetime(
              *(time.strptime(start_date, "%Y%m%d")[0:6])).date()

        except ValueError:
          # Format of start_date and end_date checked in class ServicePeriod
          pass

        else:
          self.ValidateServiceExceptions(problems,
                                         first_service_day,
                                         last_service_day)
          self.ValidateFeedStartAndExpirationDates(problems,
                                                   first_service_day,
                                                   last_service_day,
                                                   today)

          # We start checking for service gaps a bit in the past if the
          # feed was active then. See
          # http://code.google.com/p/googletransitdatafeed/issues/detail?id=188
          #
          # We subtract 1 from service_gap_interval so that if today has
          # service no warning is issued.
          #
          # Service gaps are searched for only up to one year from today
          if service_gap_interval is not None:
            service_gap_timedelta = datetime.timedelta(
                                        days=service_gap_interval - 1)
            one_year = datetime.timedelta(days=365)
            self.ValidateServiceGaps(
                problems,
                max(first_service_day,
                    today - service_gap_timedelta),
                min(last_service_day,
                    today + one_year),
                service_gap_interval)

  def ValidateStops(self, problems, validate_children):
    # Check for stops that aren't referenced by any trips and broken
    # parent_station references. Also check that the parent station isn't too
    # far from its child stops.
    for stop in self.stops.values():
      if validate_children:
        stop.Validate(problems)
      cursor = self._connection.cursor()
      cursor.execute("SELECT count(*) FROM stop_times WHERE stop_id=? LIMIT 1",
                     (stop.stop_id,))
      count = cursor.fetchone()[0]
      if stop.location_type == 0 and count == 0:
          problems.UnusedStop(stop.stop_id, stop.stop_name)
      elif stop.location_type == 1 and count != 0:
          problems.UsedStation(stop.stop_id, stop.stop_name)

      if stop.location_type != 1 and stop.parent_station:
        if stop.parent_station not in self.stops:
          problems.InvalidValue("parent_station",
                                util.EncodeUnicode(stop.parent_station),
                                "parent_station '%s' not found for stop_id "
                                "'%s' in stops.txt" %
                                (util.EncodeUnicode(stop.parent_station),
                                 util.EncodeUnicode(stop.stop_id)))
        elif self.stops[stop.parent_station].location_type != 1:
          problems.InvalidValue("parent_station",
                                util.EncodeUnicode(stop.parent_station),
                                "parent_station '%s' of stop_id '%s' must "
                                "have location_type=1 in stops.txt" %
                                (util.EncodeUnicode(stop.parent_station),
                                 util.EncodeUnicode(stop.stop_id)))
        else:
          parent_station = self.stops[stop.parent_station]
          distance = util.ApproximateDistanceBetweenStops(stop, parent_station)
          if distance > problems_module.MAX_DISTANCE_BETWEEN_STOP_AND_PARENT_STATION_ERROR:
            problems.StopTooFarFromParentStation(
                stop.stop_id, stop.stop_name, parent_station.stop_id,
                parent_station.stop_name, distance, problems_module.TYPE_ERROR)
          elif distance > problems_module.MAX_DISTANCE_BETWEEN_STOP_AND_PARENT_STATION_WARNING:
            problems.StopTooFarFromParentStation(
                stop.stop_id, stop.stop_name, parent_station.stop_id,
                parent_station.stop_name, distance,
                problems_module.TYPE_WARNING)

  def ValidateNearbyStops(self, problems):
    # Check for stops that might represent the same location (specifically,
    # stops that are less that 2 meters apart) First filter out stops without a
    # valid lat and lon. Then sort by latitude, then find the distance between
    # each pair of stations within 2 meters latitude of each other. This avoids
    # doing n^2 comparisons in the average case and doesn't need a spatial
    # index.
    sorted_stops = filter(lambda s: s.stop_lat and s.stop_lon,
                          self.GetStopList())
    sorted_stops.sort(key=(lambda x: x.stop_lat))
    TWO_METERS_LAT = 0.000018
    for index, stop in enumerate(sorted_stops[:-1]):
      index += 1
      while ((index < len(sorted_stops)) and
             ((sorted_stops[index].stop_lat - stop.stop_lat) < TWO_METERS_LAT)):
        distance  = util.ApproximateDistanceBetweenStops(stop,
                                                         sorted_stops[index])
        if distance < 2:
          other_stop = sorted_stops[index]
          if stop.location_type == 0 and other_stop.location_type == 0:
            problems.StopsTooClose(
                util.EncodeUnicode(stop.stop_name),
                util.EncodeUnicode(stop.stop_id),
                util.EncodeUnicode(other_stop.stop_name),
                util.EncodeUnicode(other_stop.stop_id), distance)
          elif stop.location_type == 1 and other_stop.location_type == 1:
            problems.StationsTooClose(
                util.EncodeUnicode(stop.stop_name),
                util.EncodeUnicode(stop.stop_id),
                util.EncodeUnicode(other_stop.stop_name),
                util.EncodeUnicode(other_stop.stop_id), distance)
          elif (stop.location_type in (0, 1) and
                other_stop.location_type  in (0, 1)):
            if stop.location_type == 0 and other_stop.location_type == 1:
              this_stop = stop
              this_station = other_stop
            elif stop.location_type == 1 and other_stop.location_type == 0:
              this_stop = other_stop
              this_station = stop
            if this_stop.parent_station != this_station.stop_id:
              problems.DifferentStationTooClose(
                  util.EncodeUnicode(this_stop.stop_name),
                  util.EncodeUnicode(this_stop.stop_id),
                  util.EncodeUnicode(this_station.stop_name),
                  util.EncodeUnicode(this_station.stop_id), distance)
        index += 1

  def ValidateRouteNames(self, problems, validate_children):
    # Check for multiple routes using same short + long name
    route_names = {}
    for route in self.routes.values():
      if validate_children:
        route.Validate(problems)
      short_name = ''
      if not util.IsEmpty(route.route_short_name):
        short_name = route.route_short_name.lower().strip()
      long_name = ''
      if not util.IsEmpty(route.route_long_name):
        long_name = route.route_long_name.lower().strip()
      name = (short_name, long_name)
      if name in route_names:
        problems.InvalidValue('route_long_name',
                              long_name,
                              'The same combination of '
                              'route_short_name and route_long_name '
                              'shouldn\'t be used for more than one '
                              'route, as it is for the for the two routes '
                              'with IDs "%s" and "%s".' %
                              (route.route_id, route_names[name].route_id),
                              type=problems_module.TYPE_WARNING)
      else:
        route_names[name] = route

  def ValidateTrips(self, problems):
    stop_types = {} # a dict mapping stop_id to [route_id, route_type, is_match]
    trips = {} # a dict mapping tuple to (route_id, trip_id)

    # a dict mapping block_id to a list of tuple of
    # (trip_id, first_arrival_secs, last_arrival_secs)
    trip_intervals_by_block_id = defaultdict(lambda: [])

    for trip in sorted(self.trips.values()):
      if trip.route_id not in self.routes:
        continue
      route_type = self.GetRoute(trip.route_id).route_type
      stop_ids = []
      stop_times = trip.GetStopTimes(problems)
      for index, st in enumerate(stop_times):
        stop_id = st.stop.stop_id
        stop_ids.append(stop_id)
        # Check a stop if which belongs to both subway and bus.
        if (route_type == self._gtfs_factory.Route._ROUTE_TYPE_NAMES['Subway'] or
            route_type == self._gtfs_factory.Route._ROUTE_TYPE_NAMES['Bus']):
          if stop_id not in stop_types:
            stop_types[stop_id] = [trip.route_id, route_type, 0]
          elif (stop_types[stop_id][1] != route_type and
                stop_types[stop_id][2] == 0):
            stop_types[stop_id][2] = 1
            if stop_types[stop_id][1] == \
                self._gtfs_factory.Route._ROUTE_TYPE_NAMES['Subway']:
              subway_route_id = stop_types[stop_id][0]
              bus_route_id = trip.route_id
            else:
              subway_route_id = trip.route_id
              bus_route_id = stop_types[stop_id][0]
            problems.StopWithMultipleRouteTypes(st.stop.stop_name, stop_id,
                                                subway_route_id, bus_route_id)

      # We only care about trips with a block id
      if not util.IsEmpty(trip.block_id) and stop_times:

        first_arrival_secs = stop_times[0].arrival_secs
        last_departure_secs = stop_times[-1].departure_secs

        # The arrival and departure time of the first and last stop_time
        # SHOULD be set, but we need to handle the case where we're given
        # an invalid feed anyway
        if first_arrival_secs is not None and last_departure_secs is not None:

          # Create a trip interval tuple of the trip id and arrival time
          # intervals
          key = trip.block_id
          trip_intervals = trip_intervals_by_block_id[key]
          trip_interval = (trip, first_arrival_secs, last_departure_secs)
          trip_intervals.append(trip_interval)

      # Check duplicate trips which go through the same stops with same
      # service and start times.
      if self._check_duplicate_trips:
        if not stop_ids or not stop_times:
          continue
        key = (trip.service_id, stop_times[0].arrival_time, str(stop_ids))
        if key not in trips:
          trips[key] = (trip.route_id, trip.trip_id)
        else:
          problems.DuplicateTrip(trips[key][1], trips[key][0], trip.trip_id,
                                 trip.route_id)

    # Now that we've generated our block trip intervls, we can check for
    # overlaps in the intervals
    self.ValidateBlocks(problems, trip_intervals_by_block_id)

  def ValidateBlocks(self, problems, trip_intervals_by_block_id):
    # Expects trip_intervals_by_block_id to be a dict with a key of block ids
    # and a value of lists of tuples
    # (trip, min_arrival_secs, max_departure_secs)

    # Cache potentially expensive ServicePeriod overlap checks
    service_period_overlap_cache = {}

    for (block_id,trip_intervals) in trip_intervals_by_block_id.items():

      # Sort trip intervals by min arrival time
      trip_intervals.sort(key=(lambda x: x[1]))

      for xi in range(len(trip_intervals)):
        trip_interval_a = trip_intervals[xi]
        trip_a = trip_interval_a[0]

        for xj in range(xi+1,len(trip_intervals)):
          trip_interval_b = trip_intervals[xj]
          trip_b = trip_interval_b[0]

          # If the last departure of trip interval A is less than or equal
          # to the first arrival of trip interval B, stop checking
          if trip_interval_a[2] <= trip_interval_b[1]:
            break

          # We have an overlap between the times in two trip intervals in
          # the same block.  Potentially a problem...

          # If they have the same service id, the trips run on the same
          # day, yet have overlapping stop times.  Definitely a problem.
          if trip_a.service_id == trip_b.service_id:
            problems.OverlappingTripsInSameBlock(trip_a.trip_id,
                                                 trip_b.trip_id, block_id)
          else:
            # Even if the the trips don't have the same service_id, their
            # service dates might still overlap.  Since the ServicePeriod
            # overlap check is potentially expensive, we cache the
            # computation

            service_id_pair_key = tuple(sorted([trip_a.service_id,
                                                trip_b.service_id]))

            # If the serivce_id_pair_key is not in the cache, we do the
            # full service period comparison
            if service_id_pair_key not in service_period_overlap_cache:

              service_period_a = self.GetServicePeriod(trip_a.service_id)
              service_period_b = self.GetServicePeriod(trip_b.service_id)

              dates_a = service_period_a.ActiveDates()
              dates_b = service_period_b.ActiveDates()

              overlap = False

              for date in dates_a:
                if date in dates_b:
                  overlap = True
                  break

              service_period_overlap_cache[service_id_pair_key] = overlap

            if service_period_overlap_cache[service_id_pair_key]:
              problems.OverlappingTripsInSameBlock(trip_a.trip_id,
                                                   trip_b.trip_id,
                                                   block_id)

  def ValidateRouteAgencyId(self, problems):
    # Check that routes' agency IDs are valid, if set
    for route in self.routes.values():
      if (not util.IsEmpty(route.agency_id) and
          not route.agency_id in self._agencies):
        problems.InvalidAgencyID('agency_id', route.agency_id,
                                 'route', route.route_id)

  def ValidateTripStopTimes(self, problems):
    # Make sure all trips have stop_times
    # We're doing this here instead of in Trip.Validate() so that
    # Trips can be validated without error during the reading of trips.txt
    for trip in self.trips.values():
      trip.ValidateChildren(problems)
      count_stop_times = trip.GetCountStopTimes()
      if not count_stop_times:
        problems.OtherProblem('The trip with the trip_id "%s" doesn\'t have '
                              'any stop times defined.' % trip.trip_id,
                              type=problems_module.TYPE_WARNING)
        if len(trip._headways) > 0:  # no stoptimes, but there are headways
          problems.OtherProblem('Frequencies defined, but no stop times given '
                                'in trip %s' % trip.trip_id,
                                type=problems_module.TYPE_ERROR)
      elif count_stop_times == 1:
        problems.OtherProblem('The trip with the trip_id "%s" only has one '
                              'stop on it; it should have at least one more '
                              'stop so that the riders can leave!' %
                              trip.trip_id, type=problems_module.TYPE_WARNING)
      else:
        # These methods report InvalidValue if there's no first or last time
        trip.GetStartTime(problems=problems)
        trip.GetEndTime(problems=problems)

  def ValidateUnusedShapes(self, problems):
    # Check for unused shapes
    known_shape_ids = set(self._shapes.keys())
    used_shape_ids = set()
    for trip in self.GetTripList():
      used_shape_ids.add(trip.shape_id)
    unused_shape_ids = known_shape_ids - used_shape_ids
    if unused_shape_ids:
      problems.OtherProblem('The shapes with the following shape_ids aren\'t '
                            'used by any trips: %s' %
                            ', '.join(unused_shape_ids),
                            type=problems_module.TYPE_WARNING)

  def Validate(self,
               problems=None,
               validate_children=True,
               today=None,
               service_gap_interval=None):
    """Validates various holistic aspects of the schedule
       (mostly interrelationships between the various data sets)."""

    if not problems:
      problems = self.problem_reporter

    self.ValidateServiceRangeAndExceptions(problems, today,
                                           service_gap_interval)
    # TODO: Check Trip fields against valid values
    self.ValidateStops(problems, validate_children)
    #TODO: check that every station is used.
    # Then uncomment testStationWithoutReference.
    self.ValidateNearbyStops(problems)
    self.ValidateRouteNames(problems, validate_children)
    self.ValidateTrips(problems)
    self.ValidateRouteAgencyId(problems)
    self.ValidateTripStopTimes(problems)
    self.ValidateUnusedShapes(problems)
