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

"""Easy interface for handling a Google Transit Feed file.

This module is a library to help you create, read and write Google
Transit Feed files. Refer to the feed specification, available at
http://code.google.com/transit/spec/transit_feed_specification.htm, for a
complete description how the transit feed represents a transit schedule. This
library supports all required parts of the specification but does not yet
support all optional parts. Patches welcome!

The specification describes several tables such as stops, routes and trips.
In a feed file these are stored as comma separeted value files. This library
represents each row of these tables with a single Python object. This object has
attributes for each value on the row. For example, schedule.AddStop returns a
Stop object which has attributes such as stop_lat and stop_name.

  Schedule: Central object of the parser
  Route: Represents a single route
  Trip: Represents a single trip
  Stop: Represents a single stop
  ServicePeriod: Represents a single service, a set of dates
  Agency: Represents the agency in this feed
  TimeToSecondsSinceMidnight(): Convert HH:MM:SS into seconds since midnight.
  FormatSecondsSinceMidnight(s): Formats number of seconds past midnight into a string
"""

# TODO: Preserve arbitrary columns?

import bisect
import cStringIO as StringIO
import codecs
import csv
import logging
import math
import os
import random
import re
import zipfile

OUTPUT_ENCODING = 'utf-8'


__version__ = '1.0.8'


class ProblemReporter:
  """This is a basic problem reporter that just prints to console."""

  def __init__(self):
    self._context = None

  def SetContext(self, context):
    self._context = context

  def SetFileContext(self, filename, row_num, row):
    """Save the current context to be output with any errors.

    Args:
      filename: string
      row_num: int
      row: list of unicode strings
    """
    self.SetContext('in line %d of %s:\n%s' %
                    (row_num, filename, ', '.join(map(unicode, row))))

  def _Report(self, problem_text):
    print self._EncodeUnicode(self._LineWrap(problem_text, 79))
    if self._context:
      print self._EncodeUnicode(self._LineWrap(self._context, 79))

  @staticmethod
  def _EncodeUnicode(text):
    """
    Optionally encode text and return it. The result should be safe to print.
    """
    if type(text) == type(u''):
      return text.encode(OUTPUT_ENCODING)
    else:
      return text

  @staticmethod
  def _LineWrap(text, width):
    """
    A word-wrap function that preserves existing line breaks
    and most spaces in the text. Expects that existing line
    breaks are posix newlines (\n).
    
    Taken from:
    http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/148061
    """
    return reduce(lambda line, word, width=width: '%s%s%s' %
                  (line,
                   ' \n'[(len(line) - line.rfind('\n') - 1 +
                         len(word.split('\n', 1)[0]) >= width)],
                   word),
                  text.split(' ')
                 )

  def FeedNotFound(self, feed_name):
    self._Report('Couldn\'t find a feed named "%s"' % feed_name)

  def UnknownFormat(self, feed_name):
    self._Report('The feed named "%s" had an unknown format:\n'
                 'feeds should be either .zip files or directories.' %
                 feed_name)

  def MissingFile(self, file_name):
    self._Report('Missing required file "%s"' % file_name)

  def EmptyFile(self, file_name):
    self._Report('File "%s" was empty' % file_name)

  def MissingColumn(self, file_name, column_name):
    self._Report('Missing column "%s" in file "%s"' % (column_name, file_name))

  def MissingValue(self, field_name):
    self._Report('Missing value for field "%s"' % field_name)

  def InvalidValue(self, field_name, value, reason=None):
    text = 'Invalid value "%s" found for field "%s"' % (value, field_name)
    if reason:
      text += '\n' + reason
    self._Report(text)

  def DuplicateID(self, column_name, value):
    self._Report('Duplicated ID "%s" found in "%s" column' %
                 (value, column_name))
				 
  def UnusedStop(self, stop_id, stop_name):
    self._Report('The stop "%s" (with ID "%s") isn\'t '
                 'used for any trips.' % (stop_name, stop_id))

  def OtherProblem(self, description):
    self._Report(description)


class FeedException(Exception):
  def __init__(self, feed_name):
    Exception.__init__(self)
    self.feed_name = feed_name

  def __str__(self):
    return self.feed_name


class FeedNotFound(FeedException):
  pass


class UnknownFormat(FeedException):
  pass


class FileException(Exception):
  def __init__(self, file_name):
    Exception.__init__(self)
    self.file_name = file_name

  def __str__(self):
    return self.file_name


class MissingFile(FileException):
  pass


class EmptyFile(FileException):
  pass


class MissingColumn(Exception):
  def __init__(self, file_name, column_name):
    Exception.__init__(self)
    self.file_name = file_name
    self.column_name = column_name

  def __str__(self):
    return '"%s" in file "%s"' % (self.file_name, self.column_name)


class MissingValue(Exception):
  def __init__(self, field_name):
    Exception.__init__(self)
    self.field_name = field_name

  def __str__(self):
    return self.field_name


class InvalidValue(Exception):
  def __init__(self, field_name, value, reason):
    Exception.__init__(self)
    self.field_name = field_name
    self.value = value
    self.reason = reason

  def __str__(self):
    text = '"%s" in field "%s"' % (self.value, self.field_name)
    if (self.reason):
      text += '\n' + self.reason
    return text


class DuplicateID(Exception):
  def __init__(self, column_name, value):
    Exception.__init__(self)
    self.column_name = column_name
    self.value = value

  def __str__(self):
    return '"%s" in column "%s"' % (self.value, self.column_name)
	
	
class UnusedStop(Exception):
  def __init__(self, stop_id, stop_name):
    Exception.__init__(self)
    self.stop_id = stop_id
    self.stop_name = stop_name

  def __str__(self):
    return '%s (ID %s)' % (self.stop_name, self.stop_id)

class OtherProblem(Exception):
  def __init__(self, description):
    Exception.__init__(self)
    self.description = description

  def __str__(self):
    return self.description


class ExceptionProblemReporter(ProblemReporter):
  def FeedNotFound(self, feed_name):
    raise FeedNotFound(feed_name)

  def UnknownFormat(self, feed_name):
    raise UnknownFormat(feed_name)

  def MissingFile(self, file_name):
    raise MissingFile(file_name)

  def EmptyFile(self, file_name):
    raise EmptyFile(file_name)

  def MissingColumn(self, file_name, column_name):
    raise MissingColumn(file_name, column_name)

  def MissingValue(self, field_name):
    raise MissingValue(field_name)

  def InvalidValue(self, column_name, value, reason=None):
    raise InvalidValue(column_name, value, reason)

  def DuplicateID(self, column_name, value):
    raise DuplicateID(column_name, value)
	
  def UnusedStop(self, stop_id, stop_name):
    raise UnusedStop(stop_id, stop_name)

  def OtherProblem(self, description):
    raise OtherProblem(description)

default_problem_reporter = ExceptionProblemReporter()

# Add a default handler to send log messages to console
console = logging.StreamHandler()
console.setLevel(logging.WARNING)
log = logging.getLogger("schedule_builder")
log.addHandler(console)


class Error(Exception):
  pass


def IsValidURL(url):
  """Checks the validity of a URL value."""
  # TODO: Add more thorough checking of URL
  return url.startswith(u'http://') or url.startswith(u'https://')


def IsValidColor(color):
  """Checks the validity of a hex color value."""
  return not re.match('^[0-9a-fA-F]{6}$', color) == None


def IsEmpty(value):
  return not value or (isinstance(value, basestring) and not value.strip())


def FindUniqueId(dic):
  """Return a string not used as a key in the dictionary dic"""
  name = str(len(dic))
  while name in dic:
    name = str(random.randint(1, 999999999))
  return name


def TimeToSecondsSinceMidnight(time_string):
  """Convert HH:MM:SS into seconds since midnight.

  For example "01:02:03" returns 3723. The leading zero of the hours may be
  omitted. HH may be more than 23 if the time is on the following day."""
  m = re.match(r'(\d?\d):(\d\d):(\d\d)$', time_string)
  if not m:
    raise Error, 'Bad HH:MM:SS "%s"' % time_string
  return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))


def FormatSecondsSinceMidnight(s):
  """Formats an int number of seconds past midnight into a string
  as "HH:MM:SS"."""
  return "%02d:%02d:%02d" % (s / 3600, (s / 60) % 60, s % 60)


EARTH_RADIUS = 6378135          # in meters
def ApproximateDistanceBetweenStops(stop1, stop2):
  """Compute approximate distance between two stops in meters. Assumes the
  Earth is a sphere."""
  # TODO: change to ellipsoid approximation, such as
  # http://www.codeguru.com/Cpp/Cpp/algorithms/article.php/c5115/
  lat1 = math.radians(stop1.stop_lat)
  lat2 = math.radians(stop2.stop_lat)
  lng1 = math.radians(stop1.stop_lon)
  lng2 = math.radians(stop2.stop_lon)
  dlat = math.sin(0.5 * (lat2 - lat1))
  dlng = math.sin(0.5 * (lng2 - lng1))
  x = dlat * dlat + dlng * dlng * math.cos(lat1) * math.cos(lat2)
  return EARTH_RADIUS * (2 * math.atan2(math.sqrt(x),
      math.sqrt(max(0.0, 1.0 - x))))


def ReadCSV(str, encoding, cols):
  """Reads lines from str, yielding a list of values corresponding to the
  column names in cols."""
  reader = csv.reader(StringIO.StringIO(str))  # Use default, excel, dialect

  header = reader.next()
  col_index = [-1] * len(cols)
  for i in range(len(cols)):
    lower_header = map(lambda x:x.lower(), header)
    if cols[i] in lower_header:
      col_index[i] = lower_header.index(cols[i])
      if header[col_index[i]] != lower_header[col_index[i]]:
        log.warning('Column header "%s" should have the capitalization "%s"'
                    % (header[col_index[i]], lower_header[col_index[i]]))

  for row in reader:
    result = [None] * len(cols)
    for i in range(len(cols)):
      ci = col_index[i]
      if ci >= 0:
        result[i] = row[ci].decode(encoding).strip()
    yield result


class Stop(object):
  """Represents a single stop. A stop must have a latitude, longitude and name."""
  _REQUIRED_FIELD_NAMES = ['stop_id', 'stop_name', 'stop_lat', 'stop_lon']
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ['stop_desc', 'zone_id', 'stop_url']

  def __init__(self, lat=None, lng=None, name=None, stop_id=None,
               field_list=None):
    self.stop_desc = ''
    self.zone_id = ''
    self.stop_url = ''
    if field_list:
      (stop_id, name, lat, lng, self.stop_desc, self.zone_id, self.stop_url) =\
      field_list
    try:
      self.stop_lat = float(lat)
    except (ValueError, TypeError):
      self.stop_lat = 0
    try:
      self.stop_lon = float(lng)
    except (ValueError, TypeError):
      self.stop_lon = 0
    self.stop_name = name
    self.trip_index = []  # list of (trip, index) for each Trip object.
                          # index is offset into Trip _stoptimes
    self.stop_id = stop_id

  def GetFieldValuesTuple(self):
    return [getattr(self, fn) for fn in Stop._FIELD_NAMES]

  def GetStopTimeTrips(self):
    """Returns an list of (time, trip), where time is some time.
    TODO: handle stops between timed stops."""
    time_trips = []
    for trip, index in self.trip_index:
      time_trips.append((trip._stoptimes[index].GetTimeSecs(), trip))
    return time_trips

  def _AddTripStop(self, trip, index):
    self.trip_index.append((trip, index))

  def __getitem__(self, name):
    return getattr(self, name)

  def __eq__(self, other):
    if not other:
      return False

    if id(self) == id(other):
      return True

    return self.GetFieldValuesTuple() == other.GetFieldValuesTuple()

  def __ne__(self, other):
    return not self.__eq__(other)

  def __repr__(self):
    dictionary = {}
    for field in Stop._FIELD_NAMES:
      dictionary[field] = getattr(self, field)
    return unicode(dictionary)

  def Validate(self, problems=default_problem_reporter):
    if IsEmpty(self.stop_id):
      problems.MissingValue('stop_id')
    if IsEmpty(self.stop_name):
      problems.MissingValue('stop_name')
    if abs(self.stop_lat) > 90.0:
      problems.InvalidValue('stop_lat', self.stop_lat)
    if abs(self.stop_lon) > 180.0:
      problems.InvalidValue('stop_lon', self.stop_lon)
    if (abs(self.stop_lat) < 1.0) and (abs(self.stop_lon) < 1.0):
      problems.InvalidValue('stop_lat', self.stop_lat,
                            'Stop location too close to 0, 0')
    if hasattr(self, 'stop_url') and self.stop_url and not IsValidURL(self.stop_url):
      problems.InvalidValue('stop_url', self.stop_url)
    if hasattr(self, 'stop_desc') and (not IsEmpty(self.stop_desc) and
        self.stop_name.strip().lower() == self.stop_desc.strip().lower()):
      problems.InvalidValue('stop_desc', self.stop_desc,
                            'stop_desc should not be the same as stop_name')



class Route(object):
  """Represents a single route."""

  _REQUIRED_FIELD_NAMES = [
    'route_id', 'route_short_name', 'route_long_name', 'route_type'
    ]
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + [
    'agency_id', 'route_desc', 'route_url', 'route_color', 'route_text_color'
    ]
  _ROUTE_TYPE_IDS = {
    'Tram': 0,
    'Subway': 1,
    'Rail': 2,
    'Bus': 3,
    'Ferry': 4,
    'Cable Car': 5,
    'Gondola': 6,
    'Funicular': 7
    }

  def __init__(self, short_name=None, long_name=None, route_type=None,
               route_id=None, agency_id=None, field_list=None):
    self.route_desc = ''
    self.route_url = ''
    self.route_color = ''
    self.route_text_color = ''
    self.route_type = -1
    if field_list:
      (route_id, short_name, long_name, route_type,
       agency_id, self.route_desc, self.route_url,
       self.route_color, self.route_text_color) = field_list
    self.route_id = route_id
    self.route_short_name = short_name
    self.route_long_name = long_name
    self.agency_id = agency_id

    if route_type in Route._ROUTE_TYPE_IDS:
      self.route_type = Route._ROUTE_TYPE_IDS[route_type]
    else:
      try:
        self.route_type = int(route_type)
      except TypeError:
        self.route_type = route_type
      except ValueError:
        self.route_type = route_type
    self.trips = []

  def GetFieldValuesTuple(self):
    return [getattr(self, fn) for fn in Route._FIELD_NAMES]

  def AddTrip(self, schedule, headsign, service_period=None, trip_id=None):
    """ Adds a trip to this route.

    Args:
      headsign: headsign of the trip as a string

    Returns:
      a new Trip object
    """
    if trip_id is None:
      trip_id = unicode(len(schedule.trips))
    if service_period is None:
      service_period = schedule.GetDefaultServicePeriod()
    trip = Trip(route=self, headsign=headsign, service_period=service_period,
                trip_id=trip_id)
    schedule.AddTripObject(trip)
    return trip

  def AddTripObject(self, trip):
    self.trips.append(trip)

  def GetPatternIdTripDict(self):
    """Return a dictionary that maps pattern_id to a list of Trip objects."""
    d = {}
    for t in self.trips:
      d.setdefault(t.pattern_id, []).append(t)
    return d

  def __getitem__(self, name):
    return getattr(self, name)

  def __eq__(self, other):
    if not other:
      return False

    if id(self) == id(other):
      return True

    return self.GetFieldValuesTuple() == other.GetFieldValuesTuple()

  def __ne__(self, other):
    return not self.__eq__(other)

  def __repr__(self):
    dictionary = {}
    for field in Route._FIELD_NAMES:
      dictionary[field] = getattr(self, field)
    return unicode(dictionary)

  def Validate(self, problems=default_problem_reporter):
    if IsEmpty(self.route_id):
      problems.MissingValue('route_id')
    if IsEmpty(self.route_short_name) and IsEmpty(self.route_long_name):
      problems.InvalidValue('route_short_name',
                            self.route_short_name,
                            'Both route_short_name and '
                            'route_long name are blank.')
                            
    if self.route_short_name and len(self.route_short_name) > 5:
      problems.InvalidValue('route_short_name',
                            self.route_short_name,
                            'This route_short_name is relatively long, which '
                            'probably means that it contains a place name.  '
                            'You should only use this field to hold a short '
                            'code that riders use to identify a route.  '
                            'If this route doesn\'t have such a code, it\'s '
                            'OK to leave this field empty.')
                            
    if (self.route_short_name and
        (self.route_long_name.strip().lower().startswith(
            self.route_short_name.strip().lower() + ' ') or
         self.route_long_name.strip().lower().startswith(
            self.route_short_name.strip().lower() + '-'))):
      problems.InvalidValue('route_long_name',
                            self.route_long_name,
                            'route_long_name shouldn\'t contain '
                            'the route_short_name value, as both '
                            'fields are often displayed '
                            'side-by-side.')
    if (self.route_short_name and
        (self.route_long_name.strip().lower() ==
         self.route_short_name.strip().lower())):
      problems.InvalidValue('route_long_name',
                            self.route_long_name,
                            'route_long_name shouldn\'t be the same '
                            'the route_short_name value, as both '
                            'fields are often displayed '
                            'side-by-side.  It\'s OK to omit either the '
                            'short or long name (but not both).')
    if (self.route_desc and
        ((self.route_desc == self.route_short_name) or
         (self.route_desc == self.route_long_name))):
      problems.InvalidValue('route_desc',
                            self.route_desc,
                            'route_desc shouldn\'t be the same as '
                            'route_short_name or route_long_name')
    if (type(self.route_type) != type(0) or
        self.route_type not in range(len(Route._ROUTE_TYPE_IDS))):
      problems.InvalidValue('route_type', self.route_type)
    if self.route_url and not IsValidURL(self.route_url):
      problems.InvalidValue('route_url', self.route_url)
    if self.route_color and not IsValidColor(self.route_color):
      problems.InvalidValue('route_color', self.route_color)
    if (self.route_text_color and not IsValidColor(self.route_text_color)):
      problems.InvalidValue('route_text_color',
                            self.route_text_color)


def SortListOfTripByTime(trips):
  trips.sort(key=Trip.GetStartTime)


class StopTime(object):
  """
  Represents a single stop of a trip. StopTime contains most of the columns
  from the stop_times.txt file. It does not contain trip_id or stop_sequence,
  which are implied by its position in a Trip._stoptimes list.

  See the Google Transit Feed Specification for the semantic details.

  stop: A Stop object
  arrival_time: str in the form HH:MM:SS; readonly after __init__
  departure_time: str in the form HH:MM:SS; readonly after __init__
  arrival_secs: int number of seconds since midnight
  departure_secs: int number of seconds since midnight
  stop_headsign: str
  pickup_type: int
  drop_off_type: int
  shape_dist_traveled: float
  stop_id: str; readonly
  """
  _REQUIRED_FIELD_NAMES = ['trip_id', 'arrival_time', 'departure_time',
                           'stop_id', 'stop_sequence']
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ['stop_headsign', 'pickup_type',
                    'drop_off_type', 'shape_dist_traveled']

  def __init__(self, problems, stop, arrival_time=None, departure_time=None,
               stop_headsign=None, pickup_type=None, drop_off_type=None,
               shape_dist_traveled=None, arrival_secs=None,
               departure_secs=None):
    if arrival_secs != None:
      self.arrival_secs = arrival_secs
    elif arrival_time in (None, ""):
      self.arrival_secs = None  # Untimed
    else:
      try:
        self.arrival_secs = TimeToSecondsSinceMidnight(arrival_time)
      except Error:
        problems.InvalidValue('arrival_time', arrival_time)

    if departure_secs != None:
      self.departure_secs = departure_secs
    elif departure_time in (None, ""):
      self.departure_secs = None
    else:
      try:
        self.departure_secs = TimeToSecondsSinceMidnight(departure_time)
      except Error:
        problems.InvalidValue('departure_time', departure_time)

    if not isinstance(stop, Stop):
      # Not quite correct, but better than letting the problem propagate
      problems.InvalidValue('stop', stop)
    self.stop = stop
    self.stop_headsign = stop_headsign

    if pickup_type in (None, ""):
      self.pickup_type = None
    else:
      try:
        pickup_type = int(pickup_type)
      except ValueError:
        problems.InvalidValue('pickup_type', pickup_type)
      if pickup_type < 0 or pickup_type > 3:
        problems.InvalidValue('pickup_type', pickup_type)
      self.pickup_type = pickup_type

    if drop_off_type in (None, ""):
      self.drop_off_type = None
    else:
      try:
        drop_off_type = int(drop_off_type)
      except ValueError:
        problems.InvalidValue('drop_off_type', drop_off_type)
      if drop_off_type < 0 or drop_off_type > 3:
        problems.InvalidValue('drop_off_type', drop_off_type)
      self.drop_off_type = drop_off_type

    if shape_dist_traveled in (None, ""):
      self.shape_dist_traveled = None
    else:
      try:
        self.shape_dist_traveled = float(shape_dist_traveled)
      except ValueError:
        problems.InvalidValue('shape_dist_traveled', shape_dist_traveled)

  def GetFieldValuesTuple(self, trip_id, sequence):
    """Return a tuple that outputs a row of _FIELD_NAMES.

    trip and sequence must be provided because they are not stored in StopTime.
    """
    result = []
    for fn in StopTime._FIELD_NAMES:
      if fn == 'trip_id':
        result.append(trip_id)
      elif fn == 'stop_sequence':
        result.append(str(sequence))
      else:
        result.append(getattr(self, fn) or '' )
    return tuple(result)

  def GetTimeSecs(self):
    """Return the first of arrival_secs and departure_secs that is not None.
    If both are None return None."""
    if self.arrival_secs != None:
      return self.arrival_secs
    elif self.departure_secs != None:
      return self.departure_secs
    else:
      return None

  def __getattr__(self, name):
    if name == 'stop_id':
      return self.stop.stop_id
    elif name == 'arrival_time':
      return (self.arrival_secs != None and
          FormatSecondsSinceMidnight(self.arrival_secs) or '')
    elif name == 'departure_time':
      return (self.departure_secs != None and
          FormatSecondsSinceMidnight(self.departure_secs) or '')
    raise AttributeError(name)


class Trip(object):
  _REQUIRED_FIELD_NAMES = ['route_id', 'service_id', 'trip_id']
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + [
    'trip_headsign', 'direction_id', 'block_id', 'shape_id'
    ]
  _FIELD_NAMES_HEADWAY = ['trip_id', 'start_time', 'end_time', 'headway_secs']


  def __init__(self, headsign=None, service_period=None,
               route=None, trip_id=None, field_list=None):
    self._stoptimes = []  # [StopTime, StopTime, ...]
    self._headways = []  # [(start_time, end_time, headway_secs)]
    self.trip_headsign = headsign
    self.shape_id = None
    if route:
      self.route_id = route.route_id
    self.trip_id = trip_id
    self.service_period = service_period
    self.direction_id = None
    self.block_id = None
    if field_list:
      (self.route_id, self.service_id, self.trip_id, self.trip_headsign,
       self.direction_id, self.block_id, self.shape_id) = field_list

  def GetFieldValuesTuple(self):
    return [getattr(self, fn) or '' for fn in Trip._FIELD_NAMES]

  def AddStopTime(self, stop, problems=default_problem_reporter, **kwargs):
    """Add a stop to this trip. Stops must be added in the order visited.

    Args:
      stop: A Stop object
      kwargs: remaining keyword args passed to StopTime.__init__

    Returns:
      None
    """
    stoptime = StopTime(problems=problems, stop=stop, **kwargs)
    self.AddStopTimeObject(stoptime, problems=problems)

  def AddStopTimeObject(self, stoptime, problems=default_problem_reporter):
    """Add a StopTime object to the end of this trip.

    Args:
      stoptime: A StopTime object. Should not be reused in multiple trips.

    Returns:
      None
    """
    new_secs = stoptime.GetTimeSecs()
    prev_secs = None
    for st in reversed(self._stoptimes):
      prev_secs = st.GetTimeSecs()
      if prev_secs != None:
        break
    if new_secs != None and prev_secs != None and new_secs < prev_secs:
      problems.OtherProblem('out of order stop time for stop_id=%s trip_id=%s %s < %s'
                            % (stoptime.stop_id, self.trip_id,
                               FormatSecondsSinceMidnight(new_secs),
                               FormatSecondsSinceMidnight(prev_secs)))
    else:
      stoptime.stop._AddTripStop(self, len(self._stoptimes))
      self._stoptimes.append(stoptime)

  def GetTimeStops(self):
    """Return a list of (arrival_secs, departure_secs, stop) tuples.

    Caution: arrival_secs and departure_secs may be 0, a false value meaning a
    stop at midnight or None, a false value meaning the stop is untimed."""
    return [(st.arrival_secs, st.departure_secs, st.stop) for st in self._stoptimes]

  def GetStopTimes(self):
    """Return a sorted list of StopTime objects for this trip."""
    return self._stoptimes

  def GetStartTime(self):
    """Return the first time of the trip. TODO: For trips defined by frequency
    return the first time of the first trip."""
    if self._stoptimes[0].arrival_secs is not None:
      return self._stoptimes[0].arrival_secs
    elif self._stoptimes[0].departure_secs is not None:
      return self._stoptimes[0].departure_secs
    else:
      raise Error("Trip without valid first time %s" % self.trip_id)

  def GetEndTime(self):
    """Return the last time of the trip. TODO: For trips defined by frequency
    return the last time of the last trip."""
    if self._stoptimes[-1].departure_secs is not None:
      return self._stoptimes[-1].departure_secs
    elif self._stoptimes[-1].arrival_secs is not None:
      return self._stoptimes[-1].arrival_secs
    else:
      raise Error("Trip without valid last time %s" % self.trip_id)

  def _GenerateStopTimesTuples(self):
    """Generator for rows of the stop_times file"""
    for i, st in enumerate(self._stoptimes):
      # sequence is 1-based index
      yield st.GetFieldValuesTuple(self.trip_id, i + 1)

  def GetStopTimesTuples(self):
    results = []
    for time_tuple in self._GenerateStopTimesTuples():
      results.append(time_tuple)
    return results

  def GetPattern(self):
    """Return a tuple of Stop objects, in the order visited"""
    return tuple(timestop.stop for timestop in self._stoptimes)

  def AddHeadwayPeriod(self, start_time, end_time, headway_secs,
                       problem_reporter=default_problem_reporter):
    """Adds a period to this trip during which the vehicle travels
    at regular intervals (rather than specifying exact times for each stop).

    Args:
      start_time: The time at which this headway period starts, either in
          numerical seconds since midnight or as "HH:MM:SS" since midnight.
      end_time: The time at which this headway period ends, either in
          numerical seconds since midnight or as "HH:MM:SS" since midnight.
          This value should be larger than start_time.
      headway_secs: The amount of time, in seconds, between occurences of
          this trip.
      problem_reporter: Optional parameter that can be used to select
          how any errors in the other input parameters will be reported.
    Returns:
      None
    """
    if start_time == None or start_time == '':  # 0 is OK
      problem_reporter.MissingValue('start_time')
      return
    if isinstance(start_time, basestring):
      try:
        start_time = TimeToSecondsSinceMidnight(start_time)
      except Error:
        problem_reporter.InvalidValue('start_time', start_time)
        return
    elif start_time < 0:
      problem_reporter.InvalidValue('start_time', start_time)

    if end_time == None or end_time == '':
      problem_reporter.MissingValue('end_time')
      return
    if isinstance(end_time, basestring):
      try:
        end_time = TimeToSecondsSinceMidnight(end_time)
      except Error:
        problem_reporter.InvalidValue('end_time', end_time)
        return
    elif end_time < 0:
      problem_reporter.InvalidValue('end_time', end_time)
      return

    if not headway_secs:
      problem_reporter.MissingValue('headway_secs')
      return
    try:
      headway_secs = int(headway_secs)
    except ValueError:
      problem_reporter.InvalidValue('headway_secs', headway_secs)
      return

    if headway_secs <= 0:
      problem_reporter.InvalidValue('headway_secs', headway_secs)
      return

    if end_time <= start_time:
      problem_reporter.InvalidValue('end_time', end_time,
                                    'should be greater than start_time')

    self._headways.append((start_time, end_time, headway_secs))

  def ClearHeadwayPeriods(self):
    self._headways = []

  def _HeadwayOutputTuple(self, headway):
      return (self.trip_id,
              FormatSecondsSinceMidnight(headway[0]),
              FormatSecondsSinceMidnight(headway[1]),
              unicode(headway[2]))

  def GetHeadwayPeriodOutputTuples(self):
    tuples = []
    for headway in self._headways:
      tuples.append(self._HeadwayOutputTuple(headway))
    return tuples

  def GetHeadwayPeriodTuples(self):
    return self._headways

  def __getattr__(self, name):
    if name == 'service_id':
      return self.service_period.service_id
    elif name == 'pattern_id':
      if 'pattern_id' not in self.__dict__:
        self.__dict__['pattern_id'] = hash(self.GetPattern())
      return self.__dict__['pattern_id']
    raise AttributeError(name)

  def __getitem__(self, name):
    return getattr(self, name)

  def __eq__(self, other):
    if not other:
      return False
    if id(self) == id(other):
      return True

    if self.GetFieldValuesTuple() != other.GetFieldValuesTuple():
      return False
    if self.GetStopTimesTuples() != other.GetStopTimesTuples():
      return False

    return True

  def __ne__(self, other):
    return not self.__eq__(other)

  def __repr__(self):
    dictionary = {}
    for field in Trip._FIELD_NAMES:
      dictionary[field] = getattr(self, field)
    return "%s with trips: %s" % (dictionary, self.GetStopTimesTuples())

  def Validate(self, problems=default_problem_reporter):
    if IsEmpty(self.route_id):
      problems.MissingValue('route_id')
    if IsEmpty(self.service_id):
      problems.MissingValue('service_id')
    if IsEmpty(self.trip_id):
      problems.MissingValue('trip_id')
    if hasattr(self, 'direction_id') and (not IsEmpty(self.direction_id)) and \
        (self.direction_id != '0') and (self.direction_id != '1'):
      problems.InvalidValue('direction_id', self.direction_id,
                            'direction_id must be "0" or "1"')
    # O(n^2), but we don't anticipate many headway periods per trip
    for headway_index, headway in enumerate(self._headways[0:-1]):
      for other in self._headways[headway_index + 1:]:
        if (other[0] < headway[1]) and (other[1] > headway[0]):
          problems.OtherProblem('Trip contains overlapping headway periods '
                                '%s and %s' %
                                (self._HeadwayOutputTuple(headway),
                                 self._HeadwayOutputTuple(other)))

# TODO: move these into a separate file
class ISO4217(object):
  """Represents the set of currencies recognized by the ISO-4217 spec."""
  codes = {  # map of alpha code to numerical code
    'AED': 784, 'AFN': 971, 'ALL':   8, 'AMD':  51, 'ANG': 532, 'AOA': 973,
    'ARS':  32, 'AUD':  36, 'AWG': 533, 'AZN': 944, 'BAM': 977, 'BBD':  52,
    'BDT':  50, 'BGN': 975, 'BHD':  48, 'BIF': 108, 'BMD':  60, 'BND':  96,
    'BOB':  68, 'BOV': 984, 'BRL': 986, 'BSD':  44, 'BTN':  64, 'BWP':  72,
    'BYR': 974, 'BZD':  84, 'CAD': 124, 'CDF': 976, 'CHE': 947, 'CHF': 756,
    'CHW': 948, 'CLF': 990, 'CLP': 152, 'CNY': 156, 'COP': 170, 'COU': 970,
    'CRC': 188, 'CUP': 192, 'CVE': 132, 'CYP': 196, 'CZK': 203, 'DJF': 262,
    'DKK': 208, 'DOP': 214, 'DZD':  12, 'EEK': 233, 'EGP': 818, 'ERN': 232,
    'ETB': 230, 'EUR': 978, 'FJD': 242, 'FKP': 238, 'GBP': 826, 'GEL': 981,
    'GHC': 288, 'GIP': 292, 'GMD': 270, 'GNF': 324, 'GTQ': 320, 'GYD': 328,
    'HKD': 344, 'HNL': 340, 'HRK': 191, 'HTG': 332, 'HUF': 348, 'IDR': 360,
    'ILS': 376, 'INR': 356, 'IQD': 368, 'IRR': 364, 'ISK': 352, 'JMD': 388,
    'JOD': 400, 'JPY': 392, 'KES': 404, 'KGS': 417, 'KHR': 116, 'KMF': 174,
    'KPW': 408, 'KRW': 410, 'KWD': 414, 'KYD': 136, 'KZT': 398, 'LAK': 418,
    'LBP': 422, 'LKR': 144, 'LRD': 430, 'LSL': 426, 'LTL': 440, 'LVL': 428,
    'LYD': 434, 'MAD': 504, 'MDL': 498, 'MGA': 969, 'MKD': 807, 'MMK': 104,
    'MNT': 496, 'MOP': 446, 'MRO': 478, 'MTL': 470, 'MUR': 480, 'MVR': 462,
    'MWK': 454, 'MXN': 484, 'MXV': 979, 'MYR': 458, 'MZN': 943, 'NAD': 516,
    'NGN': 566, 'NIO': 558, 'NOK': 578, 'NPR': 524, 'NZD': 554, 'OMR': 512,
    'PAB': 590, 'PEN': 604, 'PGK': 598, 'PHP': 608, 'PKR': 586, 'PLN': 985,
    'PYG': 600, 'QAR': 634, 'ROL': 642, 'RON': 946, 'RSD': 941, 'RUB': 643,
    'RWF': 646, 'SAR': 682, 'SBD':  90, 'SCR': 690, 'SDD': 736, 'SDG': 938,
    'SEK': 752, 'SGD': 702, 'SHP': 654, 'SKK': 703, 'SLL': 694, 'SOS': 706,
    'SRD': 968, 'STD': 678, 'SYP': 760, 'SZL': 748, 'THB': 764, 'TJS': 972,
    'TMM': 795, 'TND': 788, 'TOP': 776, 'TRY': 949, 'TTD': 780, 'TWD': 901,
    'TZS': 834, 'UAH': 980, 'UGX': 800, 'USD': 840, 'USN': 997, 'USS': 998,
    'UYU': 858, 'UZS': 860, 'VEB': 862, 'VND': 704, 'VUV': 548, 'WST': 882,
    'XAF': 950, 'XAG': 961, 'XAU': 959, 'XBA': 955, 'XBB': 956, 'XBC': 957,
    'XBD': 958, 'XCD': 951, 'XDR': 960, 'XFO': None, 'XFU': None, 'XOF': 952,
    'XPD': 964, 'XPF': 953, 'XPT': 962, 'XTS': 963, 'XXX': 999, 'YER': 886,
    'ZAR': 710, 'ZMK': 894, 'ZWD': 716,
  }


class Fare(object):
  """Represents a fare type."""
  _REQUIRED_FIELD_NAMES = ['fare_id', 'price', 'currency_type',
                           'payment_method', 'transfers']
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ['transfer_duration']

  def __init__(self,
               fare_id=None, price=None, currency_type=None,
               payment_method=None, transfers=None, transfer_duration=None,
               field_list=None):
    self.rules = []
    (self.fare_id, self.price, self.currency_type, self.payment_method,
     self.transfers, self.transfer_duration) = \
     (fare_id, price, currency_type, payment_method,
      transfers, transfer_duration)
    if field_list:
      (self.fare_id, self.price, self.currency_type, self.payment_method,
       self.transfers, self.transfer_duration) = field_list

    try:
      self.price = float(self.price)
    except (TypeError, ValueError):
      pass
    try:
      self.payment_method = int(self.payment_method)
    except (TypeError, ValueError):
      pass
    if self.transfers == None or self.transfers == "":
      self.transfers = None
    else:
      try:
        self.transfers = int(self.transfers)
      except (TypeError, ValueError):
        pass
    if self.transfer_duration == None or self.transfer_duration == "":
      self.transfer_duration = None
    else:
      try:
        self.transfer_duration = int(self.transfer_duration)
      except (TypeError, ValueError):
        pass

  def GetFareRuleList(self):
    return self.rules

  def ClearFareRules(self):
    self.rules = []

  def GetFieldValuesTuple(self):
    return [getattr(self, fn) for fn in Fare._FIELD_NAMES]

  def __getitem__(self, name):
    return getattr(self, name)

  def __eq__(self, other):
    if not other:
      return False

    if id(self) == id(other):
      return True

    if self.GetFieldValuesTuple() != other.GetFieldValuesTuple():
      return False

    return self.GetFareRuleList() == other.GetFareRuleList()

  def __ne__(self, other):
    return not self.__eq__(other)

  def Validate(self, problems=default_problem_reporter):
    if IsEmpty(self.fare_id):
      problems.MissingValue("fare_id")

    if self.price == None:
      problems.MissingValue("price")
    if not isinstance(self.price, float) and not isinstance(self.price, int):
      problems.InvalidValue("price", self.price)
    elif self.price < 0:
      problems.InvalidValue("price", self.price)

    if IsEmpty(self.currency_type):
      problems.MissingValue("currency_type")
    if self.currency_type not in ISO4217.codes:
      problems.InvalidValue("currency_type", self.currency_type)

    if self.payment_method == "" or self.payment_method == None:
      problems.MissingValue("payment_method")
    elif (not isinstance(self.payment_method, int) or
          self.payment_method not in range(0, 2)):
      problems.InvalidValue("payment_method", self.payment_method)

    if not ((self.transfers == None) or
            (isinstance(self.transfers, int) and
             self.transfers in range(0, 3))):
      problems.InvalidValue("transfers", self.transfers)

    if ((self.transfer_duration != None) and
        not isinstance(self.transfer_duration, int)):
      problems.InvalidValue("transfer_duration", self.transfer_duration)
    if self.transfer_duration and (self.transfer_duration < 0):
      problems.InvalidValue("transfer_duration", self.transfer_duration)
    if (self.transfer_duration and (self.transfer_duration > 0) and
        self.transfers == 0):
      problems.InvalidValue("transfer_duration", self.transfer_duration,
                            "can't have a nonzero transfer_duration for "
                            "a fare that doesn't allow transfers!")


class FareRule(object):
  """This class represents a rule that determines which itineraries a
  fare rule applies to."""
  _REQUIRED_FIELD_NAMES = ['fare_id']
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ['route_id',
                                         'origin_id', 'destination_id',
                                         'contains_id']

  def __init__(self, fare_id=None, route_id=None,
               origin_id=None, destination_id=None, contains_id=None,
               field_list=None):
    (self.fare_id, self.route_id, self.origin_id, self.destination_id,
     self.contains_id) = \
     (fare_id, route_id, origin_id, destination_id, contains_id)
    if field_list:
      (self.fare_id, self.route_id, self.origin_id, self.destionation_id,
       self.contains_id) = field_list

    # canonicalize non-content values as None
    if not self.route_id:
      self.route_id = None
    if not self.origin_id:
      self.origin_id = None
    if not self.destination_id:
      self.destination_id = None
    if not self.contains_id:
      self.contains_id = None

  def GetFieldValuesTuple(self):
    return [getattr(self, fn) for fn in FareRule._FIELD_NAMES]

  def __getitem__(self, name):
    return getattr(self, name)

  def __eq__(self, other):
    if not other:
      return False

    if id(self) == id(other):
      return True

    return self.GetFieldValuesTuple() == other.GetFieldValuesTuple()

  def __ne__(self, other):
    return not self.__eq__(other)


class Shape(object):
  """This class represents a geographic shape that corresponds to the route
  taken by one or more Trips."""
  _REQUIRED_FIELD_NAMES = ['shape_id', 'shape_pt_lat', 'shape_pt_lon',
                           'shape_pt_sequence']
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ['shape_dist_traveled']
  def __init__(self, shape_id):
    self.points = []
    self.shape_id = shape_id
    self.max_distance = 0

  def AddPoint(self, lat, lon, distance=None,
               problems=default_problem_reporter):

    try:
      lat = float(lat)
      if abs(lat) > 90.0:
        problems.InvalidValue('shape_pt_lat', lat)
        return
    except (TypeError, ValueError):
      problems.InvalidValue('shape_pt_lat', lat)
      return

    try:
      lon = float(lon)
      if abs(lon) > 180.0:
        problems.InvalidValue('shape_pt_lon', lon)
        return
    except (TypeError, ValueError):
      problems.InvalidValue('shape_pt_lon', lon)
      return

    if (abs(lat) < 1.0) and (abs(lon) < 1.0):
      problems.InvalidValue('shape_pt_lat', lat,
                            'Point location too close to 0, 0, which means '
                            'that it\'s probably an incorrect location.')
      return

    if distance == '':  # canonicalizing empty string to None for comparison
      distance = None

    if distance != None:
      try:
        distance = float(distance)
        if (distance < self.max_distance and not
            (len(self.points) == 0 and distance == 0)):  # first one can be 0
          problems.InvalidValue('shape_dist_traveled', distance,
                                'Each subsequent point in a shape should '
                                'have a distance value that\'s at least as '
                                'large as the previous ones.  In this case, '
                                'the previous distance was %f.' % distance)
          return
        else:
          self.max_distance = distance
      except (TypeError, ValueError):
        problems.InvalidValue('shape_dist_traveled', distance,
                              'This value should be a positive number.')
        return

    self.points.append((lat, lon, distance))

  def ClearPoints(self):
    self.points = []

  def __eq__(self, other):
    if not other:
      return False

    if id(self) == id(other):
      return True

    return self.points == other.points

  def __ne__(self, other):
    return not self.__eq__(other)

  def __repr__(self):
    return unicode(self.__dict__)

  def Validate(self, problems=default_problem_reporter):
    if IsEmpty(self.shape_id):
      problems.MissingValue('shape_id')

    if not self.points:
      problems.OtherProblem('The shape with shape_id "%s" contains no points.' %
                            self.shape_id)

class Agency(object):
  """Represents an agency in a schedule"""
  _REQUIRED_FIELD_NAMES = ['agency_name', 'agency_url', 'agency_timezone']
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ['agency_id']

  def __init__(self, name=None, url=None, timezone=None, id=None,
               field_list=None, agency_url=None, agency_name=None,
               agency_timezone=None, agency_id=None):
    if field_list:
      for fn, fv in zip(Agency._FIELD_NAMES, field_list):
        self.__dict__[fn] = fv
    else:
      self.agency_name = name or agency_name
      self.agency_url = url or agency_url
      self.agency_timezone = timezone or agency_timezone
      self.agency_id = id or agency_id

  def GetFieldValuesTuple(self):
    return [getattr(self, fn) for fn in Agency._FIELD_NAMES]

  def __getitem__(self, name):
    return getattr(self, name)

  def __eq__(self, other):
    if not other:
      return False

    if id(self) == id(other):
      return True

    return self.GetFieldValuesTuple() == other.GetFieldValuesTuple()

  def __ne__(self, other):
    return not self.__eq__(other)

  def __repr__(self):
    dictionary = {}
    for field in Agency._FIELD_NAMES:
      dictionary[field] = getattr(self, field)
    return unicode(dictionary)

  def Validate(self, problems=default_problem_reporter):
    if IsEmpty(self.agency_name):
      problems.MissingValue('agency_name')
      return False
    if IsEmpty(self.agency_url):
      problems.MissingValue('agency_url')
      return False
    elif not IsValidURL(self.agency_url):
      problems.InvalidValue('agency_url', self.agency_url)
      return False

    try:
      from pytz import common_timezones
      if self.agency_timezone not in common_timezones:
        problems.InvalidValue('agency_timezone',
                              self.agency_timezone,
                              '"%s" isn\'t a recognized time zone')
        return False
    except ImportError:  # no pytz
      print ("Timezone not checked "
             "(install pytz package for timezone validation)")
    return True


class ServicePeriod(object):
  """Represents a service, which identifies a set of dates when one or more
  trips operate."""
  _DAYS_OF_WEEK = [
    'monday', 'tuesday', 'wednesday', 'thursday', 'friday',
    'saturday', 'sunday'
    ]
  _FIELD_NAMES_REQUIRED = [
    'service_id', 'start_date', 'end_date'
    ] + _DAYS_OF_WEEK
  _FIELD_NAMES = _FIELD_NAMES_REQUIRED  # no optional fields in this one
  _FIELD_NAMES_CALENDAR_DATES = ['service_id', 'date', 'exception_type']

  def __init__(self, id=None, field_list=None):
    self.original_day_values = []
    if field_list:
      self.service_id = field_list[self._FIELD_NAMES.index('service_id')]
      self.day_of_week = [False] * len(self._DAYS_OF_WEEK)

      for day in self._DAYS_OF_WEEK:
        value = field_list[self._FIELD_NAMES.index(day)] or ''  # can be None
        self.original_day_values += [value.strip()]
        self.day_of_week[self._DAYS_OF_WEEK.index(day)] = (value == u'1')

      self.start_date = field_list[self._FIELD_NAMES.index('start_date')]
      self.end_date = field_list[self._FIELD_NAMES.index('end_date')]
    else:
      self.service_id = id
      self.day_of_week = [False] * 7
      self.start_date = None
      self.end_date = None
    self.date_exceptions = {}  # Map from 'YYYYMMDD' to 1 (add) or 2 (remove)

  def _IsValidDate(self, date):
    # TODO: Add more knowledge of possible dates here
    return not (re.match('^\d{8}$', date) == None)

  def GetCalendarFieldValuesTuple(self):
    """Return the tuple of calendar.txt values or None if this ServicePeriod
    should not be in calendar.txt ."""
    if self.start_date and self.end_date:
      return [getattr(self, fn) for fn in ServicePeriod._FIELD_NAMES]

  def GenerateCalendarDatesFieldValuesTuples(self):
    """Generates tuples of calendar_dates.txt values. Yield zero tuples if
    this ServicePeriod should not be in calendar_dates.txt ."""
    for date, exception_type in self.date_exceptions.items():
      yield (self.service_id, date, unicode(exception_type))

  def GetCalendarDatesFieldValuesTuples(self):
    """Return a list of date execeptions"""
    result = []
    for date_tuple in self.GenerateCalendarDatesFieldValuesTuples():
      result.append(date_tuple)
    result.sort()  # helps with __eq__
    return result

  def SetDateHasService(self, date, has_service=True):
    self.date_exceptions[date] = has_service and 1 or 2

  def ResetDateToNormalService(self, date):
    if date in self.date_exceptions:
      del self.date_exceptions[date]

  def SetStartDate(self, start_date):
    """Set the first day of service as a string in YYYYMMDD format"""
    self.start_date = start_date

  def SetEndDate(self, end_date):
    """Set the last day of service as a string in YYYYMMDD format"""
    self.end_date = end_date

  def SetDayOfWeekHasService(self, dow, has_service=True):
    """Set service as running (or not) on a day of the week. By default the
    service does not run on any days.

    Args:
      dow: 0 for Monday through 6 for Sunday
      has_service: True if this service operates on dow, False if it does not.

    Returns:
      None
    """
    assert(dow >= 0 and dow < 7)
    self.day_of_week[dow] = has_service

  def SetWeekdayService(self, has_service=True):
    """Set service as running (or not) on all of Monday through Friday."""
    for i in range(0, 5):
      self.SetDayOfWeekHasService(i, has_service)

  def SetWeekendService(self, has_service=True):
    """Set service as running (or not) on Saturday and Sunday."""
    self.SetDayOfWeekHasService(5, has_service)
    self.SetDayOfWeekHasService(6, has_service)

  def SetServiceId(self, service_id):
    """Set the service_id for this schedule. Generally the default will
    suffice so you won't need to call this method."""
    self.service_id = service_id

  def __getattr__(self, name):
    try:
      # Return 1 if value in day_of_week is True, 0 otherwise
      return (self.day_of_week[ServicePeriod._DAYS_OF_WEEK.index(name)]
              and 1 or 0)
    except KeyError:
      pass
    except ValueError:  # not a day of the week
      pass
    raise AttributeError(name)

  def __getitem__(self, name):
    return getattr(self, name)

  def __eq__(self, other):
    if not other:
      return False

    if id(self) == id(other):
      return True

    if (self.GetCalendarFieldValuesTuple() !=
        other.GetCalendarFieldValuesTuple()):
      return False

    if (self.GetCalendarDatesFieldValuesTuples() !=
        other.GetCalendarDatesFieldValuesTuples()):
      return False

    return True

  def __ne__(self, other):
    return not self.__eq__(other)

  def Validate(self, problems=default_problem_reporter):
    if IsEmpty(self.service_id):
      problems.MissingValue('service_id')
    if IsEmpty(self.start_date) and not IsEmpty(self.end_date):
      problems.MissingValue('start_date')
    if IsEmpty(self.end_date) and not IsEmpty(self.start_date):
      problems.MissingValue('end_date')
    if not IsEmpty(self.start_date) and not self._IsValidDate(self.start_date):
      problems.InvalidValue('start_date', self.start_date)
    if not IsEmpty(self.end_date) and not self._IsValidDate(self.end_date):
      problems.InvalidValue('end_date', self.end_date)
    if (not IsEmpty(self.start_date) and not IsEmpty(self.end_date) and
        self.end_date < self.start_date):
      problems.InvalidValue('end_date', self.end_date,
                            'end_date of %s is earlier than '
                            'start_date of "%s"' %
                            (self.end_date, self.start_date))
    if self.original_day_values:
      index = 0
      for value in self.original_day_values:
        field_name = self._DAYS_OF_WEEK[index]
        if IsEmpty(value):
          problems.MissingValue(field_name)
        elif (value != u'0') and (value != '1'):
          problems.InvalidValue(field_name, value)
        index += 1
    if (True not in self.day_of_week and
        1 not in self.date_exceptions.values()):
      problems.OtherProblem('Service period with service_id "%s" '
                            'doesn\'t have service on any days '
                            'of the week.' % self.service_id)
    for date in self.date_exceptions:
      if not self._IsValidDate(date):
        problems.InvalidValue('date', date)


class CsvUnicodeWriter:
  """
  Create a wrapper around a csv writer object which can safely write unicode
  values. Passes all arguments to csv.writer.
  """
  def __init__(self, *args, **kwargs):
    self.writer = csv.writer(*args, **kwargs)

  def writerow(self, row):
    """Write row to the csv file. Any unicode strings in row are encoded as
    utf-8."""
    encoded_row = []
    for s in row:
      if isinstance(s, unicode):
        encoded_row.append(s.encode("utf-8"))
      else:
        encoded_row.append(s)
    try:
      self.writer.writerow(encoded_row)
    except Exception, e:
      print 'error writing %s as %s' % (row, encoded_row)
      raise e

  def writerows(self, rows):
    """Write rows to the csv file. Any unicode strings in rows are encoded as
    utf-8."""
    for row in rows:
      self.writerow(row)

  def __getattr__(self, name):
    return getattr(self.writer, name)


class Schedule:
  """Represents a Schedule, a collection of stops, routes, trips and
  an agency.  This is the main class for this module."""

  def __init__(self, problem_reporter=default_problem_reporter):
    self._agencies = {}
    self.stops = {}
    self.routes = {}
    self.trips = {}
    self.service_periods = {}
    self.fares = {}
    self.fare_zones = {}  # represents the set of all known fare zones
    self._shapes = {}  # shape_id to Shape
    self._default_service_period = None
    self._default_agency = None
    self.problem_reporter = problem_reporter

  def GetStopBoundingBox(self):
    return (min(s.stop_lat for s in self.stops.values()),
            min(s.stop_lon for s in self.stops.values()),
            max(s.stop_lat for s in self.stops.values()),
            max(s.stop_lon for s in self.stops.values()),
           )

  def AddAgency(self, name, url, timezone, agency_id=None):
    """Adds an agency to this schedule."""
    agency = Agency(name, url, timezone, agency_id)
    self.AddAgencyObject(agency)
    return agency

  def AddAgencyObject(self, agency, problem_reporter=None, validate=True):
    if not problem_reporter:
      problem_reporter = self.problem_reporter

    if agency.agency_id in self._agencies:
      problem_reporter.DuplicateID('agency_id', agency.agency_id)
      return

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
    agency = Agency(**kwargs)
    if not agency.agency_id:
      agency.agency_id = FindUniqueId(self._agencies)
    self._default_agency = agency
    self.SetDefaultAgency(agency, validate=False)  # Blank agency won't validate
    return agency

  def SetDefaultAgency(self, agency, validate=True):
    """Make agency the default and add it to the schedule if not already added"""
    assert isinstance(agency, Agency)
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
    service_period = ServicePeriod()
    service_period.service_id = FindUniqueId(self.service_periods)
    # blank service won't validate in AddServicePeriodObject
    self.SetDefaultServicePeriod(service_period, validate=False)
    return service_period

  def SetDefaultServicePeriod(self, service_period, validate=True):
    assert isinstance(service_period, ServicePeriod)
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

  def AddStop(self, lat, lng, name):
    """Add a stop to this schedule.

    Args:
      lat: Latitude of the stop as a float or string
      lng: Longitude of the stop as a float or string
      name: Name of the stop, which will appear in the feed
    Returns:
      A new Stop object
    """
    stop_id = unicode(len(self.stops))
    stop = Stop(stop_id=stop_id, lat=lat, lng=lng, name=name)
    self.AddStopObject(stop)
    return stop

  def AddStopObject(self, stop):
    self.stops[stop.stop_id] = stop
    if hasattr(stop, 'zone_id') and stop.zone_id:
      self.fare_zones[stop.zone_id] = True

  def GetStopList(self):
    return self.stops.values()

  def AddRoute(self, short_name, long_name, route_type):
    """Add a route to this schedule.

    Args:
      short_name: Short name of the route, such as "71L"
      long_name: Full name of the route, such as "NW 21st Ave/St Helens Rd"
      route_type: A type such as "Tram", "Subway" or "Bus"
    Returns:
      A new Route object
    """
    route_id = unicode(len(self.routes))
    route = Route(short_name=short_name, long_name=long_name,
                  route_type=route_type, route_id=route_id)
    route.agency_id = self.GetDefaultAgency().agency_id
    self.AddRouteObject(route)
    return route

  def AddRouteObject(self, route, problem_reporter=None):
    if not problem_reporter:
      problem_reporter = self.problem_reporter

    route.Validate(problem_reporter)

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

  def AddTripObject(self, trip, problem_reporter=None):
    # Validate trip object before adding
    if not problem_reporter:
      problem_reporter = self.problem_reporter
    trip.Validate(problem_reporter)

    if trip.trip_id in self.trips:
      problem_reporter.DuplicateID('trip_id', trip.trip_id)
      return

    if trip.shape_id and trip.shape_id not in self._shapes:
      problem_reporter.InvalidValue('shape_id', trip.shape_id)

    if not trip.service_period:
      if trip.service_id not in self.service_periods:
        problem_reporter.InvalidValue('service_id', trip.service_id)
        return
      else:
        trip.service_period = self.service_periods[trip.service_id]
        del trip.service_id  # so that trip only has one service member

    # TODO: validate distance values in stop times (if applicable)

    self.trips[trip.trip_id] = trip
    if trip.route_id not in self.routes:
      problem_reporter.InvalidValue('route_id', trip.route_id)
    else:
      self.routes[trip.route_id].AddTripObject(trip)

  def GetTripList(self):
    return self.trips.values()

  def GetTrip(self, trip_id):
    return self.trips[trip_id]

  def AddFareObject(self, fare, problem_reporter=None):
    if not problem_reporter:
      problem_reporter = self.problem_reporter
    fare.Validate(problem_reporter)

    if fare.fare_id in self.fares:
      problem_reporter.DuplicateID('fare_id', fare.fare_id)
      return

    self.fares[fare.fare_id] = fare

  def GetFareList(self):
    return self.fares.values()

  def GetFare(self, fare_id):
    return self.fares[fare_id]

  def AddFareRuleObject(self, rule, problem_reporter=None):
    if not problem_reporter:
      problem_reporter = self.problem_reporter

    if IsEmpty(rule.fare_id):
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
      self.GetFare(rule.fare_id).rules.append(rule)
    else:
      problem_reporter.InvalidValue('fare_id', rule.fare_id,
                                    '(This fare_id doesn\'t correspond to any '
                                    'of the IDs defined in the '
                                    'fare attributes.)')

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
      # TODO: Use ApproximateDistanceBetweenStops?
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
    loader = Loader(feed_path, self, problems=self.problem_reporter,
                    extra_validation=extra_validation)
    loader.Load()

  def WriteGoogleTransitFeed(self, file):
    """Output this schedule as a Google Transit Feed in file_name.

    Args:
      file: path of new feed file (a string) or a file-like object

    Returns:
      None
    """
    archive = zipfile.ZipFile(file, 'w')

    agency_string = StringIO.StringIO()
    writer = CsvUnicodeWriter(agency_string)
    writer.writerow(Agency._FIELD_NAMES)
    for agency in self._agencies.values():
      writer.writerow(agency.GetFieldValuesTuple())
    archive.writestr('agency.txt', agency_string.getvalue())

    calendar_dates_string = StringIO.StringIO()
    writer = CsvUnicodeWriter(calendar_dates_string)
    writer.writerow(ServicePeriod._FIELD_NAMES_CALENDAR_DATES)
    has_data = False
    for period in self.service_periods.values():
      for row in period.GenerateCalendarDatesFieldValuesTuples():
        has_data = True
        writer.writerow(row)
    wrote_calendar_dates = False
    if has_data:
      wrote_calendar_dates = True
      archive.writestr('calendar_dates.txt', calendar_dates_string.getvalue())

    calendar_string = StringIO.StringIO()
    writer = CsvUnicodeWriter(calendar_string)
    writer.writerow(ServicePeriod._FIELD_NAMES)
    has_data = False
    for s in self.service_periods.values():
      row = s.GetCalendarFieldValuesTuple()
      if row:
        has_data = True
        writer.writerow(row)
    if has_data or not wrote_calendar_dates:
      archive.writestr('calendar.txt', calendar_string.getvalue())

    stop_string = StringIO.StringIO()
    writer = CsvUnicodeWriter(stop_string)
    writer.writerow(Stop._FIELD_NAMES)
    writer.writerows(s.GetFieldValuesTuple() for s in self.stops.values())
    archive.writestr('stops.txt', stop_string.getvalue())

    route_string = StringIO.StringIO()
    writer = CsvUnicodeWriter(route_string)
    writer.writerow(Route._FIELD_NAMES)
    writer.writerows(r.GetFieldValuesTuple() for r in self.routes.values())
    archive.writestr('routes.txt', route_string.getvalue())

    # write trips.txt
    trips_string = StringIO.StringIO()
    writer = CsvUnicodeWriter(trips_string)
    writer.writerow(Trip._FIELD_NAMES)
    writer.writerows(t.GetFieldValuesTuple() for t in self.trips.values())
    archive.writestr('trips.txt', trips_string.getvalue())

    # write frequencies.txt (if applicable)
    headway_rows = []
    for trip in self.GetTripList():
      headway_rows += trip.GetHeadwayPeriodOutputTuples()
    if headway_rows:
      headway_string = StringIO.StringIO()
      writer = CsvUnicodeWriter(headway_string)
      writer.writerow(Trip._FIELD_NAMES_HEADWAY)
      writer.writerows(headway_rows)
      archive.writestr('frequencies.txt', headway_string.getvalue())

    # write fares (if applicable)
    if self.GetFareList():
      fare_string = StringIO.StringIO()
      writer = CsvUnicodeWriter(fare_string)
      writer.writerow(Fare._FIELD_NAMES)
      writer.writerows(f.GetFieldValuesTuple() for f in self.GetFareList())
      archive.writestr('fare_attributes.txt', fare_string.getvalue())

    # write fare rules (if applicable)
    rule_rows = []
    for fare in self.GetFareList():
      for rule in fare.GetFareRuleList():
        rule_rows.append(rule.GetFieldValuesTuple())
    if rule_rows:
      rule_string = StringIO.StringIO()
      writer = CsvUnicodeWriter(rule_string)
      writer.writerow(FareRule._FIELD_NAMES)
      writer.writerows(rule_rows)
      archive.writestr('fare_rules.txt', rule_string.getvalue())

    stop_times_string = StringIO.StringIO()
    writer = CsvUnicodeWriter(stop_times_string)
    writer.writerow(StopTime._FIELD_NAMES)
    for t in self.trips.values():
      writer.writerows(t._GenerateStopTimesTuples())
    archive.writestr('stop_times.txt', stop_times_string.getvalue())

    # write shapes (if applicable)
    shape_rows = []
    for shape in self.GetShapeList():
      seq = 1
      for (lat, lon, dist) in shape.points:
        shape_rows.append((shape.shape_id, lat, lon, seq, dist))
        seq += 1
    if shape_rows:
      shape_string = StringIO.StringIO()
      writer = CsvUnicodeWriter(shape_string)
      writer.writerow(Shape._FIELD_NAMES)
      writer.writerows(shape_rows)
      archive.writestr('shapes.txt', shape_string.getvalue())

    archive.close()

  def Validate(self, problems=None, validate_children=True):
    """Validates various holistic aspects of the schedule
       (mostly interrelationships between the various data sets)."""
    if not problems:
      problems = self.problem_reporter

    # TODO: Check Trip fields against valid values

    # Check for stops that aren't referenced by any trips
    for stop in self.stops.values():
      if validate_children:
        stop.Validate(problems)
      if not stop.trip_index:
        problems.UnusedStop(stop.stop_id, stop.stop_name)

    # Check for stops that might represent the same location
    # (specifically, stops that are less that 2 meters apart)
    sorted_stops = self.GetStopList()
    sorted_stops.sort(key=(lambda x: x.stop_lat))
    TWO_METERS_LAT = 0.000018
    for index, stop in enumerate(sorted_stops[:-1]):
      index += 1
      while ((index < len(sorted_stops)) and
             ((sorted_stops[index].stop_lat - stop.stop_lat) < TWO_METERS_LAT)):
        if ApproximateDistanceBetweenStops(stop, sorted_stops[index]) < 2:
          problems.OtherProblem('The stops "%s" (ID "%s") and '
                                '"%s" (ID "%s") are so close together that '
                                'they probably represent the same location.' %
                                (stop.stop_name, stop.stop_id,
                                 sorted_stops[index].stop_name,
                                 sorted_stops[index].stop_id))
        index += 1

    # Check for multiple routes using same short + long name
    route_names = {}
    for route in self.routes.values():
      if validate_children:
        route.Validate(problems)
      short_name = ''
      if not IsEmpty(route.route_short_name):
        short_name = route.route_short_name.lower().strip()
      long_name = ''
      if not IsEmpty(route.route_long_name):
        long_name = route.route_long_name.lower().strip()
      name = (short_name, long_name)
      if name in route_names:
        problems.InvalidValue('route_long_name',
                              name,
                              'The same combination of '
                              'route_short_name and route_long_name '
                              'shouldn\'t be used for more than one '
                              'route, as it is for the for the two routes '
                              'with IDs "%s" and "%s".' %
                              (route.route_id, route_names[name].route_id))
      else:
        route_names[name] = route

    # Check that routes' agency IDs are valid, if set
    for route in self.routes.values():
      if (not IsEmpty(route.agency_id) and
          not route.agency_id in self._agencies):
        problems.InvalidValue('agency_id',
                              route.agency_id,
                              'The route with ID "%s" specifies agency_id '
                              '"%s", which doesn\'t exist.' %
                              (route.route_id, route.agency_id))

    # Make sure all trips have stop_times
    # We're doing this here instead of in Trip.Validate() so that
    # Trips can be validated without error during the reading of trips.txt
    for trip in self.trips.values():
      if not trip.GetTimeStops():
        problems.OtherProblem('The trip with the trip_id "%s" doesn\'t have '
                              'any stop times defined.' % trip.trip_id)
      if len(trip.GetTimeStops()) == 1:
        problems.OtherProblem('The trip with the trip_id "%s" only has one '
                              'stop on it; it should have at least one more '
                              'stop so that the riders can leave!' %
                              trip.trip_id)

    # Check for unused shapes
    known_shape_ids = set(self._shapes.keys())
    used_shape_ids = set()
    for trip in self.GetTripList():
      used_shape_ids.add(trip.shape_id)
    unused_shape_ids = known_shape_ids - used_shape_ids
    if unused_shape_ids:
      problems.OtherProblem('The shapes with the following shape_ids aren\'t '
                            'used by any trips: %s' %
                            ', '.join(unused_shape_ids))


class Loader:
  def __init__(self,
               feed_path,
               schedule=None,
               problems=default_problem_reporter,
               extra_validation=False):
    if not schedule:
      schedule = Schedule(problem_reporter=problems)
    self._extra_validation = extra_validation
    self._schedule = schedule
    self._problems = problems
    self._path = feed_path
    self._zip = None

  def _DetermineFormat(self):
    """Determines whether the feed is in a form that we understand, and
       if so, returns True."""
    if not os.path.exists(self._path):
      self._problems.FeedNotFound(self._path)
      return False

    if self._path.endswith('.zip'):
      try:
        self._zip = zipfile.ZipFile(self._path, mode='r')
      except IOError:  # self._path is a directory
        pass
      except zipfile.BadZipfile:
        self._problems.UnknownFormat(self._path)
        return False

    if not self._zip and not os.path.isdir(self._path):
      self._problems.UnknownFormat(self._path)
      return False

    return True

  # TODO: Add testing for this specific function
  def _ReadCSV(self, file_name, cols, required):
    """Reads lines from file_name, yielding a list of unicode values
    corresponding to the column names in cols."""

    contents = self._FileContents(file_name)
    if not contents:  # Missing file
      return

    # strip out any UTF-8 Byte Order Marker (otherwise it'll be
    # treated as part of the first column name, causing a mis-parse)
    contents = contents.lstrip(codecs.BOM_UTF8)

    reader = csv.reader(StringIO.StringIO(contents))  # Use excel dialect

    header = reader.next()
    header = map(lambda x: x.strip(), header)  # trim any whitespace
    header_dict = {}
    for column_name in header:
      header_dict[column_name] = len(header_dict)

    col_index = [-1] * len(cols)
    for i in range(len(cols)):
      if cols[i] in header:
        col_index[i] = header.index(cols[i])
      elif cols[i] in required:
        self._problems.MissingColumn(file_name, cols[i])

    row_num = 1
    for row in reader:
      row_num += 1
      if len(row) == 0:  # skip extra empty lines in file
        continue
        
      if len(row) > len(header):
        self._problems.OtherProblem('Found too many cells (commas) in line '
                                    '%d of file "%s".  Every row in the file '
                                    'should have the same number of cells as '
                                    'the header (first line) does.' %
                                    (row_num, file_name))
        
      result = [None] * len(cols)
      for i in range(len(cols)):
        ci = col_index[i]
        if ci >= 0:
          if len(row) <= ci:  # handle short CSV rows
            result[i] = u''
          else:
            try:
              result[i] = row[ci].decode('utf-8').strip()
            except UnicodeDecodeError:
              self._problems.InvalidValue(cols[i], row[ci],
                                          'Unicode error in row %s' % row)
      yield (result, row_num, header_dict)

  def _HasFile(self, file_name):
    """Returns True if there's a file in the current feed with the
       given file_name in the current feed."""
    if self._zip:
      return file_name in self._zip.namelist()
    else:
      file_path = os.path.join(self._path, file_name)
      return os.path.exists(file_path) and os.path.isfile(file_path)

  def _FileContents(self, file_name):
    results = None
    if self._zip:
      try:
        results = self._zip.read(file_name)
      except KeyError:  # file not found in archve
        self._problems.MissingFile(file_name)
        return None
    else:
      try:
        data_file = open(os.path.join(self._path, file_name), 'r')
        results = data_file.read()
      except IOError:  # file not found
        self._problems.MissingFile(file_name)
        return None

    if not results:
      self._problems.EmptyFile(file_name)
    return results

  def _LoadAgencies(self):
    for (row, row_num, cols) in self._ReadCSV('agency.txt',
                                              Agency._FIELD_NAMES,
                                              Agency._REQUIRED_FIELD_NAMES):
      self._problems.SetFileContext('agency.txt', row_num, row)
      agency = Agency(field_list=row)
      self._schedule.AddAgencyObject(agency, self._problems)
      self._problems.SetContext(None)

  def _LoadStops(self):
    for (row, row_num, cols) in self._ReadCSV('stops.txt',
                                              Stop._FIELD_NAMES,
                                              Stop._REQUIRED_FIELD_NAMES):
      self._problems.SetFileContext('stops.txt', row_num, row)

      stop = Stop(field_list=row)
      stop.Validate(self._problems)

      if stop.stop_id in self._schedule.stops:
        self._problems.DuplicateID('stop_id', stop.stop_id)
      else:
        self._schedule.AddStopObject(stop)

      self._problems.SetContext(None)

  def _LoadRoutes(self):
    for (row, row_num, cols) in self._ReadCSV('routes.txt',
                                              Route._FIELD_NAMES,
                                              Route._REQUIRED_FIELD_NAMES):
      self._problems.SetFileContext('routes.txt', row_num, row)

      route = Route(field_list=row)
      self._schedule.AddRouteObject(route, self._problems)

      self._problems.SetContext(None)

  def _LoadCalendar(self):
    file_name = 'calendar.txt'
    file_name_dates = 'calendar_dates.txt'
    if not self._HasFile(file_name) and not self._HasFile(file_name_dates):
      self._problems.MissingFile(file_name)
      return

    if self._HasFile(file_name):
      has_useful_contents = False
      for (row, row_num, cols) in \
              self._ReadCSV(file_name,
                            ServicePeriod._FIELD_NAMES,
                            ServicePeriod._FIELD_NAMES_REQUIRED):
        self._problems.SetFileContext(file_name, row_num, row)

        period = ServicePeriod(field_list=row)
        self._schedule.AddServicePeriodObject(period, self._problems)

        if self._extra_validation:
          if True in period.day_of_week:
            has_useful_contents = True

        self._problems.SetContext(None)

      if self._extra_validation:
        if not has_useful_contents:
          self._problems.OtherProblem('Since calendar.txt isn\'t defining any '
                                      'service dates, it should be omitted '
                                      'from this feed.')

    if self._HasFile(file_name_dates):
      # ['service_id', 'date', 'exception_type']
      fields = ServicePeriod._FIELD_NAMES_CALENDAR_DATES
      for (row, row_num, cols) in self._ReadCSV(file_name_dates,
                                                fields, fields):
        self._problems.SetFileContext(file_name_dates, row_num, row)

        service_id = row[0]
        period = None
        if service_id in self._schedule.service_periods:
          period = self._schedule.service_periods[service_id]
        else:
          period = ServicePeriod(service_id)
          self._schedule.service_periods[service_id] = period
        exception_type = row[2]
        if exception_type == u'1':
          period.SetDateHasService(row[1], True)
        elif exception_type == u'2':
          period.SetDateHasService(row[1], False)
        else:
          self._problems.InvalidValue('exception_type', exception_type)

        period.Validate(self._problems)

        self._problems.SetContext(None)

  def _LoadShapes(self):
    if not self._HasFile('shapes.txt'):
      return

    shapes = {}  # shape_id to tuple
    for (row, row_num, cols) in self._ReadCSV('shapes.txt',
                                              Shape._FIELD_NAMES,
                                              Shape._REQUIRED_FIELD_NAMES):
      file_context = ('shapes.txt', row_num, row)
      self._problems.SetFileContext(*file_context)

      (shape_id, lat, lon, seq, dist) = row
      if IsEmpty(shape_id):
        self._problems.MissingValue('shape_id')
        continue
      try:
        seq = int(seq)
      except (TypeError, ValueError):
        self._problems.InvalidValue('shape_pt_sequence', seq,
                                    'Value should be a number (1 or higher)')
        continue

      shapes.setdefault(shape_id, []).append((seq, lat, lon, dist, file_context))
      self._problems.SetContext(None)

    for shape_id, points in shapes.items():
      shape = Shape(shape_id)
      points.sort()
      last_seq = 0
      for (seq, lat, lon, dist, file_context) in points:
        if (seq != last_seq + 1):
          self._problems.SetFileContext(*file_context)
          self._problems.InvalidValue('shape_pt_sequence', seq,
                                      'In shape %s, sequence number %d found when '
                                      '%d was expected' %
                                      (shape_id, seq, last_seq + 1))
        last_seq = seq
        shape.AddPoint(lat, lon, dist, self._problems)
        self._problems.SetContext(None)

      self._schedule.AddShapeObject(shape, self._problems)

  def _LoadTrips(self):
    for (row, row_num, cols) in self._ReadCSV('trips.txt',
                                              Trip._FIELD_NAMES,
                                              Trip._REQUIRED_FIELD_NAMES):
      self._problems.SetFileContext('trips.txt', row_num, row)

      trip = Trip(field_list=row)
      self._schedule.AddTripObject(trip, self._problems)

      self._problems.SetContext(None)

  def _LoadFares(self):
    if not self._HasFile('fare_attributes.txt'):
      return
    for (row, row_num, cols) in self._ReadCSV('fare_attributes.txt',
                                              Fare._FIELD_NAMES,
                                              Fare._REQUIRED_FIELD_NAMES):
      self._problems.SetFileContext('fare_attributes.txt', row_num, row)

      fare = Fare(field_list=row)
      self._schedule.AddFareObject(fare, self._problems)

      self._problems.SetContext(None)

  def _LoadFareRules(self):
    if not self._HasFile('fare_rules.txt'):
      return
    for (row, row_num, cols) in self._ReadCSV('fare_rules.txt',
                                              FareRule._FIELD_NAMES,
                                              FareRule._REQUIRED_FIELD_NAMES):
      self._problems.SetFileContext('fare_rules.txt', row_num, row)

      rule = FareRule(field_list=row)
      self._schedule.AddFareRuleObject(rule, self._problems)

      self._problems.SetContext(None)

  def _LoadHeadways(self):
    file_name = 'frequencies.txt'
    if not self._HasFile(file_name):  # headways are an optional feature
      return

    # ['trip_id', 'start_time', 'end_time', 'headway_secs']
    fields = Trip._FIELD_NAMES_HEADWAY
    modified_trips = {}
    for (row, row_num, cols) in self._ReadCSV(file_name, fields, fields):
      self._problems.SetFileContext(file_name, row_num, row)
      (trip_id, start_time, end_time, headway_secs) = row
      try:
        trip = self._schedule.GetTrip(trip_id)
        trip.AddHeadwayPeriod(start_time, end_time, headway_secs,
                              self._problems)
        modified_trips[trip_id] = trip
      except KeyError:
        self._problems.InvalidValue('trip_id', trip_id)
      self._problems.SetContext(None)

    for trip in modified_trips.values():
      trip.Validate(self._problems)

  def _LoadStopTimes(self):
    stoptimes = {}  # maps trip_id to list of stop time tuples
    for (row, row_num, cols) in self._ReadCSV('stop_times.txt',
                                              StopTime._FIELD_NAMES,
                                              StopTime._REQUIRED_FIELD_NAMES):
      self._problems.SetFileContext('stop_times.txt', row_num, row)

      (trip_id, arrival_time, departure_time, stop_id, stop_sequence,
         stop_headsign, pickup_type, drop_off_type, shape_dist_traveled) = row

      # Check that the stop sequence is a one-based number.
      try:
        sequence = int(stop_sequence)
        if (sequence < 1):
          self._problems.InvalidValue('stop_sequence', stop_sequence,
                                      'stop_sequence should count up from 1.')
      except (TypeError, ValueError):
        self._problems.InvalidValue('stop_sequence', stop_sequence,
                                    'This should be a number.')
        continue
      # TODO: check for duplicate sequence numbers

      if stop_id not in self._schedule.stops:
        self._problems.InvalidValue('stop_id', stop_id,
                                    'This value wasn\'t defined in stops.txt')
        continue
      stop = self._schedule.stops[stop_id]
      stoptimes.setdefault(trip_id, []).append(
          (sequence, StopTime(self._problems, stop, arrival_time,
                                   departure_time, stop_headsign,
                                   pickup_type, drop_off_type,
                                   shape_dist_traveled)))
      self._problems.SetContext(None)

    for trip_id, sequence in stoptimes.iteritems():
      sequence.sort()
      try:
        trip = self._schedule.GetTrip(trip_id)
      except KeyError:
        self._problems.InvalidValue('trip_id', trip_id)
        continue
      if sequence[0][1] is None and sequence[0][2] is None:
        self._problems.OtherProblem(
          'No time for start of trip_id "%s" at stop_sequence "%d"' %
          (trip_id, sequence[0][0]))
      if sequence[-1][1] is None and sequence[-1][2] is None:
        self._problems.OtherProblem(
          'No time for end of trip_id "%s" at stop_sequence "%d"' %
          (trip_id, sequence[-1][0]))
      expected_sequence = 1
      for stop_sequence, stoptime in sequence:
        if expected_sequence != stop_sequence:
          self._problems.OtherProblem(
            'Bad stop_sequence. Expected %i, found %i in trip_id "%s"' %
            (expected_sequence, stop_sequence, trip_id))
        trip.AddStopTimeObject(stoptime, problems=self._problems)
        expected_sequence = stop_sequence + 1

  def Load(self):
    if not self._DetermineFormat():
      return self._schedule

    self._LoadAgencies()
    self._LoadStops()
    self._LoadRoutes()
    self._LoadCalendar()
    self._LoadShapes()
    self._LoadTrips()
    self._LoadHeadways()
    self._LoadStopTimes()
    self._LoadFares()
    self._LoadFareRules()

    if self._zip:
      self._zip.close()
      self._zip = None

    if self._extra_validation:
      self._schedule.Validate(self._problems, validate_children=False)

    return self._schedule
