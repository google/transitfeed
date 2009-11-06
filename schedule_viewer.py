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

# An example application that uses the transitfeed module.
#
# You must provide a Google Maps API key.
#
# Usage:
# schedule_viewer.py --key `cat key` --port 8765 --feed_filename feed.zip

import BaseHTTPServer, sys, urlparse
import bisect
from gtfsscheduleviewer.marey_graph import MareyGraph
import gtfsscheduleviewer
import mimetypes
from optparse import OptionParser
import os.path
import re
import signal
import simplejson
import socket
import time
import transitfeed
import urllib


# By default Windows kills Python with Ctrl+Break. Instead make Ctrl+Break
# raise a KeyboardInterrupt.
if hasattr(signal, 'SIGBREAK'):
  signal.signal(signal.SIGBREAK, signal.default_int_handler)


mimetypes.add_type('text/plain', '.vbs')


class ResultEncoder(simplejson.JSONEncoder):
  def default(self, obj):
    try:
      iterable = iter(obj)
    except TypeError:
      pass
    else:
      return list(iterable)
    return simplejson.JSONEncoder.default(self, obj)

# Code taken from
# http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/425210/index_txt
# An alternate approach is shown at
# http://mail.python.org/pipermail/python-list/2003-July/212751.html
# but it requires multiple threads. A sqlite object can only be used from one
# thread.
class StoppableHTTPServer(BaseHTTPServer.HTTPServer):
  def server_bind(self):
    BaseHTTPServer.HTTPServer.server_bind(self)
    self.socket.settimeout(1)
    self._run = True

  def get_request(self):
    while self._run:
      try:
        sock, addr = self.socket.accept()
        sock.settimeout(None)
        return (sock, addr)
      except socket.timeout:
        pass

  def stop(self):
    self._run = False

  def serve(self):
    while self._run:
      self.handle_request()


def StopToTuple(stop):
  """Return tuple as expected by javascript function addStopMarkerFromList"""
  return (stop.stop_id, stop.stop_name, float(stop.stop_lat),
          float(stop.stop_lon), stop.location_type)


class ScheduleRequestHandler(BaseHTTPServer.BaseHTTPRequestHandler):
  def do_GET(self):
    scheme, host, path, x, params, fragment = urlparse.urlparse(self.path)
    parsed_params = {}
    for k in params.split('&'):
      k = urllib.unquote(k)
      if '=' in k:
        k, v = k.split('=', 1)
        parsed_params[k] = unicode(v, 'utf8')
      else:
        parsed_params[k] = ''

    if path == '/':
      return self.handle_GET_home()

    m = re.match(r'/json/([a-z]{1,64})', path)
    if m:
      handler_name = 'handle_json_GET_%s' % m.group(1)
      handler = getattr(self, handler_name, None)
      if callable(handler):
        return self.handle_json_wrapper_GET(handler, parsed_params)

    # Restrict allowable file names to prevent relative path attacks etc
    m = re.match(r'/file/([a-z0-9_-]{1,64}\.?[a-z0-9_-]{1,64})$', path)
    if m and m.group(1):
      try:
        f, mime_type = self.OpenFile(m.group(1))
        return self.handle_static_file_GET(f, mime_type)
      except IOError, e:
        print "Error: unable to open %s" % m.group(1)
        # Ignore and treat as 404

    m = re.match(r'/([a-z]{1,64})', path)
    if m:
      handler_name = 'handle_GET_%s' % m.group(1)
      handler = getattr(self, handler_name, None)
      if callable(handler):
        return handler(parsed_params)

    return self.handle_GET_default(parsed_params, path)

  def OpenFile(self, filename):
    """Try to open filename in the static files directory of this server.
    Return a tuple (file object, string mime_type) or raise an exception."""
    (mime_type, encoding) = mimetypes.guess_type(filename)
    assert mime_type
    # A crude guess of when we should use binary mode. Without it non-unix
    # platforms may corrupt binary files.
    if mime_type.startswith('text/'):
      mode = 'r'
    else:
      mode = 'rb'
    return open(os.path.join(self.server.file_dir, filename), mode), mime_type

  def handle_GET_default(self, parsed_params, path):
    self.send_error(404)

  def handle_static_file_GET(self, fh, mime_type):
    content = fh.read()
    self.send_response(200)
    self.send_header('Content-Type', mime_type)
    self.send_header('Content-Length', str(len(content)))
    self.end_headers()
    self.wfile.write(content)

  def AllowEditMode(self):
    return False

  def handle_GET_home(self):
    schedule = self.server.schedule
    (min_lat, min_lon, max_lat, max_lon) = schedule.GetStopBoundingBox()
    forbid_editing = ('true', 'false')[self.AllowEditMode()]

    agency = ', '.join(a.agency_name for a in schedule.GetAgencyList()).encode('utf-8')

    key = self.server.key
    host = self.server.host

    # A very simple template system. For a fixed set of values replace [xxx]
    # with the value of local variable xxx
    f, _ = self.OpenFile('index.html')
    content = f.read()
    for v in ('agency', 'min_lat', 'min_lon', 'max_lat', 'max_lon', 'key',
              'host', 'forbid_editing'):
      content = content.replace('[%s]' % v, str(locals()[v]))

    self.send_response(200)
    self.send_header('Content-Type', 'text/html')
    self.send_header('Content-Length', str(len(content)))
    self.end_headers()
    self.wfile.write(content)

  def handle_json_GET_routepatterns(self, params):
    """Given a route_id generate a list of patterns of the route. For each
    pattern include some basic information and a few sample trips."""
    schedule = self.server.schedule
    route = schedule.GetRoute(params.get('route', None))
    if not route:
      self.send_error(404)
      return
    time = int(params.get('time', 0))
    sample_size = 3  # For each pattern return the start time for this many trips

    pattern_id_trip_dict = route.GetPatternIdTripDict()
    patterns = []

    for pattern_id, trips in pattern_id_trip_dict.items():
      time_stops = trips[0].GetTimeStops()
      if not time_stops:
        continue
      has_non_zero_trip_type = False;
      for trip in trips:
        if trip['trip_type'] and trip['trip_type'] != '0':
          has_non_zero_trip_type = True
      name = u'%s to %s, %d stops' % (time_stops[0][2].stop_name, time_stops[-1][2].stop_name, len(time_stops))
      transitfeed.SortListOfTripByTime(trips)

      num_trips = len(trips)
      if num_trips <= sample_size:
        start_sample_index = 0
        num_after_sample = 0
      else:
        # Will return sample_size trips that start after the 'time' param.

        # Linear search because I couldn't find a built-in way to do a binary
        # search with a custom key.
        start_sample_index = len(trips)
        for i, trip in enumerate(trips):
          if trip.GetStartTime() >= time:
            start_sample_index = i
            break

        num_after_sample = num_trips - (start_sample_index + sample_size)
        if num_after_sample < 0:
          # Less than sample_size trips start after 'time' so return all the
          # last sample_size trips.
          num_after_sample = 0
          start_sample_index = num_trips - sample_size

      sample = []
      for t in trips[start_sample_index:start_sample_index + sample_size]:
        sample.append( (t.GetStartTime(), t.trip_id) )

      patterns.append((name, pattern_id, start_sample_index, sample,
                       num_after_sample, (0,1)[has_non_zero_trip_type]))

    patterns.sort()
    return patterns

  def handle_json_wrapper_GET(self, handler, parsed_params):
    """Call handler and output the return value in JSON."""
    schedule = self.server.schedule
    result = handler(parsed_params)
    content = ResultEncoder().encode(result)
    self.send_response(200)
    self.send_header('Content-Type', 'text/plain')
    self.send_header('Content-Length', str(len(content)))
    self.end_headers()
    self.wfile.write(content)

  def handle_json_GET_routes(self, params):
    """Return a list of all routes."""
    schedule = self.server.schedule
    result = []
    for r in schedule.GetRouteList():
      result.append( (r.route_id, r.route_short_name, r.route_long_name) )
    result.sort(key = lambda x: x[1:3])
    return result

  def handle_json_GET_routerow(self, params):
    schedule = self.server.schedule
    route = schedule.GetRoute(params.get('route', None))
    return [transitfeed.Route._FIELD_NAMES, route.GetFieldValuesTuple()]

  def handle_json_GET_triprows(self, params):
    """Return a list of rows from the feed file that are related to this
    trip."""
    schedule = self.server.schedule
    try:
      trip = schedule.GetTrip(params.get('trip', None))
    except KeyError:
      # if a non-existent trip is searched for, the return nothing
      return
    route = schedule.GetRoute(trip.route_id)
    trip_row = dict(trip.iteritems())
    route_row = dict(route.iteritems())
    return [['trips.txt', trip_row], ['routes.txt', route_row]]

  def handle_json_GET_tripstoptimes(self, params):
    schedule = self.server.schedule
    try:
      trip = schedule.GetTrip(params.get('trip'))
    except KeyError:
       # if a non-existent trip is searched for, the return nothing
      return
    time_stops = trip.GetTimeStops()
    stops = []
    times = []
    for arr,dep,stop in time_stops:
      stops.append(StopToTuple(stop))
      times.append(arr)
    return [stops, times]

  def handle_json_GET_tripshape(self, params):
    schedule = self.server.schedule
    try:
      trip = schedule.GetTrip(params.get('trip'))
    except KeyError:
       # if a non-existent trip is searched for, the return nothing
      return
    points = []
    if trip.shape_id:
      shape = schedule.GetShape(trip.shape_id)
      for (lat, lon, dist) in shape.points:
        points.append((lat, lon))
    else:
      time_stops = trip.GetTimeStops()
      for arr,dep,stop in time_stops:
        points.append((stop.stop_lat, stop.stop_lon))
    return points

  def handle_json_GET_neareststops(self, params):
    """Return a list of the nearest 'limit' stops to 'lat', 'lon'"""
    schedule = self.server.schedule
    lat = float(params.get('lat'))
    lon = float(params.get('lon'))
    limit = int(params.get('limit'))
    stops = schedule.GetNearestStops(lat=lat, lon=lon, n=limit)
    return [StopToTuple(s) for s in stops]

  def handle_json_GET_boundboxstops(self, params):
    """Return a list of up to 'limit' stops within bounding box with 'n','e'
    and 's','w' in the NE and SW corners. Does not handle boxes crossing
    longitude line 180."""
    schedule = self.server.schedule
    n = float(params.get('n'))
    e = float(params.get('e'))
    s = float(params.get('s'))
    w = float(params.get('w'))
    limit = int(params.get('limit'))
    stops = schedule.GetStopsInBoundingBox(north=n, east=e, south=s, west=w, n=limit)
    return [StopToTuple(s) for s in stops]

  def handle_json_GET_stopsearch(self, params):
    schedule = self.server.schedule
    query = params.get('q', None).lower()
    matches = []
    for s in schedule.GetStopList():
      if s.stop_id.lower().find(query) != -1 or s.stop_name.lower().find(query) != -1:
        matches.append(StopToTuple(s))
    return matches

  def handle_json_GET_stoptrips(self, params):
    """Given a stop_id and time in seconds since midnight return the next
    trips to visit the stop."""
    schedule = self.server.schedule
    stop = schedule.GetStop(params.get('stop', None))
    time = int(params.get('time', 0))
    time_trips = stop.GetStopTimeTrips(schedule)
    time_trips.sort()  # OPT: use bisect.insort to make this O(N*ln(N)) -> O(N)
    # Keep the first 5 after param 'time'.
    # Need make a tuple to find correct bisect point
    time_trips = time_trips[bisect.bisect_left(time_trips, (time, 0)):]
    time_trips = time_trips[:5]
    # TODO: combine times for a route to show next 2 departure times
    result = []
    for time, (trip, index), tp in time_trips:
      headsign = None
      # Find the most recent headsign from the StopTime objects
      for stoptime in trip.GetStopTimes()[index::-1]:
        if stoptime.stop_headsign:
          headsign = stoptime.stop_headsign
          break
      # If stop_headsign isn't found, look for a trip_headsign
      if not headsign:
        headsign = trip.trip_headsign
      route = schedule.GetRoute(trip.route_id)
      trip_name = ''
      if route.route_short_name:
        trip_name += route.route_short_name
      if route.route_long_name:
        if len(trip_name):
          trip_name += " - "
        trip_name += route.route_long_name
      if headsign:
        trip_name += " (Direction: %s)" % headsign

      result.append((time, (trip.trip_id, trip_name, trip.service_id), tp))
    return result

  def handle_GET_ttablegraph(self,params):
    """Draw a Marey graph in SVG for a pattern (collection of trips in a route
    that visit the same sequence of stops)."""
    schedule = self.server.schedule
    marey = MareyGraph()
    trip = schedule.GetTrip(params.get('trip', None))
    route = schedule.GetRoute(trip.route_id)
    height = int(params.get('height', 300))

    if not route:
      print 'no such route'
      self.send_error(404)
      return

    pattern_id_trip_dict = route.GetPatternIdTripDict()
    pattern_id = trip.pattern_id
    if pattern_id not in pattern_id_trip_dict:
      print 'no pattern %s found in %s' % (pattern_id, pattern_id_trip_dict.keys())
      self.send_error(404)
      return
    triplist = pattern_id_trip_dict[pattern_id]

    pattern_start_time = min((t.GetStartTime() for t in triplist))
    pattern_end_time = max((t.GetEndTime() for t in triplist))

    marey.SetSpan(pattern_start_time,pattern_end_time)
    marey.Draw(triplist[0].GetPattern(), triplist, height)

    content = marey.Draw()

    self.send_response(200)
    self.send_header('Content-Type', 'image/svg+xml')
    self.send_header('Content-Length', str(len(content)))
    self.end_headers()
    self.wfile.write(content)


def FindPy2ExeBase():
  """If this is running in py2exe return the install directory else return
  None"""
  # py2exe puts gtfsscheduleviewer in library.zip. For py2exe setup.py is
  # configured to put the data next to library.zip.
  windows_ending = gtfsscheduleviewer.__file__.find('\\library.zip\\')
  if windows_ending != -1:
    return transitfeed.__file__[:windows_ending]
  else:
    return None


def FindDefaultFileDir():
  """Return the path of the directory containing the static files. By default
  the directory is called 'files'. The location depends on where setup.py put
  it."""
  base = FindPy2ExeBase()
  if base:
    return os.path.join(base, 'schedule_viewer_files')
  else:
    # For all other distributions 'files' is in the gtfsscheduleviewer
    # directory.
    base = os.path.dirname(gtfsscheduleviewer.__file__)  # Strip __init__.py
    return os.path.join(base, 'files')


def GetDefaultKeyFilePath():
  """In py2exe return absolute path of file in the base directory and in all
  other distributions return relative path 'key.txt'"""
  windows_base = FindPy2ExeBase()
  if windows_base:
    return os.path.join(windows_base, 'key.txt')
  else:
    return 'key.txt'


def main(RequestHandlerClass = ScheduleRequestHandler):
  parser = OptionParser(usage='usage: %prog [options] feed_filename',
                        version='%prog '+transitfeed.__version__)
  parser.add_option('--feed_filename', '--feed', dest='feed_filename',
                    help='file name of feed to load')
  parser.add_option('--key', dest='key',
                    help='Google Maps API key or the name '
                    'of a text file that contains an API key')
  parser.add_option('--host', dest='host', help='Host name of Google Maps')
  parser.add_option('--port', dest='port', type='int',
                    help='port on which to listen')
  parser.add_option('--file_dir', dest='file_dir',
                    help='directory containing static files')
  parser.add_option('-n', '--noprompt', action='store_false',
                    dest='manual_entry',
                    help='disable interactive prompts')
  parser.set_defaults(port=8765,
                      host='maps.google.com',
                      file_dir=FindDefaultFileDir(),
                      manual_entry=True)
  (options, args) = parser.parse_args()

  if not os.path.isfile(os.path.join(options.file_dir, 'index.html')):
    print "Can't find index.html with --file_dir=%s" % options.file_dir
    exit(1)

  if not options.feed_filename and len(args) == 1:
    options.feed_filename = args[0]

  if not options.feed_filename and options.manual_entry:
    options.feed_filename = raw_input('Enter Feed Location: ').strip('"')

  default_key_file = GetDefaultKeyFilePath()
  if not options.key and os.path.isfile(default_key_file):
    options.key = open(default_key_file).read().strip()

  if options.key and os.path.isfile(options.key):
    options.key = open(options.key).read().strip()

  schedule = transitfeed.Schedule(problem_reporter=transitfeed.ProblemReporter())
  print 'Loading data from feed "%s"...' % options.feed_filename
  print '(this may take a few minutes for larger cities)'
  schedule.Load(options.feed_filename)

  server = StoppableHTTPServer(server_address=('', options.port),
                               RequestHandlerClass=RequestHandlerClass)
  server.key = options.key
  server.schedule = schedule
  server.file_dir = options.file_dir
  server.host = options.host
  server.feed_path = options.feed_filename

  print ("To view, point your browser at http://localhost:%d/" %
         (server.server_port))
  server.serve_forever()

if __name__ == '__main__':
  main()
