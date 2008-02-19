#!/usr/bin/python2.5
#
# Copyright 2008 Google Inc. All Rights Reserved.
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

"""A module for writing GTFS feeds out into Google Earth KML format.

Command-line usage:
python kmlwriter.py [options] <input GTFS filename> [<output KML filename>]

If no output filename is specified, the output file will be given the same
name as the feed file (with ".kml" appended) and will be placed in the same
directory as the input feed.

The resulting KML file has a folder hierarchy which looks like this:

    - Stops
      * stop1
      * stop2
    - Routes
      - route1
        - Shapes
          * shape1
          * shape2
        - Patterns
          - pattern1
          - pattern2
        - Trips
          * trip1
          * trip2
    - Shapes
      * shape1
      * shape2

where the hyphens represent folders and the asteriks represent placemarks.

In a trip, a vehicle visits stops in a certain sequence. Such a sequence of
stops is called a pattern. A pattern is represented by a linestring connecting
the stops. The "Shapes" subfolder of a route folder contains placemarks for
each shape used by a trip in the route. The "Patterns" subfolder contains a
placemark for each unique pattern used by a trip in the route. The "Trips"
subfolder contains a placemark for each trip in the route.

Since there can be many trips and trips for the same route are usually similar,
they are not exported unless the --showtrips option is used. There is also
another option --splitroutes that groups the routes by vehicle type resulting
in a folder hierarchy which looks like this at the top level:

    - Stops
    - Routes - Bus
    - Routes - Tram
    - Routes - Rail
    - Shapes
"""

try:
  import xml.etree.ElementTree as ET  # python 2.5
except ImportError, e:
  import elementtree.ElementTree as ET  # older pythons
import optparse
import os.path
import sys
import transitfeed


class KMLWriter(object):
  """This class knows how to write out a transit feed as KML.

  Sample usage:
    KMLWriter().Write(<transitfeed.Schedule object>, <output filename>)

  Attributes:
    show_trips: True if the individual trips should be included in the routes.
    split_routes: True if the routes should be split by type.
  """

  def __init__(self):
    """Initialise."""
    self.show_trips = False
    self.split_routes = False

  def _SetIndentation(self, elem, level=0):
    """Indented the ElementTree DOM.

    This is the recommended way to cause an ElementTree DOM to be
    prettyprinted on output, as per: http://effbot.org/zone/element-lib.htm

    Run this on the root element before outputting the tree.

    Args:
      elem: The element to start indenting from, usually the document root.
      level: Current indentation level for recursion.
    """
    i = "\n" + level*"  "
    if len(elem):
      if not elem.text or not elem.text.strip():
        elem.text = i + "  "
      for elem in elem:
        self._SetIndentation(elem, level+1)
      if not elem.tail or not elem.tail.strip():
        elem.tail = i
    else:
      if level and (not elem.tail or not elem.tail.strip()):
        elem.tail = i

  def _CreateFolder(self, parent, name, visible=True, description=None):
    """Create a KML Folder element.

    Args:
      parent: The parent ElementTree.Element instance.
      name: The folder name as a string.
      visible: Whether the folder is initially visible or not.
      description: A description string or None.

    Returns:
      The folder ElementTree.Element instance.
    """
    folder = ET.SubElement(parent, 'Folder')
    name_tag = ET.SubElement(folder, 'name')
    name_tag.text = name
    if description is not None:
      desc_tag = ET.SubElement(folder, 'description')
      desc_tag.text = description
    if not visible:
      visibility = ET.SubElement(folder, 'visibility')
      visibility.text = '0'
    return folder

  def _CreateStyleForRoute(self, doc, route):
    """Create a KML Style element for the route.

    The style sets the line colour if the route colour is specified. The
    line thickness is set depending on the vehicle type.

    Args:
      doc: The KML Document ElementTree.Element instance.
      route: The transitfeed.Route to create the style for.

    Returns:
      The id of the style as a string.
    """
    style_id = 'route_%s' % route.route_id
    style = ET.SubElement(doc, 'Style', {'id': style_id})
    linestyle = ET.SubElement(style, 'LineStyle')
    width = ET.SubElement(linestyle, 'width')
    type_to_width = {0: '3',  # Tram
                     1: '3',  # Subway
                     2: '5',  # Rail
                     3: '1'}  # Bus
    width.text = type_to_width.get(route.route_type, '1')
    if route.route_color:
      color = ET.SubElement(linestyle, 'color')
      red = route.route_color[0:2].lower()
      green = route.route_color[2:4].lower()
      blue = route.route_color[4:6].lower()
      color.text = 'ff%s%s%s' % (blue, green, red)
    return style_id

  def _CreatePlacemark(self, parent, name, style_id=None, visible=True,
                       description=None):
    """Create a KML Placemark element.

    Args:
      parent: The parent ElementTree.Element instance.
      name: The placemark name as a string.
      style_id: If not None, the id of a style to use for the placemark.
      visible: Whether the placemark is initially visible or not.
      description: A description string or None.

    Returns:
      The placemark ElementTree.Element instance.
    """
    placemark = ET.SubElement(parent, 'Placemark')
    placemark_name = ET.SubElement(placemark, 'name')
    placemark_name.text = name
    if description is not None:
      desc_tag = ET.SubElement(placemark, 'description')
      desc_tag.text = description
    if style_id is not None:
      styleurl = ET.SubElement(placemark, 'styleUrl')
      styleurl.text = '#%s' % style_id
    if not visible:
      visibility = ET.SubElement(placemark, 'visibility')
      visibility.text = '0'
    return placemark

  def _CreateLineString(self, parent, coordinate_list):
    """Create a KML LineString element.

    The points of the string are given in coordinate_list. Each element of
    coordinate_list should be a tuple (latitude, longitude).

    Args:
      parent: The parent ElementTree.Element instance.
      coordinate_list: The list of coordinates.

    Returns:
      The LineString ElementTree.Element instance.
    """
    linestring = ET.SubElement(parent, 'LineString')
    tessellate = ET.SubElement(linestring, 'tessellate')
    tessellate.text = '1'
    coordinates = ET.SubElement(linestring, 'coordinates')
    coordinate_str_list = ['%f,%f' % (lon, lat)
                           for (lat, lon) in coordinate_list]
    coordinates.text = ' '.join(coordinate_str_list)
    return linestring

  def _CreateLineStringForShape(self, parent, shape):
    """Create a KML LineString using coordinates from a shape.

    Args:
      parent: The parent ElementTree.Element instance.
      shape: The transitfeed.Shape instance.

    Returns:
      The LineString ElementTree.Element instance.
    """
    coordinate_list = [(latitude, longitude) for
                       (latitude, longitude, distance) in shape.points]
    return self._CreateLineString(parent, coordinate_list)

  def _CreateStopsFolder(self, schedule, doc):
    """Create a KML Folder containing placemarks for each stop in the schedule.

    If there are no stops in the schedule then no folder is created.

    Args:
      schedule: The transitfeed.Schedule instance.
      doc: The KML Document ElementTree.Element instance.

    Returns:
      The Folder ElementTree.Element instance or None if there are no stops.
    """
    if not schedule.GetStopList():
      return None
    stop_folder = self._CreateFolder(doc, 'Stops')
    stops = list(schedule.GetStopList())
    stops.sort(key=lambda x: x.stop_name)
    for stop in stops:
      desc_items = []
      if stop.stop_desc:
        desc_items.append(stop.stop_desc)
      if stop.stop_url:
        desc_items.append('Stop info page: <a href="%s">%s</a>' % (
            stop.stop_url, stop.stop_url))
      description = '<br/>'.join(desc_items) or None
      placemark = self._CreatePlacemark(stop_folder, stop.stop_name,
                                        description=description)
      point = ET.SubElement(placemark, 'Point')
      coordinates = ET.SubElement(point, 'coordinates')
      coordinates.text = '%.6f,%.6f' % (stop.stop_lon, stop.stop_lat)
    return stop_folder

  def _CreateRoutePatternsFolder(self, parent, route,
                                   style_id=None, visible=True):
    """Create a KML Folder containing placemarks for each pattern in the route.

    A pattern is a sequence of stops used by one of the trips in the route.

    If there are not patterns for the route then no folder is created and None
    is returned.

    Args:
      parent: The parent ElementTree.Element instance.
      route: The transitfeed.Route instance.
      style_id: The id of a style to use if not None.
      visible: Whether the folder is initially visible or not.

    Returns:
      The Folder ElementTree.Element instance or None if there are no patterns.
    """
    pattern_id_to_trips = route.GetPatternIdTripDict()
    if not pattern_id_to_trips:
      return None

    # sort by number of trips using the pattern
    pattern_trips = pattern_id_to_trips.values()
    pattern_trips.sort(lambda a, b: cmp(len(b), len(a)))

    folder = self._CreateFolder(parent, 'Patterns', visible)
    for n, trips in enumerate(pattern_trips):
      trip_ids = [trip.trip_id for trip in trips]
      name = 'Pattern %d (trips: %d)' % (n+1, len(trips))
      description = 'Trips using this pattern (%d in total): %s' % (
          len(trips), ', '.join(trip_ids))
      placemark = self._CreatePlacemark(folder, name, style_id, visible,
                                        description)
      coordinates = [(stop.stop_lat, stop.stop_lon)
                     for stop in trips[0].GetPattern()]
      self._CreateLineString(placemark, coordinates)
    return folder

  def _CreateRouteShapesFolder(self, schedule, parent, route,
                               style_id=None, visible=True):
    """Create a KML Folder for the shapes of a route.

    The folder contains a placemark for each shape referenced by a trip in the
    route. If there are no such shapes, no folder is created and None is
    returned.

    Args:
      schedule: The transitfeed.Schedule instance.
      parent: The parent ElementTree.Element instance.
      route: The transitfeed.Route instance.
      style_id: The id of a style to use if not None.
      visible: Whether the placemark is initially visible or not.

    Returns:
      The Folder ElementTree.Element instance or None.
    """
    shape_id_to_trips = {}
    for trip in route.trips:
      if trip.shape_id:
        shape_id_to_trips.setdefault(trip.shape_id, []).append(trip)
    if not shape_id_to_trips:
      return None

    # sort by the number of trips using the shape
    shape_id_to_trips_items = shape_id_to_trips.items()
    shape_id_to_trips_items.sort(lambda a, b: cmp(len(b[1]), len(a[1])))

    folder = self._CreateFolder(parent, 'Shapes', visible)
    for shape_id, trips in shape_id_to_trips_items:
      trip_ids = [trip.trip_id for trip in trips]
      name = '%s (trips: %d)' % (shape_id, len(trips))
      description = 'Trips using this shape (%d in total): %s' % (
          len(trips), ', '.join(trip_ids))
      placemark = self._CreatePlacemark(folder, name, style_id, visible,
                                        description)
      self._CreateLineStringForShape(placemark, schedule.GetShape(shape_id))
    return folder

  def _CreateRouteTripsFolder(self, parent, route, style_id=None):
    """Create a KML Folder containing all the trips in the route.

    The folder contains a placemark for each of these trips. If there are no
    trips in the route, no folder is created and None is returned.

    Args:
      parent: The parent ElementTree.Element instance.
      route: The transitfeed.Route instance.
      style_id: A style id string for the placemarks or None.

    Returns:
      The Folder ElementTree.Element instance or None.
    """
    if not route.trips:
      return None
    trips = list(route.trips)
    trips.sort(key=lambda x: x.trip_id)
    trips_folder = self._CreateFolder(parent, 'Trips', visible=False)
    for trip in trips:
      if trip.trip_headsign:
        description = 'Headsign: %s' % trip.trip_headsign
      else:
        description = None
      coordinate_list = [(stop.stop_lat, stop.stop_lon)
                         for stop in trip.GetPattern()]
      placemark = self._CreatePlacemark(trips_folder,
                                        trip.trip_id,
                                        style_id=style_id,
                                        visible=False,
                                        description=description)
      self._CreateLineString(placemark, coordinate_list)
    return trips_folder

  def _CreateRoutesFolder(self, schedule, doc, route_type=None):
    """Create a KML Folder containing routes in a schedule.

    The folder contains a subfolder for each route in the schedule of type
    route_type. If route_type is None, then all routes are selected. Each
    subfolder contains a flattened graph placemark, a route shapes placemark
    and, if show_trips is True, a subfolder containing placemarks for each of
    the trips in the route.

    If there are no routes in the schedule then no folder is created and None
    is returned.

    Args:
      schedule: The transitfeed.Schedule instance.
      doc: The KML Document ElementTree.Element instance.
      route_type: The route type integer or None.

    Returns:
      The Folder ElementTree.Element instance or None.
    """

    def GetRouteName(route):
      """Return a placemark name for the route.

      Args:
        route: The transitfeed.Route instance.

      Returns:
        The name as a string.
      """
      name_parts = []
      if route.route_short_name:
        name_parts.append('<b>%s</b>' % route.route_short_name)
      if route.route_long_name:
        name_parts.append(route.route_long_name)
      return ' - '.join(name_parts) or route.route_id

    def GetRouteDescription(route):
      """Return a placemark description for the route.

      Args:
        route: The transitfeed.Route instance.

      Returns:
        The description as a string.
      """
      desc_items = []
      if route.route_desc:
        desc_items.append(route.route_desc)
      if route.route_url:
        desc_items.append('Route info page: <a href="%s">%s</a>' % (
            route.route_url, route.route_url))
      description = '<br/>'.join(desc_items)
      return description or None

    routes = [route for route in schedule.GetRouteList()
              if route_type is None or route.route_type == route_type]
    if not routes:
      return None
    routes.sort(key=lambda x: GetRouteName(x))

    if route_type is not None:
      route_type_names = {0: 'Tram, Streetcar or Light rail',
                          1: 'Subway or Metro',
                          2: 'Rail',
                          3: 'Bus',
                          4: 'Ferry',
                          5: 'Cable car',
                          6: 'Gondola or suspended cable car',
                          7: 'Funicular'}
      type_name = route_type_names.get(route_type, str(route_type))
      folder_name = 'Routes - %s' % type_name
    else:
      folder_name = 'Routes'
    routes_folder = self._CreateFolder(doc, folder_name, visible=False)

    for route in routes:
      style_id = self._CreateStyleForRoute(doc, route)
      route_folder = self._CreateFolder(routes_folder,
                                        GetRouteName(route),
                                        description=GetRouteDescription(route))
      self._CreateRouteShapesFolder(schedule, route_folder, route,
                                    style_id, False)
      self._CreateRoutePatternsFolder(route_folder, route, style_id, False)
      if self.show_trips:
        self._CreateRouteTripsFolder(route_folder, route, style_id)
    return routes_folder

  def _CreateShapesFolder(self, schedule, doc):
    """Create a KML Folder containing all the shapes in a schedule.

    The folder contains a placemark for each shape. If there are no shapes in
    the schedule then the folder is not created and None is returned.

    Args:
      schedule: The transitfeed.Schedule instance.
      doc: The KML Document ElementTree.Element instance.

    Returns:
      The Folder ElementTree.Element instance or None.
    """
    if not schedule.GetShapeList():
      return None
    shapes_folder = self._CreateFolder(doc, 'Shapes')
    shapes = list(schedule.GetShapeList())
    shapes.sort(key=lambda x: x.shape_id)
    for shape in shapes:
      placemark = self._CreatePlacemark(shapes_folder, shape.shape_id)
      self._CreateLineStringForShape(placemark, shape)
    return shapes_folder

  def Write(self, schedule, output_file):
    """Writes out a feed as KML.

    Args:
      schedule: A transitfeed.Schedule object containing the feed to write.
      output_file: The name of the output KML file, or file object to use.
    """
    # Generate the DOM to write
    root = ET.Element('kml')
    root.attrib['xmlns'] = 'http://earth.google.com/kml/2.1'
    doc = ET.SubElement(root, 'Document')
    open_tag = ET.SubElement(doc, 'open')
    open_tag.text = '1'
    self._CreateStopsFolder(schedule, doc)
    if self.split_routes:
      route_types = set()
      for route in schedule.GetRouteList():
        route_types.add(route.route_type)
      route_types = list(route_types)
      route_types.sort()
      for route_type in route_types:
        self._CreateRoutesFolder(schedule, doc, route_type)
    else:
      self._CreateRoutesFolder(schedule, doc)
    self._CreateShapesFolder(schedule, doc)

    # Make sure we pretty-print
    self._SetIndentation(root)

    # Now write the output
    if isinstance(output_file, file):
      output = output_file
    else:
      output = open(output_file, 'w')
    output.write("""<?xml version="1.0" encoding="UTF-8"?>\n""")
    ET.ElementTree(root).write(output, 'utf-8')


def main():
  usage = ('Usage: python kmlwriter.py [options] <input GTFS filename> '
           '[<output KML filename>]')
  parser = optparse.OptionParser(usage=usage,
                                 version='%prog '+transitfeed.__version__)
  parser.add_option('-t', '--showtrips', action='store_true',
                    dest='show_trips',
                    help='include the individual trips for each route')
  parser.add_option('-s', '--splitroutes', action='store_true',
                    dest='split_routes',
                    help='split the routes by type')
  options, args = parser.parse_args()

  if len(args) < 1:
    print usage
    sys.exit(1)

  input_path = args[0]
  if len(args) >= 2:
    output_path = args[1]
  else:
    path = os.path.normpath(input_path)
    (feed_dir, feed) = os.path.split(path)
    if '.' in feed:
      feed = feed.rsplit('.', 1)[0]  # strip extension
    output_filename = '%s.kml' % feed
    output_path = os.path.join(feed_dir, output_filename)

  loader = transitfeed.Loader(input_path,
                              problems=transitfeed.ProblemReporter())
  feed = loader.Load()
  print "Writing %s" % output_path
  writer = KMLWriter()
  writer.show_trips = options.show_trips
  writer.split_routes = options.split_routes
  writer.Write(feed, output_path)


if __name__ == '__main__':
  main()
