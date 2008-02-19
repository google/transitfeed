#!/usr/bin/python2.4
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

"""Unit tests for the kmlwriter module."""

import os
import StringIO
import tempfile
import unittest
import kmlparser
import kmlwriter
import transitfeed

try:
  import xml.etree.ElementTree as ET  # python 2.5
except ImportError, e:
  import elementtree.ElementTree as ET  # older pythons


def DataPath(path):
  """Return the path to a given file in the test data directory.

  Args:
    path: The path relative to the test data directory.

  Returns:
    The absolute path.
  """
  here = os.path.dirname(__file__)
  return os.path.join(here, 'data', path)


class TestKMLStopsRoundtrip(unittest.TestCase):
  """Checks to see whether all stops are preserved when going to and from KML.
  """

  def setUp(self):
    fd, self.kml_output = tempfile.mkstemp('kml')
    os.close(fd)

  def tearDown(self):
    os.remove(self.kml_output)

  def runTest(self):
    gtfs_input = DataPath('good_feed.zip')
    feed1 = transitfeed.Loader(gtfs_input).Load()
    kmlwriter.KMLWriter().Write(feed1, self.kml_output)
    feed2 = transitfeed.Schedule()
    kmlparser.KmlParser().Parse(self.kml_output, feed2)

    stop_name_mapper = lambda x: x.stop_name

    stops1 = set(map(stop_name_mapper, feed1.GetStopList()))
    stops2 = set(map(stop_name_mapper, feed2.GetStopList()))

    self.assertEqual(stops1, stops2)


class TestKMLGeneratorMethods(unittest.TestCase):
  """Tests the various KML element creation methods of KMLWriter."""

  def setUp(self):
    self.kmlwriter = kmlwriter.KMLWriter()
    self.parent = ET.Element('parent')

  def _ElementToString(self, root):
    """Returns the node as an XML string.

    Args:
      root: The ElementTree.Element instance.

    Returns:
      The XML string.
    """
    output = StringIO.StringIO()
    ET.ElementTree(root).write(output, 'utf-8')
    return output.getvalue()

  def testCreateFolderVisible(self):
    element = self.kmlwriter._CreateFolder(self.parent, 'folder_name')
    self.assertEqual(self._ElementToString(element),
                     '<Folder><name>folder_name</name></Folder>')

  def testCreateFolderNotVisible(self):
    element = self.kmlwriter._CreateFolder(self.parent, 'folder_name',
                                           visible=False)
    self.assertEqual(self._ElementToString(element),
                     '<Folder><name>folder_name</name>'
                     '<visibility>0</visibility></Folder>')

  def testCreateFolderWithDescription(self):
    element = self.kmlwriter._CreateFolder(self.parent, 'folder_name',
                                           description='folder_desc')
    self.assertEqual(self._ElementToString(element),
                     '<Folder><name>folder_name</name>'
                     '<description>folder_desc</description></Folder>')

  def testCreatePlacemark(self):
    element = self.kmlwriter._CreatePlacemark(self.parent, 'abcdef')
    self.assertEqual(self._ElementToString(element),
                     '<Placemark><name>abcdef</name></Placemark>')

  def testCreatePlacemarkWithStyle(self):
    element = self.kmlwriter._CreatePlacemark(self.parent, 'abcdef',
                                              style_id='ghijkl')
    self.assertEqual(self._ElementToString(element),
                     '<Placemark><name>abcdef</name>'
                     '<styleUrl>#ghijkl</styleUrl></Placemark>')

  def testCreatePlacemarkNotVisible(self):
    element = self.kmlwriter._CreatePlacemark(self.parent, 'abcdef',
                                              visible=False)
    self.assertEqual(self._ElementToString(element),
                     '<Placemark><name>abcdef</name>'
                     '<visibility>0</visibility></Placemark>')

  def testCreatePlacemarkWithDescription(self):
    element = self.kmlwriter._CreatePlacemark(self.parent, 'abcdef',
                                              description='ghijkl')
    self.assertEqual(self._ElementToString(element),
                     '<Placemark><name>abcdef</name>'
                     '<description>ghijkl</description></Placemark>')

  def testCreateLineString(self):
    coord_list = [(1.0, 2.0), (3.0, 4.0), (5.0, 6.0)]
    element = self.kmlwriter._CreateLineString(self.parent, coord_list)
    self.assertEqual(self._ElementToString(element),
                     '<LineString><tessellate>1</tessellate>'
                     '<coordinates>%f,%f %f,%f %f,%f</coordinates>'
                     '</LineString>' % (2.0, 1.0, 4.0, 3.0, 6.0, 5.0))

  def testCreateLineStringForShape(self):
    shape = transitfeed.Shape('shape')
    shape.AddPoint(1.0, 1.0)
    shape.AddPoint(2.0, 4.0)
    shape.AddPoint(3.0, 9.0)
    element = self.kmlwriter._CreateLineStringForShape(self.parent, shape)
    self.assertEqual(self._ElementToString(element),
                     '<LineString><tessellate>1</tessellate>'
                     '<coordinates>%f,%f %f,%f %f,%f</coordinates>'
                     '</LineString>' % (1.0, 1.0, 4.0, 2.0, 9.0, 3.0))


class TestRouteKML(unittest.TestCase):
  """Tests the routes folder KML generation methods of KMLWriter."""

  def setUp(self):
    self.feed = transitfeed.Loader(DataPath('flatten_feed.zip')).Load()
    self.kmlwriter = kmlwriter.KMLWriter()
    self.parent = ET.Element('parent')

  def testCreateRoutePatternsFolderNoPatterns(self):
    folder = self.kmlwriter._CreateRoutePatternsFolder(
        self.parent, self.feed.GetRoute('route_7'))
    self.assert_(folder is None)

  def testCreateRoutePatternsFolderOnePattern(self):
    folder = self.kmlwriter._CreateRoutePatternsFolder(
        self.parent, self.feed.GetRoute('route_1'))
    placemarks = folder.findall('Placemark')
    self.assertEquals(len(placemarks), 1)

  def testCreateRoutePatternsFolderTwoPatterns(self):
    folder = self.kmlwriter._CreateRoutePatternsFolder(
        self.parent, self.feed.GetRoute('route_3'))
    placemarks = folder.findall('Placemark')
    self.assertEquals(len(placemarks), 2)

  def testCreateRoutePatternFolderTwoEqualPatterns(self):
    folder = self.kmlwriter._CreateRoutePatternsFolder(
        self.parent, self.feed.GetRoute('route_4'))
    placemarks = folder.findall('Placemark')
    self.assertEquals(len(placemarks), 1)

  def testCreateRouteShapesFolderOneTripOneShape(self):
    folder = self.kmlwriter._CreateRouteShapesFolder(
        self.feed, self.parent, self.feed.GetRoute('route_1'))
    self.assertEqual(len(folder.findall('Placemark')), 1)

  def testCreateRouteShapesFolderTwoTripsTwoShapes(self):
    folder = self.kmlwriter._CreateRouteShapesFolder(
        self.feed, self.parent, self.feed.GetRoute('route_2'))
    self.assertEqual(len(folder.findall('Placemark')), 2)

  def testCreateRouteShapesFolderTwoTripsOneShape(self):
    folder = self.kmlwriter._CreateRouteShapesFolder(
        self.feed, self.parent, self.feed.GetRoute('route_3'))
    self.assertEqual(len(folder.findall('Placemark')), 1)

  def testCreateRouteShapesFolderTwoTripsNoShapes(self):
    folder = self.kmlwriter._CreateRouteShapesFolder(
        self.feed, self.parent, self.feed.GetRoute('route_4'))
    self.assert_(folder is None)

  def testCreateRouteTripsFolderTwoTrips(self):
    folder = self.kmlwriter._CreateRouteTripsFolder(
        self.parent, self.feed.GetRoute('route_4'))
    self.assertEquals(len(folder.findall('Placemark')), 2)

  def testCreateRouteTripsFolderNoTrips(self):
    folder = self.kmlwriter._CreateRouteTripsFolder(
        self.parent, self.feed.GetRoute('route_7'))
    self.assert_(folder is None)

  def testCreateRoutesFolderNoRoutes(self):
    schedule = transitfeed.Schedule()
    folder = self.kmlwriter._CreateRoutesFolder(schedule, self.parent)
    self.assert_(folder is None)

  def testCreateRoutesFolderNoRoutesWithRouteType(self):
    folder = self.kmlwriter._CreateRoutesFolder(self.feed, self.parent, 999)
    self.assert_(folder is None)

  def _TestCreateRoutesFolder(self, show_trips):
    self.kmlwriter.show_trips = show_trips
    folder = self.kmlwriter._CreateRoutesFolder(self.feed, self.parent)
    self.assertEquals(folder.tag, 'Folder')
    styles = self.parent.findall('Style')
    self.assertEquals(len(styles), len(self.feed.GetRouteList()))
    route_folders = folder.findall('Folder')
    self.assertEquals(len(route_folders), len(self.feed.GetRouteList()))

  def testCreateRoutesFolder(self):
    self._TestCreateRoutesFolder(False)

  def testCreateRoutesFolderShowTrips(self):
    self._TestCreateRoutesFolder(True)

  def testCreateRoutesFolderWithRouteType(self):
    folder = self.kmlwriter._CreateRoutesFolder(self.feed, self.parent, 1)
    route_folders = folder.findall('Folder')
    self.assertEquals(len(route_folders), 1)


class TestShapesKML(unittest.TestCase):
  """Tests the shapes folder KML generation methods of KMLWriter."""

  def setUp(self):
    self.flatten_feed = transitfeed.Loader(DataPath('flatten_feed.zip')).Load()
    self.good_feed = transitfeed.Loader(DataPath('good_feed.zip')).Load()
    self.kmlwriter = kmlwriter.KMLWriter()
    self.parent = ET.Element('parent')

  def testCreateShapesFolderNoShapes(self):
    folder = self.kmlwriter._CreateShapesFolder(self.good_feed, self.parent)
    self.assertEquals(folder, None)

  def testCreateShapesFolder(self):
    folder = self.kmlwriter._CreateShapesFolder(self.flatten_feed, self.parent)
    placemarks = folder.findall('Placemark')
    self.assertEquals(len(placemarks), 3)
    for placemark in placemarks:
      self.assert_(placemark.find('LineString') is not None)


class TestStopsKML(unittest.TestCase):
  """Tests the stops folder KML generation methods of KMLWriter."""

  def setUp(self):
    self.feed = transitfeed.Loader(DataPath('flatten_feed.zip')).Load()
    self.kmlwriter = kmlwriter.KMLWriter()
    self.parent = ET.Element('parent')

  def testCreateStopsFolderNoStops(self):
    schedule = transitfeed.Schedule()
    folder = self.kmlwriter._CreateStopsFolder(schedule, self.parent)
    self.assert_(folder is None)

  def testCreateStopsFolder(self):
    folder = self.kmlwriter._CreateStopsFolder(self.feed, self.parent)
    placemarks = folder.findall('Placemark')
    self.assertEquals(len(placemarks), len(self.feed.GetStopList()))


class TestTripsKML(unittest.TestCase):
  """Tests the trips folder KML generation methods of KMLWriter."""

  def setUp(self):
    self.feed = transitfeed.Loader(DataPath('flatten_feed.zip')).Load()
    self.kmlwriter = kmlwriter.KMLWriter()
    self.parent = ET.Element('parent')

  def testCreateTripsFolderForRouteNoTrips(self):
    route = self.feed.GetRoute('route_7')
    folder = self.kmlwriter._CreateRouteTripsFolder(self.parent, route)
    self.assert_(folder is None)

  def testCreateTripsFolderForRoute(self):
    route = self.feed.GetRoute('route_2')
    folder = self.kmlwriter._CreateRouteTripsFolder(self.parent, route)
    placemarks = folder.findall('Placemark')
    trip_placemarks = set()
    for placemark in placemarks:
      trip_placemarks.add(placemark.find('name').text)
    self.assertEquals(trip_placemarks, set(['route_2_1', 'route_2_2']))


if __name__ == '__main__':
  unittest.main()
