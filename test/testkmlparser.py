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

# Unit tests for the kmlparser module.

import kmlparser
import os.path
import transitfeed
import unittest


def DataPath(path):
  here = os.path.dirname(__file__)
  return os.path.join(here, 'data', path)


class TestStopsParsing(unittest.TestCase):
  def setUp(self):
    self.feed = transitfeed.Schedule()

  def testSingleStop(self):
    kmlFile = DataPath('one_stop.kml')
    kmlparser.KmlParser().Parse(kmlFile, self.feed)
    stops = self.feed.GetStopList()
    self.assertEqual(1, len(stops))
    stop = stops[0]
    self.assertEqual(u'Stop Name', stop.stop_name)
    self.assertAlmostEqual(-93.239037, stop.stop_lon)
    self.assertAlmostEqual(44.854164, stop.stop_lat)

class TestShapesParsing(unittest.TestCase):
  def setUp(self):
    self.feed = transitfeed.Schedule()

  def testSingleShape(self):
    kmlFile = DataPath('one_line.kml')
    kmlparser.KmlParser().Parse(kmlFile, self.feed)
    shapes = self.feed.GetShapeList()
    self.assertEqual(1, len(shapes))
    shape = shapes[0]
    self.assertEqual(3, len(shape.points))
    self.assertAlmostEqual(44.854240, shape.points[0][0])
    self.assertAlmostEqual(-93.238861, shape.points[0][1])
    self.assertAlmostEqual(44.853081, shape.points[1][0])
    self.assertAlmostEqual(-93.238708, shape.points[1][1])
    self.assertAlmostEqual(44.852638, shape.points[2][0])
    self.assertAlmostEqual(-93.237923, shape.points[2][1])


if __name__ == '__main__':
  unittest.main()

