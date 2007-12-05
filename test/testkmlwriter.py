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

# Unit tests for the kmlwriter module.

import kmlparser
import kmlwriter
import os
import tempfile
import transitfeed
import unittest

def DataPath(path):
  here = os.path.dirname(__file__)
  return os.path.join(here, 'data', path)

class TestKMLStopsRoundtrip(unittest.TestCase):
  """
  Checks to see whether all stops are preserved when going to and from KML.
  """

  def setUp(self):
    (fd, self.kml_output) = tempfile.mkstemp('kml')
    os.close(fd)

  def tearDown(self):
    os.remove(self.kml_output)

  def runTest(self):
    gtfs_input = DataPath('good_feed')
    feed1 = transitfeed.Loader(gtfs_input).Load()
    kmlwriter.KMLWriter().Write(feed1, self.kml_output)
    feed2 = transitfeed.Schedule()
    kmlparser.KmlParser().Parse(self.kml_output, feed2)

    stop_name_mapper = lambda x: x.stop_name

    stops1 = set(map(stop_name_mapper, feed1.GetStopList()))
    stops2 = set(map(stop_name_mapper, feed2.GetStopList()))

    self.assertEqual(stops1, stops2)

if __name__ == '__main__':
  unittest.main()

