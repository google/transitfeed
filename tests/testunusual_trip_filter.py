#!/usr/bin/python3
#
# Copyright (C) 2009 Google Inc.
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

"""Tests for unusual_trip_filter.py"""

__author__ = "Jiri Semecky <jiri.semecky@gmail.com>"

import unittest

import transitfeed
import unusual_trip_filter
from tests import util


class UnusualTripFilterTestCase(util.TempDirTestCaseBase):
    """Test of unusual trip filter functionality."""

    def testFilter(self):
        """Test if filtering works properly."""
        expected_values = {
            "CITY1": 0,
            "CITY2": 0,
            "CITY3": 0,
            "CITY4": 0,
            "CITY5": 0,
            "CITY6": 0,
            "CITY7": 0,
            "CITY8": 0,
            "CITY9": 0,
            "CITY10": 0,
            "CITY11": 1,
            "CITY12": 1,
        }
        filter = unusual_trip_filter.UnusualTripFilter(0.1, quiet=True)
        input = self.GetPath("tests", "data", "filter_unusual_trips")
        loader = transitfeed.Loader(input, extra_validation=True)
        schedule = loader.Load()
        filter.filter(schedule)
        for trip_id, expected_trip_type in list(expected_values.items()):
            actual_trip_type = schedule.trips[trip_id]["trip_type"]
            try:
                self.assertEqual(int(actual_trip_type), expected_trip_type)
            except ValueError:
                self.assertEqual(actual_trip_type, "")

    def testFilterNoForceFilter(self):
        """Test that force==False doesn't set default values"""
        filter = unusual_trip_filter.UnusualTripFilter(
            0.1, force=False, quiet=True
        )
        input = self.GetPath("tests", "data", "filter_unusual_trips")
        loader = transitfeed.Loader(input, extra_validation=True)
        schedule = loader.Load()
        schedule.trips["CITY2"].trip_type = "odd-trip"
        filter.filter(schedule)
        trip1 = schedule.trips["CITY1"]
        self.assertEqual(trip1["trip_type"], "")
        trip2 = schedule.trips["CITY2"]
        self.assertEqual(trip2["trip_type"], "odd-trip")

    def testFilterForceFilter(self):
        """Test that force==True does set default values"""
        filter = unusual_trip_filter.UnusualTripFilter(
            0.1, force=True, quiet=False
        )
        input = self.GetPath("tests", "data", "filter_unusual_trips")
        loader = transitfeed.Loader(input, extra_validation=True)
        schedule = loader.Load()
        schedule.trips["CITY2"].trip_type = "odd-trip"
        filter.filter(schedule)
        trip1 = schedule.trips["CITY1"]
        self.assertEqual(trip1["trip_type"], "0")
        trip2 = schedule.trips["CITY2"]
        self.assertEqual(trip2["trip_type"], "0")

    def testFilterAppliedForSpecifiedRouteType(self):
        """Setting integer route_type filters trips of this route type."""
        filter = unusual_trip_filter.UnusualTripFilter(
            0.1, quiet=True, route_type=3
        )
        input = self.GetPath("tests", "data", "filter_unusual_trips")
        loader = transitfeed.Loader(input, extra_validation=True)
        schedule = loader.Load()
        filter.filter(schedule)
        actual_trip_type = schedule.trips["CITY11"]["trip_type"]
        self.assertEqual(actual_trip_type, "1")

    def testFilterNotAppliedForUnspecifiedRouteType(self):
        """Setting integer route_type filters trips of this route type."""
        filter = unusual_trip_filter.UnusualTripFilter(
            0.1, quiet=True, route_type=2
        )
        input = self.GetPath("tests", "data", "filter_unusual_trips")
        loader = transitfeed.Loader(input, extra_validation=True)
        schedule = loader.Load()
        filter.filter(schedule)
        actual_trip_type = schedule.trips["CITY11"]["trip_type"]
        self.assertEqual(actual_trip_type, "")

    def testFilterAppliedForRouteTypeSpecifiedByName(self):
        """Setting integer route_type filters trips of this route type."""
        filter = unusual_trip_filter.UnusualTripFilter(
            0.1, quiet=True, route_type="Bus"
        )
        input = self.GetPath("tests", "data", "filter_unusual_trips")
        loader = transitfeed.Loader(input, extra_validation=True)
        schedule = loader.Load()
        filter.filter(schedule)
        actual_trip_type = schedule.trips["CITY11"]["trip_type"]
        self.assertEqual(actual_trip_type, "1")

    def testFilterNotAppliedForDifferentRouteTypeSpecifiedByName(self):
        """Setting integer route_type filters trips of this route type."""
        filter = unusual_trip_filter.UnusualTripFilter(
            0.1, quiet=True, route_type="Ferry"
        )
        input = self.GetPath("tests", "data", "filter_unusual_trips")
        loader = transitfeed.Loader(input, extra_validation=True)
        schedule = loader.Load()
        filter.filter(schedule)
        actual_trip_type = schedule.trips["CITY11"]["trip_type"]
        self.assertEqual(actual_trip_type, "")


if __name__ == "__main__":
    unittest.main()
