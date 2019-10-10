#!/usr/bin/python3

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

# Unit tests for the transitfeed module.


import unittest

import transitfeed
from tests import util


class TransitFeedSampleCodeTestCase(util.TestCase):
    """
    This test should simply contain the sample code printed on the page:
    https://github.com/google/transitfeed/wiki/TransitFeed
    to ensure that it doesn't cause any exceptions.
    """

    def runTest(self):
        import transitfeed

        schedule = transitfeed.Schedule()
        schedule.AddAgency(
            "Sample Agency", "http://example.com", "America/Los_Angeles"
        )
        route = transitfeed.Route()
        route.route_id = "SAMPLE_ID"
        route.route_type = 3
        route.route_short_name = "66"
        route.route_long_name = "Sample Route"
        schedule.AddRouteObject(route)

        service_period = transitfeed.ServicePeriod("WEEK")
        service_period.SetStartDate("20070101")
        service_period.SetEndDate("20071231")
        service_period.SetWeekdayService(True)
        schedule.AddServicePeriodObject(service_period)

        trip = transitfeed.Trip()
        trip.route_id = "SAMPLE_ID"
        trip.service_period = service_period
        trip.trip_id = "SAMPLE_TRIP"
        trip.direction_id = "0"
        trip.block_id = None
        schedule.AddTripObject(trip)

        stop1 = transitfeed.Stop()
        stop1.stop_id = "STOP1"
        stop1.stop_name = "Stop 1"
        stop1.stop_lat = 78.243587
        stop1.stop_lon = 32.258937
        schedule.AddStopObject(stop1)
        trip.AddStopTime(
            stop1, arrival_time="12:00:00", departure_time="12:00:00"
        )

        stop2 = transitfeed.Stop()
        stop2.stop_id = "STOP2"
        stop2.stop_name = "Stop 2"
        stop2.stop_lat = 78.253587
        stop2.stop_lon = 32.258937
        schedule.AddStopObject(stop2)
        trip.AddStopTime(
            stop2, arrival_time="12:05:00", departure_time="12:05:00"
        )

        schedule.Validate()  # not necessary, but helpful for finding problems
        schedule.WriteGoogleTransitFeed("new_feed.zip")


class DeprecatedFieldNamesTestCase(util.MemoryZipTestCase):
    # create class extensions and change fields to be deprecated
    class Agency(transitfeed.Agency):
        _DEPRECATED_FIELD_NAMES = transitfeed.Agency._DEPRECATED_FIELD_NAMES[:]
        _DEPRECATED_FIELD_NAMES.append(("agency_url", None))
        _REQUIRED_FIELD_NAMES = transitfeed.Agency._REQUIRED_FIELD_NAMES[:]
        _REQUIRED_FIELD_NAMES.remove("agency_url")
        _FIELD_NAMES = transitfeed.Agency._FIELD_NAMES[:]
        _FIELD_NAMES.remove("agency_url")

    class Stop(transitfeed.Stop):
        _DEPRECATED_FIELD_NAMES = transitfeed.Stop._DEPRECATED_FIELD_NAMES[:]
        _DEPRECATED_FIELD_NAMES.append(("stop_desc", None))
        _FIELD_NAMES = transitfeed.Stop._FIELD_NAMES[:]
        _FIELD_NAMES.remove("stop_desc")

    def setUp(self):
        super(DeprecatedFieldNamesTestCase, self).setUp()
        # init a new gtfs_factory instance and update its class mappings
        self.gtfs_factory = transitfeed.GetGtfsFactory()
        self.gtfs_factory.UpdateClass("Agency", self.Agency)
        self.gtfs_factory.UpdateClass("Stop", self.Stop)

    def testDeprectatedFieldNames(self):
        self.SetArchiveContents(
            "agency.txt",
            "agency_id,agency_name,agency_timezone,agency_url\n"
            "DTA,Demo Agency,America/Los_Angeles,http://google.com\n",
        )
        schedule = self.MakeLoaderAndLoad(
            self.problems, gtfs_factory=self.gtfs_factory
        )
        e = self.accumulator.PopException("DeprecatedColumn")
        self.assertEqual("agency_url", e.column_name)
        self.accumulator.AssertNoMoreExceptions()

    def testDeprecatedFieldDefaultsToNoneIfNotProvided(self):
        # load agency.txt with no 'agency_url', accessing the variable agency_url
        # should default to None instead of raising an AttributeError
        self.SetArchiveContents(
            "agency.txt",
            "agency_id,agency_name,agency_timezone\n"
            "DTA,Demo Agency,America/Los_Angeles\n",
        )
        schedule = self.MakeLoaderAndLoad(
            self.problems, gtfs_factory=self.gtfs_factory
        )
        agency = list(schedule._agencies.values())[0]
        self.assertTrue(agency.agency_url == None)
        # stop.txt from util.MemoryZipTestCase does not have 'stop_desc', accessing
        # the variable stop_desc should default to None instead of raising an
        # AttributeError
        stop = list(schedule.stops.values())[0]
        self.assertTrue(stop.stop_desc == None)
        self.accumulator.AssertNoMoreExceptions()


if __name__ == "__main__":
    unittest.main()
