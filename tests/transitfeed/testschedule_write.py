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

# Unit tests for the schedule module.


import transitfeed
from tests import util


class MinimalWriteTestCase(util.TempFileTestCaseBase):
    """
    This test case simply constructs an incomplete feed with very few
    fields set and ensures that there are no exceptions when writing it out.

    This is very similar to TransitFeedSampleCodeTestCase below, but that one
    will no doubt change as the sample code is altered.
    """

    def runTest(self):
        schedule = transitfeed.Schedule()
        schedule.AddAgency(
            "Sample Agency", "http://example.com", "America/Los_Angeles"
        )
        route = transitfeed.Route()
        route.route_id = "SAMPLE_ID"
        route.route_type = 3
        route.route_short_name = "66"
        route.route_long_name = "Sample Route acute letter e\202"
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
        schedule.AddTripObject(trip)

        stop1 = transitfeed.Stop()
        stop1.stop_id = "STOP1"
        stop1.stop_name = "Stop 1 acute letter e\202"
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

        schedule.Validate()
        schedule.WriteGoogleTransitFeed(self.tempfilepath)


class ScheduleBuilderTestCase(util.TempFileTestCaseBase):
    """Tests for using a Schedule object to build a GTFS file."""

    def testBuildFeedWithUtf8Names(self):
        problems = util.GetTestFailureProblemReporter(self)
        schedule = transitfeed.Schedule(problem_reporter=problems)
        schedule.AddAgency(
            b"\xc8\x8b Fly Agency",
            "http://iflyagency.com",
            "America/Los_Angeles",
        )
        service_period = schedule.GetDefaultServicePeriod()
        service_period.SetDateHasService("20070101")
        # "u020b i with inverted accent breve" encoded in utf-8
        stop1 = schedule.AddStop(lng=140, lat=48.2, name="\xc8\x8b hub")
        # "u020b i with inverted accent breve" as unicode string
        stop2 = schedule.AddStop(
            lng=140.001, lat=48.201, name="remote \u020b station"
        )
        route = schedule.AddRoute("\u03b2", "Beta", "Bus")
        trip = route.AddTrip(schedule, "to remote \u020b station")
        repr(stop1)
        repr(stop2)
        repr(route)
        repr(trip)
        trip.AddStopTime(stop1, schedule=schedule, stop_time="10:00:00")
        trip.AddStopTime(stop2, stop_time="10:10:00")

        schedule.Validate(problems)
        schedule.WriteGoogleTransitFeed(self.tempfilepath)
        read_schedule = transitfeed.Loader(
            self.tempfilepath, problems=problems, extra_validation=True
        ).Load()
        self.assertEqual(
            "\u020b Fly Agency", read_schedule.GetDefaultAgency().agency_name
        )
        self.assertEqual(
            "\u03b2", read_schedule.GetRoute(route.route_id).route_short_name
        )
        self.assertEqual(
            "to remote \u020b station",
            read_schedule.GetTrip(trip.trip_id).trip_headsign,
        )

    def testBuildSimpleFeed(self):
        """Make a very simple feed using the Schedule class."""
        problems = util.GetTestFailureProblemReporter(
            self, ("ExpirationDate", "NoServiceExceptions")
        )
        schedule = transitfeed.Schedule(problem_reporter=problems)

        schedule.AddAgency(
            "Test Agency", "http://example.com", "America/Los_Angeles"
        )

        service_period = schedule.GetDefaultServicePeriod()
        self.assertTrue(service_period.service_id)
        service_period.SetWeekdayService(has_service=True)
        service_period.SetStartDate("20070320")
        service_period.SetEndDate("20071231")

        stop1 = schedule.AddStop(
            lng=-140.12, lat=48.921, name="one forty at forty eight"
        )
        stop2 = schedule.AddStop(
            lng=-140.22, lat=48.421, name="west and south"
        )
        stop3 = schedule.AddStop(lng=-140.32, lat=48.121, name="more away")
        stop4 = schedule.AddStop(
            lng=-140.42, lat=48.021, name="more more away"
        )

        route = schedule.AddRoute(
            short_name="R", long_name="My Route", route_type="Bus"
        )
        self.assertTrue(route.route_id)
        self.assertEqual(route.route_short_name, "R")
        self.assertEqual(route.route_type, 3)

        trip = route.AddTrip(
            schedule, headsign="To The End", service_period=service_period
        )
        trip_id = trip.trip_id
        self.assertTrue(trip_id)
        trip = schedule.GetTrip(trip_id)
        self.assertEqual("To The End", trip.trip_headsign)
        self.assertEqual(service_period, trip.service_period)

        trip.AddStopTime(
            stop=stop1, arrival_secs=3600 * 8, departure_secs=3600 * 8
        )
        trip.AddStopTime(stop=stop2)
        trip.AddStopTime(
            stop=stop3,
            arrival_secs=3600 * 8 + 60 * 60,
            departure_secs=3600 * 8 + 60 * 60,
        )
        trip.AddStopTime(
            stop=stop4,
            arrival_time="9:13:00",
            departure_secs=3600 * 8 + 60 * 103,
            stop_headsign="Last stop",
            pickup_type=1,
            drop_off_type=3,
        )

        schedule.Validate()
        schedule.WriteGoogleTransitFeed(self.tempfilepath)
        read_schedule = transitfeed.Loader(
            self.tempfilepath, problems=problems, extra_validation=True
        ).Load()
        self.assertEqual(4, len(read_schedule.GetTrip(trip_id).GetTimeStops()))
        self.assertEqual(1, len(read_schedule.GetRouteList()))
        self.assertEqual(4, len(read_schedule.GetStopList()))

    def testStopIdConflict(self):
        problems = util.GetTestFailureProblemReporter(self)
        schedule = transitfeed.Schedule(problem_reporter=problems)
        schedule.AddStop(lat=3, lng=4.1, name="stop1", stop_id="1")
        schedule.AddStop(lat=3, lng=4.0, name="stop0", stop_id="0")
        schedule.AddStop(lat=3, lng=4.2, name="stop2")
        schedule.AddStop(lat=3, lng=4.2, name="stop4", stop_id="4")
        # AddStop will try to use stop_id=4 first but it is taken
        schedule.AddStop(lat=3, lng=4.2, name="stop5")
        stop_list = sorted(schedule.GetStopList(), key=lambda s: s.stop_name)
        self.assertEqual(
            "stop0 stop1 stop2 stop4 stop5",
            " ".join([s.stop_name for s in stop_list]),
        )
        self.assertMatchesRegex(
            r"0 1 2 4 \d{7,9}", " ".join(s.stop_id for s in stop_list)
        )

    def testRouteIdConflict(self):
        problems = util.GetTestFailureProblemReporter(self)
        schedule = transitfeed.Schedule(problem_reporter=problems)
        route0 = schedule.AddRoute("0", "Long Name", "Bus")
        route1 = schedule.AddRoute("1", "", "Bus", route_id="1")
        route3 = schedule.AddRoute("3", "", "Bus", route_id="3")
        route_rand = schedule.AddRoute("R", "LNR", "Bus")
        route4 = schedule.AddRoute("4", "GooCar", "Bus")
        route_list = schedule.GetRouteList()
        route_list.sort(key=lambda r: r.route_short_name)
        self.assertEqual(
            "0 1 3 4 R", " ".join(r.route_short_name for r in route_list)
        )
        self.assertMatchesRegex(
            "0 1 3 4 \d{7,9}", " ".join(r.route_id for r in route_list)
        )
        self.assertEqual(
            "Long Name,,,GooCar,LNR",
            ",".join(r.route_long_name for r in route_list),
        )

    def testTripIdConflict(self):
        problems = util.GetTestFailureProblemReporter(self)
        schedule = transitfeed.Schedule(problem_reporter=problems)
        service_period = schedule.GetDefaultServicePeriod()
        service_period.SetDateHasService("20070101")
        route = schedule.AddRoute("0", "Long Name", "Bus")
        route.AddTrip()
        route.AddTrip(
            schedule=schedule,
            headsign="hs1",
            service_period=service_period,
            trip_id="1",
        )
        route.AddTrip(schedule, "hs2", service_period, "2")
        route.AddTrip(trip_id="4")
        route.AddTrip()  # This will be given a random trip_id
        trip_list = sorted(
            schedule.GetTripList(), key=lambda t: int(t.trip_id)
        )
        self.assertMatchesRegex(
            "0 1 2 4 \d{7,9}", " ".join(t.trip_id for t in trip_list)
        )
        self.assertEqual(
            ",hs1,hs2,,", ",".join(t["trip_headsign"] for t in trip_list)
        )
        for t in trip_list:
            self.assertEqual(service_period.service_id, t.service_id)
            self.assertEqual(route.route_id, t.route_id)


class WriteSampleFeedTestCase(util.TempFileTestCaseBase):
    def assertEqualTimeString(self, a, b):
        """Assert that a and b are equal, even if they don't have the same zero
        padding on the hour. IE 08:45:00 vs 8:45:00."""
        if a[1] == ":":
            a = "0" + a
        if b[1] == ":":
            b = "0" + b
        self.assertEqual(a, b)

    def assertEqualWithDefault(self, a, b, default):
        """Assert that a and b are equal. Treat None and default as equal."""
        if a == b:
            return
        if a in (None, default) and b in (None, default):
            return
        self.assertTrue(False, "a=%s b=%s" % (a, b))

    def runTest(self):
        accumulator = util.RecordingProblemAccumulator(
            self, ignore_types=("ExpirationDate",)
        )
        problems = transitfeed.ProblemReporter(accumulator)
        schedule = transitfeed.Schedule(problem_reporter=problems)
        agency = transitfeed.Agency()
        agency.agency_id = "DTA"
        agency.agency_name = "Demo Transit Authority"
        agency.agency_url = "http://google.com"
        agency.agency_timezone = "America/Los_Angeles"
        agency.agency_lang = "en"
        # Test that unknown columns, such as agency_mission, are preserved
        agency.agency_mission = "Get You There"
        schedule.AddAgencyObject(agency)

        routes = []
        route_data = [
            ("AB", "DTA", "10", "Airport - Bullfrog", 3),
            ("BFC", "DTA", "20", "Bullfrog - Furnace Creek Resort", 3),
            ("STBA", "DTA", "30", "Stagecoach - Airport Shuttle", 3),
            ("CITY", "DTA", "40", "City", 3),
            ("AAMV", "DTA", "50", "Airport - Amargosa Valley", 3),
        ]

        for route_entry in route_data:
            route = transitfeed.Route()
            (
                route.route_id,
                route.agency_id,
                route.route_short_name,
                route.route_long_name,
                route.route_type,
            ) = route_entry
            routes.append(route)
            schedule.AddRouteObject(route)

        shape_data = [
            (36.915760, -116.751709),
            (36.905018, -116.763206),
            (36.902134, -116.777969),
            (36.904091, -116.788185),
            (36.883602, -116.814537),
            (36.874523, -116.795593),
            (36.873302, -116.786491),
            (36.869202, -116.784241),
            (36.868515, -116.784729),
        ]

        shape = transitfeed.Shape("BFC1S")
        for (lat, lon) in shape_data:
            shape.AddPoint(lat, lon)
        schedule.AddShapeObject(shape)

        week_period = transitfeed.ServicePeriod()
        week_period.service_id = "FULLW"
        week_period.start_date = "20070101"
        week_period.end_date = "20071231"
        week_period.SetWeekdayService()
        week_period.SetWeekendService()
        week_period.SetDateHasService("20070604", False)
        schedule.AddServicePeriodObject(week_period)

        weekend_period = transitfeed.ServicePeriod()
        weekend_period.service_id = "WE"
        weekend_period.start_date = "20070101"
        weekend_period.end_date = "20071231"
        weekend_period.SetWeekendService()
        schedule.AddServicePeriodObject(weekend_period)

        stops = []
        stop_data = [
            (
                "FUR_CREEK_RES",
                "Furnace Creek Resort (Demo)",
                36.425288,
                -117.133162,
                "zone-a",
                "1234",
            ),
            (
                "BEATTY_AIRPORT",
                "Nye County Airport (Demo)",
                36.868446,
                -116.784682,
                "zone-a",
                "1235",
            ),
            (
                "BULLFROG",
                "Bullfrog (Demo)",
                36.88108,
                -116.81797,
                "zone-b",
                "1236",
            ),
            (
                "STAGECOACH",
                "Stagecoach Hotel & Casino (Demo)",
                36.915682,
                -116.751677,
                "zone-c",
                "1237",
            ),
            (
                "NADAV",
                "North Ave / D Ave N (Demo)",
                36.914893,
                -116.76821,
                "",
                "",
            ),
            (
                "NANAA",
                "North Ave / N A Ave (Demo)",
                36.914944,
                -116.761472,
                "",
                "",
            ),
            (
                "DADAN",
                "Doing AVe / D Ave N (Demo)",
                36.909489,
                -116.768242,
                "",
                "",
            ),
            (
                "EMSI",
                "E Main St / S Irving St (Demo)",
                36.905697,
                -116.76218,
                "",
                "",
            ),
            ("AMV", "Amargosa Valley (Demo)", 36.641496, -116.40094, "", ""),
        ]
        for stop_entry in stop_data:
            stop = transitfeed.Stop()
            (
                stop.stop_id,
                stop.stop_name,
                stop.stop_lat,
                stop.stop_lon,
                stop.zone_id,
                stop.stop_code,
            ) = stop_entry
            schedule.AddStopObject(stop)
            stops.append(stop)
        # Add a value to an unknown column and make sure it is preserved
        schedule.GetStop("BULLFROG").stop_sound = "croak!"

        trip_data = [
            ("AB", "FULLW", "AB1", "to Bullfrog", "0", "1", None),
            ("AB", "FULLW", "AB2", "to Airport", "1", "2", None),
            ("STBA", "FULLW", "STBA", "Shuttle", None, None, None),
            ("CITY", "FULLW", "CITY1", None, "0", None, None),
            ("CITY", "FULLW", "CITY2", None, "1", None, None),
            (
                "BFC",
                "FULLW",
                "BFC1",
                "to Furnace Creek Resort",
                "0",
                "1",
                "BFC1S",
            ),
            ("BFC", "FULLW", "BFC2", "to Bullfrog", "1", "2", None),
            ("AAMV", "WE", "AAMV1", "to Amargosa Valley", "0", None, None),
            ("AAMV", "WE", "AAMV2", "to Airport", "1", None, None),
            ("AAMV", "WE", "AAMV3", "to Amargosa Valley", "0", None, None),
            ("AAMV", "WE", "AAMV4", "to Airport", "1", None, None),
        ]

        trips = []
        for trip_entry in trip_data:
            trip = transitfeed.Trip()
            (
                trip.route_id,
                trip.service_id,
                trip.trip_id,
                trip.trip_headsign,
                trip.direction_id,
                trip.block_id,
                trip.shape_id,
            ) = trip_entry
            trips.append(trip)
            schedule.AddTripObject(trip)

        stop_time_data = {
            "STBA": [
                ("6:00:00", "6:00:00", "STAGECOACH", None, None, None, None),
                (
                    "6:20:00",
                    "6:20:00",
                    "BEATTY_AIRPORT",
                    None,
                    None,
                    None,
                    None,
                ),
            ],
            "CITY1": [
                ("6:00:00", "6:00:00", "STAGECOACH", 1.34, 0, 0, "stop 1"),
                ("6:05:00", "6:07:00", "NANAA", 2.40, 1, 2, "stop 2"),
                ("6:12:00", "6:14:00", "NADAV", 3.0, 2, 2, "stop 3"),
                ("6:19:00", "6:21:00", "DADAN", 4, 2, 2, "stop 4"),
                ("6:26:00", "6:28:00", "EMSI", 5.78, 2, 3, "stop 5"),
            ],
            "CITY2": [
                ("6:28:00", "6:28:00", "EMSI", None, None, None, None),
                ("6:35:00", "6:37:00", "DADAN", None, None, None, None),
                ("6:42:00", "6:44:00", "NADAV", None, None, None, None),
                ("6:49:00", "6:51:00", "NANAA", None, None, None, None),
                ("6:56:00", "6:58:00", "STAGECOACH", None, None, None, None),
            ],
            "AB1": [
                (
                    "8:00:00",
                    "8:00:00",
                    "BEATTY_AIRPORT",
                    None,
                    None,
                    None,
                    None,
                ),
                ("8:10:00", "8:15:00", "BULLFROG", None, None, None, None),
            ],
            "AB2": [
                ("12:05:00", "12:05:00", "BULLFROG", None, None, None, None),
                (
                    "12:15:00",
                    "12:15:00",
                    "BEATTY_AIRPORT",
                    None,
                    None,
                    None,
                    None,
                ),
            ],
            "BFC1": [
                ("8:20:00", "8:20:00", "BULLFROG", None, None, None, None),
                (
                    "9:20:00",
                    "9:20:00",
                    "FUR_CREEK_RES",
                    None,
                    None,
                    None,
                    None,
                ),
            ],
            "BFC2": [
                (
                    "11:00:00",
                    "11:00:00",
                    "FUR_CREEK_RES",
                    None,
                    None,
                    None,
                    None,
                ),
                ("12:00:00", "12:00:00", "BULLFROG", None, None, None, None),
            ],
            "AAMV1": [
                (
                    "8:00:00",
                    "8:00:00",
                    "BEATTY_AIRPORT",
                    None,
                    None,
                    None,
                    None,
                ),
                ("9:00:00", "9:00:00", "AMV", None, None, None, None),
            ],
            "AAMV2": [
                ("10:00:00", "10:00:00", "AMV", None, None, None, None),
                (
                    "11:00:00",
                    "11:00:00",
                    "BEATTY_AIRPORT",
                    None,
                    None,
                    None,
                    None,
                ),
            ],
            "AAMV3": [
                (
                    "13:00:00",
                    "13:00:00",
                    "BEATTY_AIRPORT",
                    None,
                    None,
                    None,
                    None,
                ),
                ("14:00:00", "14:00:00", "AMV", None, None, None, None),
            ],
            "AAMV4": [
                ("15:00:00", "15:00:00", "AMV", None, None, None, None),
                (
                    "16:00:00",
                    "16:00:00",
                    "BEATTY_AIRPORT",
                    None,
                    None,
                    None,
                    None,
                ),
            ],
        }

        for trip_id, stop_time_list in list(stop_time_data.items()):
            for stop_time_entry in stop_time_list:
                (
                    arrival_time,
                    departure_time,
                    stop_id,
                    shape_dist_traveled,
                    pickup_type,
                    drop_off_type,
                    stop_headsign,
                ) = stop_time_entry
                trip = schedule.GetTrip(trip_id)
                stop = schedule.GetStop(stop_id)
                trip.AddStopTime(
                    stop,
                    arrival_time=arrival_time,
                    departure_time=departure_time,
                    shape_dist_traveled=shape_dist_traveled,
                    pickup_type=pickup_type,
                    drop_off_type=drop_off_type,
                    stop_headsign=stop_headsign,
                )

        self.assertEqual(
            0, schedule.GetTrip("CITY1").GetStopTimes()[0].pickup_type
        )
        self.assertEqual(
            1, schedule.GetTrip("CITY1").GetStopTimes()[1].pickup_type
        )

        headway_data = [
            ("STBA", "6:00:00", "22:00:00", 1800),
            ("CITY1", "6:00:00", "7:59:59", 1800),
            ("CITY2", "6:00:00", "7:59:59", 1800),
            ("CITY1", "8:00:00", "9:59:59", 600),
            ("CITY2", "8:00:00", "9:59:59", 600),
            ("CITY1", "10:00:00", "15:59:59", 1800),
            ("CITY2", "10:00:00", "15:59:59", 1800),
            ("CITY1", "16:00:00", "18:59:59", 600),
            ("CITY2", "16:00:00", "18:59:59", 600),
            ("CITY1", "19:00:00", "22:00:00", 1800),
            ("CITY2", "19:00:00", "22:00:00", 1800),
        ]

        headway_trips = {}
        for headway_entry in headway_data:
            (trip_id, start_time, end_time, headway) = headway_entry
            headway_trips[trip_id] = []  # adding to set to check later
            trip = schedule.GetTrip(trip_id)
            trip.AddFrequency(start_time, end_time, headway, 0, problems)
        for trip_id in headway_trips:
            headway_trips[trip_id] = schedule.GetTrip(
                trip_id
            ).GetFrequencyTuples()

        fare_data = [("p", 1.25, "USD", 0, 0), ("a", 5.25, "USD", 0, 0)]

        fares = []
        for fare_entry in fare_data:
            fare = transitfeed.FareAttribute(
                fare_entry[0],
                fare_entry[1],
                fare_entry[2],
                fare_entry[3],
                fare_entry[4],
            )
            fares.append(fare)
            schedule.AddFareAttributeObject(fare)

        fare_rule_data = [
            ("p", "AB", "zone-a", "zone-b", None),
            ("p", "STBA", "zone-a", None, "zone-c"),
            ("p", "BFC", None, "zone-b", "zone-a"),
            ("a", "AAMV", None, None, None),
        ]

        for fare_id, route_id, orig_id, dest_id, contains_id in fare_rule_data:
            rule = transitfeed.FareRule(
                fare_id=fare_id,
                route_id=route_id,
                origin_id=orig_id,
                destination_id=dest_id,
                contains_id=contains_id,
            )
            schedule.AddFareRuleObject(rule, problems)

        feed_info = transitfeed.FeedInfo()
        feed_info.feed_version = "0.0.1"
        feed_info.feed_start_date = "20150101"
        feed_info.feed_end_date = "20151212"
        feed_info.feed_publisher_name = "Some Agency"
        feed_info.feed_publisher_url = "http://www.aurl.com"
        feed_info.feed_lang = "en"
        schedule.AddFeedInfoObject(feed_info)

        schedule.Validate(problems)
        accumulator.AssertNoMoreExceptions()
        schedule.WriteGoogleTransitFeed(self.tempfilepath)

        read_schedule = transitfeed.Loader(
            self.tempfilepath, problems=problems, extra_validation=True
        ).Load()
        e = accumulator.PopException("UnrecognizedColumn")
        self.assertEqual(e.file_name, "agency.txt")
        self.assertEqual(e.column_name, "agency_mission")
        e = accumulator.PopException("UnrecognizedColumn")
        self.assertEqual(e.file_name, "stops.txt")
        self.assertEqual(e.column_name, "stop_sound")
        accumulator.AssertNoMoreExceptions()

        self.assertEqual(1, len(read_schedule.GetAgencyList()))
        self.assertEqual(agency, read_schedule.GetAgency(agency.agency_id))

        self.assertEqual(len(routes), len(read_schedule.GetRouteList()))
        for route in routes:
            self.assertEqual(route, read_schedule.GetRoute(route.route_id))

        self.assertEqual(2, len(read_schedule.GetServicePeriodList()))
        self.assertEqual(
            week_period, read_schedule.GetServicePeriod(week_period.service_id)
        )
        self.assertEqual(
            weekend_period,
            read_schedule.GetServicePeriod(weekend_period.service_id),
        )

        self.assertEqual(len(stops), len(read_schedule.GetStopList()))
        for stop in stops:
            self.assertEqual(stop, read_schedule.GetStop(stop.stop_id))
        self.assertEqual(
            "croak!", read_schedule.GetStop("BULLFROG").stop_sound
        )

        self.assertEqual(len(trips), len(read_schedule.GetTripList()))
        for trip in trips:
            self.assertEqual(trip, read_schedule.GetTrip(trip.trip_id))

        for trip_id in headway_trips:
            self.assertEqual(
                headway_trips[trip_id],
                read_schedule.GetTrip(trip_id).GetFrequencyTuples(),
            )

        for trip_id, stop_time_list in list(stop_time_data.items()):
            trip = read_schedule.GetTrip(trip_id)
            read_stoptimes = trip.GetStopTimes()
            self.assertEqual(len(read_stoptimes), len(stop_time_list))
            for stop_time_entry, read_stoptime in zip(
                stop_time_list, read_stoptimes
            ):
                (
                    arrival_time,
                    departure_time,
                    stop_id,
                    shape_dist_traveled,
                    pickup_type,
                    drop_off_type,
                    stop_headsign,
                ) = stop_time_entry
                self.assertEqual(stop_id, read_stoptime.stop_id)
                self.assertEqual(
                    read_schedule.GetStop(stop_id), read_stoptime.stop
                )
                self.assertEqualTimeString(
                    arrival_time, read_stoptime.arrival_time
                )
                self.assertEqualTimeString(
                    departure_time, read_stoptime.departure_time
                )
                self.assertEqual(
                    shape_dist_traveled, read_stoptime.shape_dist_traveled
                )
                self.assertEqualWithDefault(
                    pickup_type, read_stoptime.pickup_type, 0
                )
                self.assertEqualWithDefault(
                    drop_off_type, read_stoptime.drop_off_type, 0
                )
                self.assertEqualWithDefault(
                    stop_headsign, read_stoptime.stop_headsign, ""
                )

        self.assertEqual(len(fares), len(read_schedule.GetFareAttributeList()))
        for fare in fares:
            self.assertEqual(
                fare, read_schedule.GetFareAttribute(fare.fare_id)
            )

        read_fare_rules_data = []
        for fare in read_schedule.GetFareAttributeList():
            for rule in fare.GetFareRuleList():
                self.assertEqual(fare.fare_id, rule.fare_id)
                read_fare_rules_data.append(
                    (
                        fare.fare_id,
                        rule.route_id,
                        rule.origin_id,
                        rule.destination_id,
                        rule.contains_id,
                    )
                )

        fare_rule_data.sort()
        read_fare_rules_data.sort()
        self.assertEqual(len(read_fare_rules_data), len(fare_rule_data))
        for rf, f in zip(read_fare_rules_data, fare_rule_data):
            self.assertEqual(rf, f)

        self.assertEqual(1, len(read_schedule.GetShapeList()))
        self.assertEqual(shape, read_schedule.GetShape(shape.shape_id))

        self.assertEqual(feed_info, read_schedule.feed_info)
        self.assertEqual(
            feed_info.feed_publisher_name,
            read_schedule.feed_info.feed_publisher_name,
        )
        self.assertEqual(
            "http://www.aurl.com", read_schedule.feed_info.feed_publisher_url
        )
