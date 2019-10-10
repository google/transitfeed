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

# Unit tests for the stoptime module.


import transitfeed
from tests import util


class ZeroBasedStopSequenceTestCase(util.LoadTestCase):
    def runTest(self):
        self.ExpectInvalidValue("negative_stop_sequence", "stop_sequence")


class StopTimeValidationTestCase(util.ValidationTestCase):
    def runTest(self):
        stop = transitfeed.Stop()
        self.ExpectInvalidValueInClosure(
            "arrival_time",
            "1a:00:00",
            lambda: transitfeed.StopTime(
                self.problems, stop, arrival_time="1a:00:00"
            ),
        )
        self.ExpectInvalidValueInClosure(
            "departure_time",
            "1a:00:00",
            lambda: transitfeed.StopTime(
                self.problems,
                stop,
                arrival_time="10:00:00",
                departure_time="1a:00:00",
            ),
        )
        self.ExpectInvalidValueInClosure(
            "pickup_type",
            "7.8",
            lambda: transitfeed.StopTime(
                self.problems,
                stop,
                arrival_time="10:00:00",
                departure_time="10:05:00",
                pickup_type="7.8",
                drop_off_type="0",
            ),
        )
        self.ExpectInvalidValueInClosure(
            "drop_off_type",
            "a",
            lambda: transitfeed.StopTime(
                self.problems,
                stop,
                arrival_time="10:00:00",
                departure_time="10:05:00",
                pickup_type="3",
                drop_off_type="a",
            ),
        )
        self.ExpectInvalidValueInClosure(
            "shape_dist_traveled",
            "$",
            lambda: transitfeed.StopTime(
                self.problems,
                stop,
                arrival_time="10:00:00",
                departure_time="10:05:00",
                pickup_type="3",
                drop_off_type="0",
                shape_dist_traveled="$",
            ),
        )
        self.ExpectInvalidValueInClosure(
            "shape_dist_traveled",
            "0,53",
            lambda: transitfeed.StopTime(
                self.problems,
                stop,
                arrival_time="10:00:00",
                departure_time="10:05:00",
                pickup_type="3",
                drop_off_type="0",
                shape_dist_traveled="0,53",
            ),
        )
        self.ExpectOtherProblemInClosure(
            lambda: transitfeed.StopTime(
                self.problems, stop, pickup_type="1", drop_off_type="1"
            )
        )
        self.ExpectInvalidValueInClosure(
            "timepoint",
            "x",
            lambda: transitfeed.StopTime(self.problems, stop, timepoint="x"),
        )
        self.ExpectInvalidValueInClosure(
            "timepoint",
            "2",
            lambda: transitfeed.StopTime(self.problems, stop, timepoint="2"),
        )
        self.ExpectInvalidValueInClosure(
            "departure_time",
            "10:00:00",
            lambda: transitfeed.StopTime(
                self.problems,
                stop,
                arrival_time="11:00:00",
                departure_time="10:00:00",
            ),
        )
        self.ExpectMissingValueInClosure(
            "arrival_time",
            lambda: transitfeed.StopTime(
                self.problems, stop, departure_time="10:00:00"
            ),
        )
        self.ExpectMissingValueInClosure(
            "arrival_time",
            lambda: transitfeed.StopTime(
                self.problems, stop, departure_time="10:00:00", arrival_time=""
            ),
        )
        self.ExpectMissingValueInClosure(
            "departure_time",
            lambda: transitfeed.StopTime(
                self.problems, stop, arrival_time="10:00:00"
            ),
        )
        self.ExpectMissingValueInClosure(
            "departure_time",
            lambda: transitfeed.StopTime(
                self.problems, stop, arrival_time="10:00:00", departure_time=""
            ),
        )
        self.ExpectInvalidValueInClosure(
            "departure_time",
            "10:70:00",
            lambda: transitfeed.StopTime(
                self.problems,
                stop,
                arrival_time="10:00:00",
                departure_time="10:70:00",
            ),
        )
        self.ExpectInvalidValueInClosure(
            "departure_time",
            "10:00:62",
            lambda: transitfeed.StopTime(
                self.problems,
                stop,
                arrival_time="10:00:00",
                departure_time="10:00:62",
            ),
        )
        self.ExpectInvalidValueInClosure(
            "arrival_time",
            "10:00:63",
            lambda: transitfeed.StopTime(
                self.problems,
                stop,
                arrival_time="10:00:63",
                departure_time="10:10:00",
            ),
        )
        self.ExpectInvalidValueInClosure(
            "arrival_time",
            "10:60:00",
            lambda: transitfeed.StopTime(
                self.problems,
                stop,
                arrival_time="10:60:00",
                departure_time="11:02:00",
            ),
        )
        self.ExpectInvalidValueInClosure(
            "stop",
            "id",
            lambda: transitfeed.StopTime(
                self.problems,
                "id",
                arrival_time="10:00:00",
                departure_time="11:02:00",
            ),
        )
        self.ExpectInvalidValueInClosure(
            "stop",
            "3",
            lambda: transitfeed.StopTime(
                self.problems,
                "3",
                arrival_time="10:00:00",
                departure_time="11:02:00",
            ),
        )
        self.ExpectInvalidValueInClosure(
            "stop",
            None,
            lambda: transitfeed.StopTime(
                self.problems,
                None,
                arrival_time="10:00:00",
                departure_time="11:02:00",
            ),
        )

        # The following should work
        transitfeed.StopTime(
            self.problems,
            stop,
            arrival_time="10:00:00",
            departure_time="10:05:00",
            pickup_type="1",
            drop_off_type="1",
        )
        transitfeed.StopTime(
            self.problems,
            stop,
            arrival_time="10:00:00",
            departure_time="10:05:00",
            pickup_type="1",
            drop_off_type="1",
        )
        transitfeed.StopTime(
            self.problems,
            stop,
            arrival_time="1:00:00",
            departure_time="1:05:00",
        )
        transitfeed.StopTime(
            self.problems,
            stop,
            arrival_time="24:59:00",
            departure_time="25:05:00",
        )
        transitfeed.StopTime(
            self.problems,
            stop,
            arrival_time="101:01:00",
            departure_time="101:21:00",
        )
        transitfeed.StopTime(self.problems, stop)
        transitfeed.StopTime(self.problems, stop, timepoint=None)
        transitfeed.StopTime(self.problems, stop, timepoint=1)
        transitfeed.StopTime(self.problems, stop, timepoint="1")
        self.accumulator.AssertNoMoreExceptions()


class TooFastTravelTestCase(util.ValidationTestCase):
    def setUp(self):
        super(TooFastTravelTestCase, self).setUp()
        self.schedule = self.SimpleSchedule()
        self.route = self.schedule.GetRoute("054C")
        self.trip = self.route.AddTrip()

    def AddStopDistanceTime(self, dist_time_list):
        # latitude where each 0.01 degrees longitude is 1km
        magic_lat = 26.062468289
        stop = self.schedule.AddStop(magic_lat, 0, "Demo Stop 0")
        time = 0
        self.trip.AddStopTime(stop, arrival_secs=time, departure_secs=time)
        for i, (dist_delta, time_delta) in enumerate(dist_time_list):
            stop = self.schedule.AddStop(
                magic_lat,
                stop.stop_lon + dist_delta * 0.00001,
                "Demo Stop %d" % (i + 1),
            )
            time += time_delta
            self.trip.AddStopTime(stop, arrival_secs=time, departure_secs=time)

    def testMovingTooFast(self):
        self.AddStopDistanceTime([(1691, 60), (1616, 60)])

        self.trip.Validate(self.problems)
        e = self.accumulator.PopException("TooFastTravel")
        self.assertMatchesRegex(
            r"High speed travel detected", e.FormatProblem()
        )
        self.assertMatchesRegex(r"Stop 0 to Demo Stop 1", e.FormatProblem())
        self.assertMatchesRegex(
            r"1691 meters in 60 seconds", e.FormatProblem()
        )
        self.assertMatchesRegex(r"\(101 km/h\)", e.FormatProblem())
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        self.accumulator.AssertNoMoreExceptions()

        self.route.route_type = 4  # Ferry with max_speed 80
        self.trip.Validate(self.problems)
        e = self.accumulator.PopException("TooFastTravel")
        self.assertMatchesRegex(
            r"High speed travel detected", e.FormatProblem()
        )
        self.assertMatchesRegex(r"Stop 0 to Demo Stop 1", e.FormatProblem())
        self.assertMatchesRegex(
            r"1691 meters in 60 seconds", e.FormatProblem()
        )
        self.assertMatchesRegex(r"\(101 km/h\)", e.FormatProblem())
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        e = self.accumulator.PopException("TooFastTravel")
        self.assertMatchesRegex(
            r"High speed travel detected", e.FormatProblem()
        )
        self.assertMatchesRegex(r"Stop 1 to Demo Stop 2", e.FormatProblem())
        self.assertMatchesRegex(
            r"1616 meters in 60 seconds", e.FormatProblem()
        )
        self.assertMatchesRegex(r"97 km/h", e.FormatProblem())
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        self.accumulator.AssertNoMoreExceptions()

        # Run test without a route_type
        self.route.route_type = None
        self.trip.Validate(self.problems)
        e = self.accumulator.PopException("TooFastTravel")
        self.assertMatchesRegex(
            r"High speed travel detected", e.FormatProblem()
        )
        self.assertMatchesRegex(r"Stop 0 to Demo Stop 1", e.FormatProblem())
        self.assertMatchesRegex(
            r"1691 meters in 60 seconds", e.FormatProblem()
        )
        self.assertMatchesRegex(r"101 km/h", e.FormatProblem())
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        self.accumulator.AssertNoMoreExceptions()

    def testNoTimeDelta(self):
        # See comments where TooFastTravel is called in transitfeed.py to
        # understand why was added.
        # Movement more than max_speed in 1 minute with no time change is a warning.
        self.AddStopDistanceTime([(1616, 0), (1000, 120), (1691, 0)])

        self.trip.Validate(self.problems)
        e = self.accumulator.PopException("TooFastTravel")
        self.assertMatchesRegex(
            "High speed travel detected", e.FormatProblem()
        )
        self.assertMatchesRegex("Stop 2 to Demo Stop 3", e.FormatProblem())
        self.assertMatchesRegex("1691 meters in 0 seconds", e.FormatProblem())
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        self.accumulator.AssertNoMoreExceptions()

        self.route.route_type = 4  # Ferry with max_speed 80
        self.trip.Validate(self.problems)
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        e = self.accumulator.PopException("TooFastTravel")
        self.assertMatchesRegex(
            "High speed travel detected", e.FormatProblem()
        )
        self.assertMatchesRegex("Stop 0 to Demo Stop 1", e.FormatProblem())
        self.assertMatchesRegex("1616 meters in 0 seconds", e.FormatProblem())
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        e = self.accumulator.PopException("TooFastTravel")
        self.assertMatchesRegex(
            "High speed travel detected", e.FormatProblem()
        )
        self.assertMatchesRegex("Stop 2 to Demo Stop 3", e.FormatProblem())
        self.assertMatchesRegex("1691 meters in 0 seconds", e.FormatProblem())
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        self.accumulator.AssertNoMoreExceptions()

        # Run test without a route_type
        self.route.route_type = None
        self.trip.Validate(self.problems)
        e = self.accumulator.PopException("TooFastTravel")
        self.assertMatchesRegex(
            "High speed travel detected", e.FormatProblem()
        )
        self.assertMatchesRegex("Stop 2 to Demo Stop 3", e.FormatProblem())
        self.assertMatchesRegex("1691 meters in 0 seconds", e.FormatProblem())
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        self.accumulator.AssertNoMoreExceptions()

    def testNoTimeDeltaNotRounded(self):
        # See comments where TooFastTravel is called in transitfeed.py to
        # understand why was added.
        # Any movement with no time change and times not rounded to the nearest
        # minute causes a warning.
        self.AddStopDistanceTime([(500, 62), (10, 0)])

        self.trip.Validate(self.problems)
        e = self.accumulator.PopException("TooFastTravel")
        self.assertMatchesRegex(
            "High speed travel detected", e.FormatProblem()
        )
        self.assertMatchesRegex("Stop 1 to Demo Stop 2", e.FormatProblem())
        self.assertMatchesRegex("10 meters in 0 seconds", e.FormatProblem())
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        self.accumulator.AssertNoMoreExceptions()


class TooManyConsecutiveStopTimesWithSameTime(util.TestCase):
    """Check for too many consecutive stop times with same time"""

    def setUp(self):

        # We ignore the lack of service dates ("OtherProblem")
        self.accumulator = util.RecordingProblemAccumulator(
            self, ("OtherProblem")
        )
        self.problems = transitfeed.ProblemReporter(self.accumulator)

        self.schedule = transitfeed.Schedule(problem_reporter=self.problems)
        self.schedule.AddAgency(
            "Demo Transit Authority", "http://dta.org", "America/Los_Angeles"
        )

        self.stop1 = self.schedule.AddStop(
            lng=-116.75167,
            lat=36.915682,
            name="Stagecoach Hotel & Casino",
            stop_id="S1",
        )

        self.stop2 = self.schedule.AddStop(
            lng=-116.76218,
            lat=36.905697,
            name="E Main St / S Irving St",
            stop_id="S2",
        )

        route = self.schedule.AddRoute("", "City", "Bus", route_id="CITY")

        self.trip = route.AddTrip(self.schedule, trip_id="CITY1")

    def testTooManyConsecutiveStopTimesWithSameTime(self):
        trip = self.trip
        trip.AddStopTime(self.stop1, stop_time="6:00:00")
        for _ in range(6):
            trip.AddStopTime(self.stop2, stop_time="6:05:00")
        trip.AddStopTime(self.stop1, stop_time="6:10:00")

        self.schedule.Validate(self.problems)

        e = self.accumulator.PopException(
            "TooManyConsecutiveStopTimesWithSameTime"
        )
        self.assertEqual(e.trip_id, "CITY1")
        self.assertEqual(e.number_of_stop_times, 6)
        self.assertEqual(e.stop_time, "06:05:00")

        self.assertEqual(
            e.FormatProblem(),
            "Trip CITY1 has 6 consecutive stop times all with the same "
            "arrival/departure time: 06:05:00.",
        )

        self.accumulator.AssertNoMoreExceptions()

    def testNotTooManyConsecutiveStopTimesWithSameTime(self):
        trip = self.trip
        trip.AddStopTime(self.stop1, stop_time="6:00:00")
        for _ in range(5):
            trip.AddStopTime(self.stop2, stop_time="6:05:00")
        trip.AddStopTime(self.stop1, stop_time="6:10:00")

        self.schedule.Validate(self.problems)

        self.accumulator.AssertNoMoreExceptions()

    def testTooManyConsecutiveStopTimesWithSameTimeAtStart(self):
        trip = self.trip
        for _ in range(6):
            trip.AddStopTime(self.stop2, stop_time="6:05:00")
        trip.AddStopTime(self.stop1, stop_time="6:10:00")

        self.schedule.Validate(self.problems)

        e = self.accumulator.PopException(
            "TooManyConsecutiveStopTimesWithSameTime"
        )
        self.assertEqual(e.trip_id, "CITY1")
        self.assertEqual(e.number_of_stop_times, 6)
        self.assertEqual(e.stop_time, "06:05:00")

        self.accumulator.AssertNoMoreExceptions()

    def testTooManyConsecutiveStopTimesWithSameTimeAtEnd(self):
        trip = self.trip
        trip.AddStopTime(self.stop1, stop_time="6:00:00")
        for _ in range(6):
            trip.AddStopTime(self.stop2, stop_time="6:05:00")

        self.schedule.Validate(self.problems)

        e = self.accumulator.PopException(
            "TooManyConsecutiveStopTimesWithSameTime"
        )
        self.assertEqual(e.trip_id, "CITY1")
        self.assertEqual(e.number_of_stop_times, 6)
        self.assertEqual(e.stop_time, "06:05:00")

        self.accumulator.AssertNoMoreExceptions()

    def testTooManyConsecutiveStopTimesWithUnspecifiedTimes(self):
        trip = self.trip
        trip.AddStopTime(self.stop1, stop_time="6:05:00")
        for _ in range(4):
            trip.AddStopTime(self.stop2)
        trip.AddStopTime(self.stop1, stop_time="6:05:00")

        self.schedule.Validate(self.problems)

        e = self.accumulator.PopException(
            "TooManyConsecutiveStopTimesWithSameTime"
        )
        self.assertEqual(e.trip_id, "CITY1")
        self.assertEqual(e.number_of_stop_times, 6)
        self.assertEqual(e.stop_time, "06:05:00")

        self.accumulator.AssertNoMoreExceptions()

    def testNotTooManyConsecutiveStopTimesWithUnspecifiedTimes(self):
        trip = self.trip
        trip.AddStopTime(self.stop1, stop_time="6:00:00")
        for _ in range(4):
            trip.AddStopTime(self.stop2)
        trip.AddStopTime(self.stop1, stop_time="6:05:00")

        self.schedule.Validate(self.problems)

        self.accumulator.AssertNoMoreExceptions()
