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


import re
import time
from datetime import date

import transitfeed
from tests import util


class DuplicateStopTestCase(util.TestCase):
    def runTest(self):
        schedule = transitfeed.Schedule(
            problem_reporter=util.ExceptionProblemReporterNoExpiration()
        )
        try:
            schedule.Load(
                util.DataPath("duplicate_stop"), extra_validation=True
            )
            self.fail("OtherProblem exception expected")
        except transitfeed.OtherProblem:
            pass


class DuplicateScheduleIDTestCase(util.TestCase):
    def runTest(self):
        schedule = transitfeed.Schedule(
            problem_reporter=util.ExceptionProblemReporterNoExpiration()
        )
        try:
            schedule.Load(
                util.DataPath("duplicate_schedule_id"), extra_validation=True
            )
            self.fail("DuplicateID exception expected")
        except transitfeed.DuplicateID:
            pass


class OverlappingBlockSchedule(transitfeed.Schedule):
    """Special Schedule subclass that counts the number of calls to
    GetServicePeriod() so we can verify service period overlap calculation
    caching"""

    _get_service_period_call_count = 0

    def GetServicePeriod(self, service_id):
        self._get_service_period_call_count += 1
        return transitfeed.Schedule.GetServicePeriod(self, service_id)

    def GetServicePeriodCallCount(self):
        return self._get_service_period_call_count


class OverlappingBlockTripsTestCase(util.TestCase):
    """Builds a simple schedule for testing of overlapping block trips"""

    def setUp(self):
        self.accumulator = util.RecordingProblemAccumulator(
            self, ("ExpirationDate", "NoServiceExceptions")
        )
        self.problems = transitfeed.ProblemReporter(self.accumulator)

        schedule = OverlappingBlockSchedule(problem_reporter=self.problems)
        schedule.AddAgency(
            "Demo Transit Authority", "http://dta.org", "America/Los_Angeles"
        )

        sp1 = transitfeed.ServicePeriod("SID1")
        sp1.SetWeekdayService(True)
        sp1.SetStartDate("20070605")
        sp1.SetEndDate("20080605")
        schedule.AddServicePeriodObject(sp1)

        sp2 = transitfeed.ServicePeriod("SID2")
        sp2.SetDayOfWeekHasService(0)
        sp2.SetDayOfWeekHasService(2)
        sp2.SetDayOfWeekHasService(4)
        sp2.SetStartDate("20070605")
        sp2.SetEndDate("20080605")
        schedule.AddServicePeriodObject(sp2)

        sp3 = transitfeed.ServicePeriod("SID3")
        sp3.SetWeekendService(True)
        sp3.SetStartDate("20070605")
        sp3.SetEndDate("20080605")
        schedule.AddServicePeriodObject(sp3)

        self.stop1 = schedule.AddStop(
            lng=-116.75167,
            lat=36.915682,
            name="Stagecoach Hotel & Casino",
            stop_id="S1",
        )

        self.stop2 = schedule.AddStop(
            lng=-116.76218,
            lat=36.905697,
            name="E Main St / S Irving St",
            stop_id="S2",
        )

        self.route = schedule.AddRoute("", "City", "Bus", route_id="CITY")

        self.schedule = schedule
        self.sp1 = sp1
        self.sp2 = sp2
        self.sp3 = sp3

    def testNoOverlap(self):
        schedule, route, sp1 = self.schedule, self.route, self.sp1

        trip1 = route.AddTrip(schedule, service_period=sp1, trip_id="CITY1")
        trip1.block_id = "BLOCK"
        trip1.AddStopTime(self.stop1, stop_time="6:00:00")
        trip1.AddStopTime(self.stop2, stop_time="6:30:00")

        trip2 = route.AddTrip(schedule, service_period=sp1, trip_id="CITY2")
        trip2.block_id = "BLOCK"
        trip2.AddStopTime(self.stop2, stop_time="6:30:00")
        trip2.AddStopTime(self.stop1, stop_time="7:00:00")

        schedule.Validate(self.problems)

        self.accumulator.AssertNoMoreExceptions()

    def testOverlapSameServicePeriod(self):
        schedule, route, sp1 = self.schedule, self.route, self.sp1

        trip1 = route.AddTrip(schedule, service_period=sp1, trip_id="CITY1")
        trip1.block_id = "BLOCK"
        trip1.AddStopTime(self.stop1, stop_time="6:00:00")
        trip1.AddStopTime(self.stop2, stop_time="6:30:00")

        trip2 = route.AddTrip(schedule, service_period=sp1, trip_id="CITY2")
        trip2.block_id = "BLOCK"
        trip2.AddStopTime(self.stop2, stop_time="6:20:00")
        trip2.AddStopTime(self.stop1, stop_time="6:50:00")

        schedule.Validate(self.problems)

        e = self.accumulator.PopException("OverlappingTripsInSameBlock")
        self.assertEqual(e.trip_id1, "CITY1")
        self.assertEqual(e.trip_id2, "CITY2")
        self.assertEqual(e.block_id, "BLOCK")

        self.accumulator.AssertNoMoreExceptions()

    def testOverlapDifferentServicePeriods(self):
        schedule, route, sp1, sp2 = (
            self.schedule,
            self.route,
            self.sp1,
            self.sp2,
        )

        trip1 = route.AddTrip(schedule, service_period=sp1, trip_id="CITY1")
        trip1.block_id = "BLOCK"
        trip1.AddStopTime(self.stop1, stop_time="6:00:00")
        trip1.AddStopTime(self.stop2, stop_time="6:30:00")

        trip2 = route.AddTrip(schedule, service_period=sp2, trip_id="CITY2")
        trip2.block_id = "BLOCK"
        trip2.AddStopTime(self.stop2, stop_time="6:20:00")
        trip2.AddStopTime(self.stop1, stop_time="6:50:00")

        trip3 = route.AddTrip(schedule, service_period=sp1, trip_id="CITY3")
        trip3.block_id = "BLOCK"
        trip3.AddStopTime(self.stop1, stop_time="7:00:00")
        trip3.AddStopTime(self.stop2, stop_time="7:30:00")

        trip4 = route.AddTrip(schedule, service_period=sp2, trip_id="CITY4")
        trip4.block_id = "BLOCK"
        trip4.AddStopTime(self.stop2, stop_time="7:20:00")
        trip4.AddStopTime(self.stop1, stop_time="7:50:00")

        schedule.Validate(self.problems)

        e = self.accumulator.PopException("OverlappingTripsInSameBlock")
        self.assertEqual(e.trip_id1, "CITY1")
        self.assertEqual(e.trip_id2, "CITY2")
        self.assertEqual(e.block_id, "BLOCK")

        e = self.accumulator.PopException("OverlappingTripsInSameBlock")
        self.assertEqual(e.trip_id1, "CITY3")
        self.assertEqual(e.trip_id2, "CITY4")
        self.assertEqual(e.block_id, "BLOCK")

        self.accumulator.AssertNoMoreExceptions()

        # If service period overlap calculation caching is working correctly,
        # we expect only two calls to GetServicePeriod(), one each for sp1 and
        # sp2, as oppossed four calls total for the four overlapping trips
        self.assertEqual(2, schedule.GetServicePeriodCallCount())

    def testNoOverlapDifferentServicePeriods(self):
        schedule, route, sp1, sp3 = (
            self.schedule,
            self.route,
            self.sp1,
            self.sp3,
        )

        trip1 = route.AddTrip(schedule, service_period=sp1, trip_id="CITY1")
        trip1.block_id = "BLOCK"
        trip1.AddStopTime(self.stop1, stop_time="6:00:00")
        trip1.AddStopTime(self.stop2, stop_time="6:30:00")

        trip2 = route.AddTrip(schedule, service_period=sp3, trip_id="CITY2")
        trip2.block_id = "BLOCK"
        trip2.AddStopTime(self.stop2, stop_time="6:20:00")
        trip2.AddStopTime(self.stop1, stop_time="6:50:00")

        schedule.Validate(self.problems)

        self.accumulator.AssertNoMoreExceptions()


class StopsNearEachOther(util.MemoryZipTestCase):
    def testTooNear(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon\n"
            "BEATTY_AIRPORT,Airport,48.20000,140\n"
            "BULLFROG,Bullfrog,48.20001,140\n"
            "STAGECOACH,Stagecoach Hotel,48.20016,140\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("StopsTooClose")
        self.assertTrue(e.FormatProblem().find("1.11m apart") != -1)
        self.accumulator.AssertNoMoreExceptions()

    def testJustFarEnough(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon\n"
            "BEATTY_AIRPORT,Airport,48.20000,140\n"
            "BULLFROG,Bullfrog,48.20002,140\n"
            "STAGECOACH,Stagecoach Hotel,48.20016,140\n",
        )
        schedule = self.MakeLoaderAndLoad()
        # Stops are 2.2m apart
        self.accumulator.AssertNoMoreExceptions()

    def testSameLocation(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon\n"
            "BEATTY_AIRPORT,Airport,48.2,140\n"
            "BULLFROG,Bullfrog,48.2,140\n"
            "STAGECOACH,Stagecoach Hotel,48.20016,140\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("StopsTooClose")
        self.assertTrue(e.FormatProblem().find("0.00m apart") != -1)
        self.accumulator.AssertNoMoreExceptions()

    def testStationsTooNear(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            "BEATTY_AIRPORT,Airport,48.20000,140,,BEATTY_AIRPORT_STATION\n"
            "BULLFROG,Bullfrog,48.20003,140,,BULLFROG_STATION\n"
            "BEATTY_AIRPORT_STATION,Airport,48.20001,140,1,\n"
            "BULLFROG_STATION,Bullfrog,48.20002,140,1,\n"
            "STAGECOACH,Stagecoach Hotel,48.20016,140,,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("StationsTooClose")
        self.assertTrue(e.FormatProblem().find("1.11m apart") != -1)
        self.assertTrue(e.FormatProblem().find("BEATTY_AIRPORT_STATION") != -1)
        self.accumulator.AssertNoMoreExceptions()

    def testStopNearNonParentStation(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            "BEATTY_AIRPORT,Airport,48.20000,140,,\n"
            "BULLFROG,Bullfrog,48.20005,140,,\n"
            "BULLFROG_STATION,Bullfrog,48.20006,140,1,\n"
            "STAGECOACH,Stagecoach Hotel,48.20016,140,,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("DifferentStationTooClose")
        fmt = e.FormatProblem()
        self.assertTrue(
            re.search(
                r"parent_station of.*BULLFROG.*station.*BULLFROG_STATION.* 1.11m apart",
                fmt,
            ),
            fmt,
        )
        self.accumulator.AssertNoMoreExceptions()


class NoServiceExceptionsTestCase(util.MemoryZipTestCase):
    def testNoCalendarDates(self):
        self.RemoveArchive("calendar_dates.txt")
        self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("NoServiceExceptions")
        self.accumulator.AssertNoMoreExceptions()

    def testNoExceptionsWhenFeedActiveForShortPeriodOfTime(self):
        self.SetArchiveContents(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\n"
            "FULLW,1,1,1,1,1,1,1,20070101,20070630\n"
            "WE,0,0,0,0,0,1,1,20070101,20070331\n",
        )
        self.RemoveArchive("calendar_dates.txt")
        self.MakeLoaderAndLoad()
        self.accumulator.AssertNoMoreExceptions()

    def testEmptyCalendarDates(self):
        self.SetArchiveContents("calendar_dates.txt", "")
        self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("EmptyFile")
        e = self.accumulator.PopException("NoServiceExceptions")
        self.accumulator.AssertNoMoreExceptions()

    def testCalendarDatesWithHeaderOnly(self):
        self.SetArchiveContents(
            "calendar_dates.txt", "service_id,date,exception_type\n"
        )
        self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("NoServiceExceptions")
        self.accumulator.AssertNoMoreExceptions()

    def testCalendarDatesWithAddedServiceException(self):
        self.SetArchiveContents(
            "calendar_dates.txt",
            "service_id,date,exception_type\n" "FULLW,20070101,1\n",
        )
        self.MakeLoaderAndLoad()
        self.accumulator.AssertNoMoreExceptions()

    def testCalendarDatesWithRemovedServiceException(self):
        self.SetArchiveContents(
            "calendar_dates.txt",
            "service_id,date,exception_type\n" "FULLW,20070101,2\n",
        )
        self.MakeLoaderAndLoad()
        self.accumulator.AssertNoMoreExceptions()


class GetServicePeriodsActiveEachDateTestCase(util.TestCase):
    def testEmpty(self):
        schedule = transitfeed.Schedule()
        self.assertEqual(
            [],
            schedule.GetServicePeriodsActiveEachDate(
                date(2009, 1, 1), date(2009, 1, 1)
            ),
        )
        self.assertEqual(
            [(date(2008, 12, 31), []), (date(2009, 1, 1), [])],
            schedule.GetServicePeriodsActiveEachDate(
                date(2008, 12, 31), date(2009, 1, 2)
            ),
        )

    def testOneService(self):
        schedule = transitfeed.Schedule()
        sp1 = transitfeed.ServicePeriod()
        sp1.service_id = "sp1"
        sp1.SetDateHasService("20090101")
        sp1.SetDateHasService("20090102")
        schedule.AddServicePeriodObject(sp1)
        self.assertEqual(
            [],
            schedule.GetServicePeriodsActiveEachDate(
                date(2009, 1, 1), date(2009, 1, 1)
            ),
        )
        self.assertEqual(
            [(date(2008, 12, 31), []), (date(2009, 1, 1), [sp1])],
            schedule.GetServicePeriodsActiveEachDate(
                date(2008, 12, 31), date(2009, 1, 2)
            ),
        )

    def testTwoService(self):
        schedule = transitfeed.Schedule()
        sp1 = transitfeed.ServicePeriod()
        sp1.service_id = "sp1"
        sp1.SetDateHasService("20081231")
        sp1.SetDateHasService("20090101")

        schedule.AddServicePeriodObject(sp1)
        sp2 = transitfeed.ServicePeriod()
        sp2.service_id = "sp2"
        sp2.SetStartDate("20081201")
        sp2.SetEndDate("20081231")
        sp2.SetWeekendService()
        sp2.SetWeekdayService()
        schedule.AddServicePeriodObject(sp2)
        self.assertEqual(
            [],
            schedule.GetServicePeriodsActiveEachDate(
                date(2009, 1, 1), date(2009, 1, 1)
            ),
        )
        date_services = schedule.GetServicePeriodsActiveEachDate(
            date(2008, 12, 31), date(2009, 1, 2)
        )
        self.assertEqual(
            [date(2008, 12, 31), date(2009, 1, 1)],
            [d for d, _ in date_services],
        )
        self.assertEqual(set([sp1, sp2]), set(date_services[0][1]))
        self.assertEqual([sp1], date_services[1][1])


class DuplicateTripTestCase(util.ValidationTestCase):
    def runTest(self):
        schedule = transitfeed.Schedule(self.problems)
        schedule._check_duplicate_trips = True

        agency = transitfeed.Agency(
            "Demo agency",
            "http://google.com",
            "America/Los_Angeles",
            "agency1",
        )
        schedule.AddAgencyObject(agency)

        service = schedule.GetDefaultServicePeriod()
        service.SetDateHasService("20070101")

        route1 = transitfeed.Route(
            "Route1", "route 1", 3, "route_1", "agency1"
        )
        schedule.AddRouteObject(route1)
        route2 = transitfeed.Route(
            "Route2", "route 2", 3, "route_2", "agency1"
        )
        schedule.AddRouteObject(route2)

        trip1 = transitfeed.Trip()
        trip1.route_id = "route_1"
        trip1.trip_id = "t1"
        trip1.trip_headsign = "via Polish Hill"
        trip1.direction_id = "0"
        trip1.service_id = service.service_id
        schedule.AddTripObject(trip1)

        trip2 = transitfeed.Trip()
        trip2.route_id = "route_2"
        trip2.trip_id = "t2"
        trip2.trip_headsign = "New"
        trip2.direction_id = "0"
        trip2.service_id = service.service_id
        schedule.AddTripObject(trip2)

        trip3 = transitfeed.Trip()
        trip3.route_id = "route_1"
        trip3.trip_id = "t3"
        trip3.trip_headsign = "New Demo"
        trip3.direction_id = "0"
        trip3.service_id = service.service_id
        schedule.AddTripObject(trip3)

        stop1 = transitfeed.Stop(
            36.425288, -117.139162, "Demo Stop 1", "STOP1"
        )
        schedule.AddStopObject(stop1)
        trip1.AddStopTime(
            stop1,
            arrival_time="5:11:00",
            departure_time="5:12:00",
            stop_sequence=0,
            shape_dist_traveled=0,
        )
        trip2.AddStopTime(
            stop1,
            arrival_time="5:11:00",
            departure_time="5:12:00",
            stop_sequence=0,
            shape_dist_traveled=0,
        )
        trip3.AddStopTime(
            stop1,
            arrival_time="6:11:00",
            departure_time="6:12:00",
            stop_sequence=0,
            shape_dist_traveled=0,
        )

        stop2 = transitfeed.Stop(
            36.424288, -117.158142, "Demo Stop 2", "STOP2"
        )
        schedule.AddStopObject(stop2)
        trip1.AddStopTime(
            stop2,
            arrival_time="5:15:00",
            departure_time="5:16:00",
            stop_sequence=1,
            shape_dist_traveled=1,
        )
        trip2.AddStopTime(
            stop2,
            arrival_time="5:25:00",
            departure_time="5:26:00",
            stop_sequence=1,
            shape_dist_traveled=1,
        )
        trip3.AddStopTime(
            stop2,
            arrival_time="6:15:00",
            departure_time="6:16:00",
            stop_sequence=1,
            shape_dist_traveled=1,
        )

        schedule.Validate(self.problems)
        e = self.accumulator.PopException("DuplicateTrip")
        self.assertTrue(e.FormatProblem().find("t1 of route") != -1)
        self.assertTrue(e.FormatProblem().find("t2 of route") != -1)
        self.accumulator.AssertNoMoreExceptions()


class StopBelongsToBothSubwayAndBusTestCase(util.ValidationTestCase):
    def runTest(self):
        schedule = transitfeed.Schedule(self.problems)

        schedule.AddAgency(
            "Demo Agency", "http://example.com", "America/Los_Angeles"
        )
        route1 = schedule.AddRoute(
            short_name="route1", long_name="route_1", route_type=3
        )
        route2 = schedule.AddRoute(
            short_name="route2", long_name="route_2", route_type=1
        )

        service = schedule.GetDefaultServicePeriod()
        service.SetDateHasService("20070101")

        trip1 = route1.AddTrip(schedule, "trip1", service, "t1")
        trip2 = route2.AddTrip(schedule, "trip2", service, "t2")

        stop1 = schedule.AddStop(36.425288, -117.133162, "stop1")
        stop2 = schedule.AddStop(36.424288, -117.133142, "stop2")
        stop3 = schedule.AddStop(36.423288, -117.134142, "stop3")

        trip1.AddStopTime(
            stop1, arrival_time="5:11:00", departure_time="5:12:00"
        )
        trip1.AddStopTime(
            stop2, arrival_time="5:21:00", departure_time="5:22:00"
        )

        trip2.AddStopTime(
            stop1, arrival_time="6:11:00", departure_time="6:12:00"
        )
        trip2.AddStopTime(
            stop3, arrival_time="6:21:00", departure_time="6:22:00"
        )

        schedule.Validate(self.problems)
        e = self.accumulator.PopException("StopWithMultipleRouteTypes")
        self.assertTrue(e.FormatProblem().find("Stop stop1") != -1)
        self.assertTrue(e.FormatProblem().find("subway (ID=1)") != -1)
        self.assertTrue(e.FormatProblem().find("bus line (ID=0)") != -1)
        self.accumulator.AssertNoMoreExceptions()


class UnusedStopAgencyTestCase(util.LoadTestCase):
    def runTest(self):
        self.Load("unused_stop"),
        e = self.accumulator.PopException("UnusedStop")
        self.assertEqual("Bogus Stop (Demo)", e.stop_name)
        self.assertEqual("BOGUS", e.stop_id)
        self.accumulator.AssertNoMoreExceptions()


class ScheduleStartAndExpirationDatesTestCase(util.MemoryZipTestCase):
    # Remove "ExpirationDate" from the accumulator _IGNORE_TYPES to get the
    # expiration errors.
    _IGNORE_TYPES = util.MemoryZipTestCase._IGNORE_TYPES[:]
    _IGNORE_TYPES.remove("ExpirationDate")

    # Init dates to be close to now
    now = time.mktime(time.localtime())
    seconds_per_day = 60 * 60 * 24
    date_format = "%Y%m%d"
    two_weeks_ago = time.strftime(
        date_format, time.localtime(now - 14 * seconds_per_day)
    )
    one_week_ago = time.strftime(
        date_format, time.localtime(now - 7 * seconds_per_day)
    )
    one_week = time.strftime(
        date_format, time.localtime(now + 7 * seconds_per_day)
    )
    two_weeks = time.strftime(
        date_format, time.localtime(now + 14 * seconds_per_day)
    )
    two_months = time.strftime(
        date_format, time.localtime(now + 60 * seconds_per_day)
    )

    def prepareArchiveContents(
        self,
        calendar_start,
        calendar_end,
        exception_date,
        feed_info_start,
        feed_info_end,
    ):
        self.SetArchiveContents(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\n"
            "FULLW,1,1,1,1,1,1,1,%s,%s\n"
            "WE,0,0,0,0,0,1,1,%s,%s\n"
            % (calendar_start, calendar_end, calendar_start, calendar_end),
        )
        self.SetArchiveContents(
            "calendar_dates.txt",
            "service_id,date,exception_type\n"
            "FULLW,%s,1\n" % (exception_date),
        )
        from_column = ""
        if feed_info_start:
            from_column = ",feed_start_date"
            feed_info_start = "," + feed_info_start
        until_column = ""
        if feed_info_end:
            until_column = ",feed_end_date"
            feed_info_end = "," + feed_info_end
        self.SetArchiveContents(
            "feed_info.txt",
            "feed_publisher_name,feed_publisher_url,feed_lang%s%s\n"
            "DTA,http://google.com,en%s%s"
            % (from_column, until_column, feed_info_start, feed_info_end),
        )

    def testNoErrors(self):
        self.prepareArchiveContents(
            self.two_weeks_ago,
            self.two_months,  # calendar
            self.two_weeks,  # calendar_dates
            "",
            "",
        )  # feed_info
        self.MakeLoaderAndLoad(self.problems)
        self.accumulator.AssertNoMoreExceptions()

    def testExpirationDateCausedByServicePeriod(self):
        # test with no validity dates specified in feed_info.txt
        self.prepareArchiveContents(
            self.two_weeks_ago,
            self.two_weeks,  # calendar
            self.one_week,  # calendar_dates
            "",
            "",
        )  # feed_info
        self.MakeLoaderAndLoad(self.problems)
        e = self.accumulator.PopException("ExpirationDate")
        self.assertTrue("calendar.txt" in e.expiration_origin_file)
        self.accumulator.AssertNoMoreExceptions()
        # test with good validity dates specified in feed_info.txt
        self.prepareArchiveContents(
            self.two_weeks_ago,
            self.two_weeks,  # calendar
            self.one_week,  # calendar_dates
            self.two_weeks_ago,
            self.two_months,
        )  # feed_info
        self.MakeLoaderAndLoad(self.problems)
        self.accumulator.AssertNoMoreExceptions()

    def testFutureServiceCausedByServicePeriod(self):
        # test with no validity dates specified in feed_info.txt
        self.prepareArchiveContents(
            self.one_week,
            self.two_months,  # calendar
            self.two_weeks,  # calendar_dates
            "",
            "",
        )  # feed_info
        self.MakeLoaderAndLoad(self.problems)
        e = self.accumulator.PopException("FutureService")
        self.assertTrue("calendar.txt" in e.start_date_origin_file)
        self.accumulator.AssertNoMoreExceptions()
        # Test with good validity dates specified in feed_info.txt
        self.prepareArchiveContents(
            self.one_week,
            self.two_months,  # calendar
            self.two_weeks,  # calendar_dates
            self.two_weeks_ago,
            self.two_months,
        )  # feed_info
        self.MakeLoaderAndLoad(self.problems)
        self.accumulator.AssertNoMoreExceptions()

    def testExpirationDateCausedByServicePeriodDateException(self):
        # Test with no validity dates specified in feed_info.txt
        self.prepareArchiveContents(
            self.two_weeks_ago,
            self.one_week,  # calendar
            self.two_weeks,  # calendar_dates
            "",
            "",
        )  # feed_info
        self.MakeLoaderAndLoad(self.problems)
        e = self.accumulator.PopException("ExpirationDate")
        self.assertTrue("calendar_dates.txt" in e.expiration_origin_file)
        self.accumulator.AssertNoMoreExceptions()
        # Test with good validity dates specified in feed_info.txt
        self.prepareArchiveContents(
            self.two_weeks_ago,
            self.one_week,  # calendar
            self.two_weeks,  # calendar_dates
            self.two_weeks_ago,
            self.two_months,
        )  # feed_info
        self.MakeLoaderAndLoad(self.problems)
        self.accumulator.AssertNoMoreExceptions()

    def testFutureServiceCausedByServicePeriodDateException(self):
        # Test with no validity dates specified in feed_info.txt
        self.prepareArchiveContents(
            self.two_weeks,
            self.two_months,  # calendar
            self.one_week,  # calendar_dates
            "",
            "",
        )  # feed_info
        self.MakeLoaderAndLoad(self.problems)
        e = self.accumulator.PopException("FutureService")
        self.assertTrue("calendar_dates.txt" in e.start_date_origin_file)
        self.accumulator.AssertNoMoreExceptions()
        # Test with good validity dates specified in feed_info.txt
        self.prepareArchiveContents(
            self.two_weeks,
            self.two_months,  # calendar
            self.one_week,  # calendar_dates
            self.two_weeks_ago,
            self.two_months,
        )  # feed_info
        self.MakeLoaderAndLoad(self.problems)
        self.accumulator.AssertNoMoreExceptions()

    def testExpirationDateCausedByFeedInfo(self):
        self.prepareArchiveContents(
            self.two_weeks_ago,
            self.two_months,  # calendar
            self.one_week,  # calendar_dates
            "",
            self.two_weeks,
        )  # feed_info
        self.MakeLoaderAndLoad(self.problems)
        e = self.accumulator.PopException("ExpirationDate")
        self.assertTrue("feed_info.txt" in e.expiration_origin_file)
        self.accumulator.AssertNoMoreExceptions()

    def testFutureServiceCausedByFeedInfo(self):
        self.prepareArchiveContents(
            self.two_weeks_ago,
            self.two_months,  # calendar
            self.one_week_ago,  # calendar_dates
            self.one_week,
            self.two_months,
        )  # feed_info
        self.MakeLoaderAndLoad(self.problems)
        e = self.accumulator.PopException("FutureService")
        self.assertTrue("feed_info.txt" in e.start_date_origin_file)
        self.accumulator.AssertNoMoreExceptions()


class DuplicateStopValidationTestCase(util.ValidationTestCase):
    def runTest(self):
        schedule = transitfeed.Schedule(problem_reporter=self.problems)
        schedule.AddAgency(
            "Sample Agency", "http://example.com", "America/Los_Angeles"
        )
        route = transitfeed.Route()
        route.route_id = "SAMPLE_ID"
        route.route_type = 3
        route.route_long_name = "Sample Route"
        schedule.AddRouteObject(route)

        service_period = transitfeed.ServicePeriod("WEEK")
        service_period.SetStartDate("20070101")
        service_period.SetEndDate("20071231")
        service_period.SetWeekdayService(True)
        schedule.AddServicePeriodObject(service_period)

        trip = transitfeed.Trip()
        trip.route_id = "SAMPLE_ID"
        trip.service_id = "WEEK"
        trip.trip_id = "SAMPLE_TRIP"
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
        schedule.Validate()

        stop3 = transitfeed.Stop()
        stop3.stop_id = "STOP3"
        stop3.stop_name = "Stop 3"
        stop3.stop_lat = 78.243587
        stop3.stop_lon = 32.268937
        schedule.AddStopObject(stop3)
        trip.AddStopTime(
            stop3, arrival_time="12:10:00", departure_time="12:10:00"
        )
        schedule.Validate()
        self.accumulator.AssertNoMoreExceptions()

        stop4 = transitfeed.Stop()
        stop4.stop_id = "STOP4"
        stop4.stop_name = "Stop 4"
        stop4.stop_lat = 78.243588
        stop4.stop_lon = 32.268936
        schedule.AddStopObject(stop4)
        trip.AddStopTime(
            stop4, arrival_time="12:15:00", departure_time="12:15:00"
        )
        schedule.Validate()
        e = self.accumulator.PopException("StopsTooClose")
        self.accumulator.AssertNoMoreExceptions()


class DuplicateTripIDValidationTestCase(util.TestCase):
    def runTest(self):
        schedule = transitfeed.Schedule(
            problem_reporter=util.ExceptionProblemReporterNoExpiration()
        )
        schedule.AddAgency(
            "Sample Agency", "http://example.com", "America/Los_Angeles"
        )
        route = transitfeed.Route()
        route.route_id = "SAMPLE_ID"
        route.route_type = 3
        route.route_long_name = "Sample Route"
        schedule.AddRouteObject(route)

        service_period = transitfeed.ServicePeriod("WEEK")
        service_period.SetStartDate("20070101")
        service_period.SetEndDate("20071231")
        service_period.SetWeekdayService(True)
        schedule.AddServicePeriodObject(service_period)

        trip1 = transitfeed.Trip()
        trip1.route_id = "SAMPLE_ID"
        trip1.service_id = "WEEK"
        trip1.trip_id = "SAMPLE_TRIP"
        schedule.AddTripObject(trip1)

        trip2 = transitfeed.Trip()
        trip2.route_id = "SAMPLE_ID"
        trip2.service_id = "WEEK"
        trip2.trip_id = "SAMPLE_TRIP"
        try:
            schedule.AddTripObject(trip2)
            self.fail("Expected Duplicate ID validation failure")
        except transitfeed.DuplicateID as e:
            self.assertEqual("trip_id", e.column_name)
            self.assertEqual("SAMPLE_TRIP", e.value)


class AgencyIDValidationTestCase(util.TestCase):
    def runTest(self):
        schedule = transitfeed.Schedule(
            problem_reporter=util.ExceptionProblemReporterNoExpiration()
        )
        route = transitfeed.Route()
        route.route_id = "SAMPLE_ID"
        route.route_type = 3
        route.route_long_name = "Sample Route"
        # no agency defined yet, failure.
        try:
            schedule.AddRouteObject(route)
            self.fail("Expected validation error")
        except transitfeed.InvalidValue as e:
            self.assertEqual("agency_id", e.column_name)
            self.assertEqual(None, e.value)

        # one agency defined, assume that the route belongs to it
        schedule.AddAgency(
            "Test Agency",
            "http://example.com",
            "America/Los_Angeles",
            "TEST_AGENCY",
        )
        schedule.AddRouteObject(route)

        schedule.AddAgency(
            "Test Agency 2",
            "http://example.com",
            "America/Los_Angeles",
            "TEST_AGENCY_2",
        )
        route = transitfeed.Route()
        route.route_id = "SAMPLE_ID_2"
        route.route_type = 3
        route.route_long_name = "Sample Route 2"
        # multiple agencies defined, don't know what omitted agency_id should be
        try:
            schedule.AddRouteObject(route)
            self.fail("Expected validation error")
        except transitfeed.InvalidValue as e:
            self.assertEqual("agency_id", e.column_name)
            self.assertEqual(None, e.value)

        # agency with no agency_id defined, matches route with no agency id
        schedule.AddAgency(
            "Test Agency 3", "http://example.com", "America/Los_Angeles"
        )
        schedule.AddRouteObject(route)


class DefaultAgencyTestCase(util.TestCase):
    def freeAgency(self, ex=""):
        agency = transitfeed.Agency()
        agency.agency_id = "agencytestid" + ex
        agency.agency_name = "Foo Bus Line" + ex
        agency.agency_url = "http://gofoo.com/" + ex
        agency.agency_timezone = "America/Los_Angeles"
        return agency

    def test_SetDefault(self):
        schedule = transitfeed.Schedule()
        agency = self.freeAgency()
        schedule.SetDefaultAgency(agency)
        self.assertEqual(agency, schedule.GetDefaultAgency())

    def test_NewDefaultAgency(self):
        schedule = transitfeed.Schedule()
        agency1 = schedule.NewDefaultAgency()
        self.assertTrue(agency1.agency_id)
        self.assertEqual(
            agency1.agency_id, schedule.GetDefaultAgency().agency_id
        )
        self.assertEqual(1, len(schedule.GetAgencyList()))
        agency2 = schedule.NewDefaultAgency()
        self.assertTrue(agency2.agency_id)
        self.assertEqual(
            agency2.agency_id, schedule.GetDefaultAgency().agency_id
        )
        self.assertEqual(2, len(schedule.GetAgencyList()))
        self.assertNotEqual(agency1, agency2)
        self.assertNotEqual(agency1.agency_id, agency2.agency_id)

        agency3 = schedule.NewDefaultAgency(
            agency_id="agency3",
            agency_name="Agency 3",
            agency_url="http://goagency",
        )
        self.assertEqual(agency3.agency_id, "agency3")
        self.assertEqual(agency3.agency_name, "Agency 3")
        self.assertEqual(agency3.agency_url, "http://goagency")
        self.assertEqual(agency3, schedule.GetDefaultAgency())
        self.assertEqual("agency3", schedule.GetDefaultAgency().agency_id)
        self.assertEqual(3, len(schedule.GetAgencyList()))

    def test_NoAgencyMakeNewDefault(self):
        schedule = transitfeed.Schedule()
        agency = schedule.GetDefaultAgency()
        self.assertTrue(isinstance(agency, transitfeed.Agency))
        self.assertTrue(agency.agency_id)
        self.assertEqual(1, len(schedule.GetAgencyList()))
        self.assertEqual(agency, schedule.GetAgencyList()[0])
        self.assertEqual(
            agency.agency_id, schedule.GetAgencyList()[0].agency_id
        )

    def test_AssumeSingleAgencyIsDefault(self):
        schedule = transitfeed.Schedule()
        agency1 = self.freeAgency()
        schedule.AddAgencyObject(agency1)
        agency2 = self.freeAgency("2")  # don't add to schedule
        # agency1 is default because it is the only Agency in schedule
        self.assertEqual(agency1, schedule.GetDefaultAgency())

    def test_MultipleAgencyCausesNoDefault(self):
        schedule = transitfeed.Schedule()
        agency1 = self.freeAgency()
        schedule.AddAgencyObject(agency1)
        agency2 = self.freeAgency("2")
        schedule.AddAgencyObject(agency2)
        self.assertEqual(None, schedule.GetDefaultAgency())

    def test_OverwriteExistingAgency(self):
        schedule = transitfeed.Schedule()
        agency1 = self.freeAgency()
        agency1.agency_id = "1"
        schedule.AddAgencyObject(agency1)
        agency2 = schedule.NewDefaultAgency()
        # Make sure agency1 was not overwritten by the new default
        self.assertEqual(agency1, schedule.GetAgency(agency1.agency_id))
        self.assertNotEqual("1", agency2.agency_id)


class ServiceGapsTestCase(util.MemoryZipTestCase):
    def setUp(self):
        super(ServiceGapsTestCase, self).setUp()
        self.SetArchiveContents(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,"
            "saturday,sunday,start_date,end_date\n"
            "FULLW,1,1,1,1,1,1,1,20090601,20090610\n"
            "WE,0,0,0,0,0,1,1,20090718,20101231\n",
        )
        self.SetArchiveContents(
            "calendar_dates.txt",
            "service_id,date,exception_type\n"
            "WE,20090815,2\n"
            "WE,20090816,2\n"
            "WE,20090822,2\n"
            # The following two lines are a 12-day service gap.
            # Shouldn't issue a warning
            "WE,20090829,2\n"
            "WE,20090830,2\n"
            "WE,20100102,2\n"
            "WE,20100103,2\n"
            "WE,20100109,2\n"
            "WE,20100110,2\n"
            "WE,20100612,2\n"
            "WE,20100613,2\n"
            "WE,20100619,2\n"
            "WE,20100620,2\n",
        )
        self.SetArchiveContents(
            "trips.txt",
            "route_id,service_id,trip_id\n" "AB,WE,AB1\n" "AB,FULLW,AB2\n",
        )
        self.SetArchiveContents(
            "stop_times.txt",
            "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
            "AB1,10:00:00,10:00:00,BEATTY_AIRPORT,1\n"
            "AB1,10:20:00,10:20:00,BULLFROG,2\n"
            "AB2,10:25:00,10:25:00,STAGECOACH,1\n"
            "AB2,10:55:00,10:55:00,BULLFROG,2\n",
        )
        self.schedule = self.MakeLoaderAndLoad(extra_validation=False)

    # If there is a service gap starting before today, and today has no service,
    # it should be found - even if tomorrow there is service
    def testServiceGapBeforeTodayIsDiscovered(self):
        self.schedule.Validate(
            today=date(2009, 7, 17), service_gap_interval=13
        )
        exception = self.accumulator.PopException("TooManyDaysWithoutService")
        self.assertEqual(date(2009, 7, 5), exception.first_day_without_service)
        self.assertEqual(date(2009, 7, 17), exception.last_day_without_service)

        self.AssertCommonExceptions(date(2010, 6, 25))

    # If today has service past service gaps should not appear
    def testNoServiceGapBeforeTodayIfTodayHasService(self):
        self.schedule.Validate(
            today=date(2009, 7, 18), service_gap_interval=13
        )

        self.AssertCommonExceptions(date(2010, 6, 25))

    # If the feed starts today NO previous service gap should be found
    # even if today does not have service
    def testNoServiceGapBeforeTodayIfTheFeedStartsToday(self):
        self.schedule.Validate(today=date(2009, 6, 1), service_gap_interval=13)

        # This service gap is the one between FULLW and WE
        exception = self.accumulator.PopException("TooManyDaysWithoutService")
        self.assertEqual(
            date(2009, 6, 11), exception.first_day_without_service
        )
        self.assertEqual(date(2009, 7, 17), exception.last_day_without_service)
        # The one-year period ends before the June 2010 gap, so that last
        # service gap should _not_ be found
        self.AssertCommonExceptions(None)

    # If there is a gap at the end of the one-year period we should find it
    def testGapAtTheEndOfTheOneYearPeriodIsDiscovered(self):
        self.schedule.Validate(
            today=date(2009, 6, 22), service_gap_interval=13
        )

        # This service gap is the one between FULLW and WE
        exception = self.accumulator.PopException("TooManyDaysWithoutService")
        self.assertEqual(
            date(2009, 6, 11), exception.first_day_without_service
        )
        self.assertEqual(date(2009, 7, 17), exception.last_day_without_service)

        self.AssertCommonExceptions(date(2010, 6, 21))

    # If we are right in the middle of a big service gap it should be
    # report as starting on "today - 12 days" and lasting until
    # service resumes
    def testCurrentServiceGapIsDiscovered(self):
        self.schedule.Validate(
            today=date(2009, 6, 30), service_gap_interval=13
        )
        exception = self.accumulator.PopException("TooManyDaysWithoutService")
        self.assertEqual(
            date(2009, 6, 18), exception.first_day_without_service
        )
        self.assertEqual(date(2009, 7, 17), exception.last_day_without_service)

        self.AssertCommonExceptions(date(2010, 6, 25))

    # Asserts the service gaps that appear towards the end of the calendar
    # and which are common to all the tests
    def AssertCommonExceptions(self, last_exception_date):
        exception = self.accumulator.PopException("TooManyDaysWithoutService")
        self.assertEqual(
            date(2009, 8, 10), exception.first_day_without_service
        )
        self.assertEqual(date(2009, 8, 22), exception.last_day_without_service)

        exception = self.accumulator.PopException("TooManyDaysWithoutService")
        self.assertEqual(
            date(2009, 12, 28), exception.first_day_without_service
        )
        self.assertEqual(date(2010, 1, 15), exception.last_day_without_service)

        if last_exception_date is not None:
            exception = self.accumulator.PopException(
                "TooManyDaysWithoutService"
            )
            self.assertEqual(
                date(2010, 6, 7), exception.first_day_without_service
            )
            self.assertEqual(
                last_exception_date, exception.last_day_without_service
            )

        self.accumulator.AssertNoMoreExceptions()
