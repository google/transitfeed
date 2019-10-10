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

# Unit tests for the serviceperiod module.


import datetime
import time
from datetime import date

import transitfeed
from tests import util


class ServicePeriodValidationTestCase(util.ValidationTestCase):
    def runTest(self):
        # success case
        period = transitfeed.ServicePeriod()
        repr(period)  # shouldn't crash
        period.service_id = "WEEKDAY"
        period.start_date = "20070101"
        period.end_date = "20071231"
        period.day_of_week[0] = True
        repr(period)  # shouldn't crash
        period.Validate(self.problems)

        # missing start_date. If one of start_date or end_date is None then
        # ServicePeriod.Validate assumes the required column is missing and already
        # generated an error. Instead set it to an empty string, such as when the
        # csv cell is empty. See also comment in ServicePeriod.Validate.
        period.start_date = ""
        self.ValidateAndExpectMissingValue(period, "start_date")
        period.start_date = "20070101"

        # missing end_date
        period.end_date = ""
        self.ValidateAndExpectMissingValue(period, "end_date")
        period.end_date = "20071231"

        # invalid start_date
        period.start_date = "2007-01-01"
        self.ValidateAndExpectInvalidValue(period, "start_date")
        period.start_date = "20070101"

        # impossible start_date
        period.start_date = "20070229"
        self.ValidateAndExpectInvalidValue(period, "start_date")
        period.start_date = "20070101"

        # invalid end_date
        period.end_date = "2007/12/31"
        self.ValidateAndExpectInvalidValue(period, "end_date")
        period.end_date = "20071231"

        # start & end dates out of order
        period.end_date = "20060101"
        self.ValidateAndExpectInvalidValue(period, "end_date")
        period.end_date = "20071231"

        # no service in period
        period.day_of_week[0] = False
        self.ValidateAndExpectOtherProblem(period)
        period.day_of_week[0] = True

        # invalid exception date
        period.SetDateHasService("2007", False)
        self.ValidateAndExpectInvalidValue(period, "date", "2007")
        period.ResetDateToNormalService("2007")

        period2 = transitfeed.ServicePeriod(
            field_list=[
                "serviceid1",
                "20060101",
                "20071231",
                "1",
                "0",
                "h",
                "1",
                "1",
                "1",
                "1",
            ]
        )
        self.ValidateAndExpectInvalidValue(period2, "wednesday", "h")
        repr(period)  # shouldn't crash

    def testHasExceptions(self):
        # A new ServicePeriod object has no exceptions
        period = transitfeed.ServicePeriod()
        self.assertFalse(period.HasExceptions())

        # Only regular service, no exceptions
        period.service_id = "WEEKDAY"
        period.start_date = "20070101"
        period.end_date = "20071231"
        period.day_of_week[0] = True
        self.assertFalse(period.HasExceptions())

        # Regular service + removed service exception
        period.SetDateHasService("20070101", False)
        self.assertTrue(period.HasExceptions())

        # Regular service + added service exception
        period.SetDateHasService("20070101", True)
        self.assertTrue(period.HasExceptions())

        # Only added service exception
        period = transitfeed.ServicePeriod()
        period.SetDateHasService("20070101", True)
        self.assertTrue(period.HasExceptions())

        # Only removed service exception
        period = transitfeed.ServicePeriod()
        period.SetDateHasService("20070101", False)
        self.assertTrue(period.HasExceptions())

    def testServicePeriodDateOutsideValidRange(self):
        # regular service, no exceptions, start_date invalid
        period = transitfeed.ServicePeriod()
        period.service_id = "WEEKDAY"
        period.start_date = "20070101"
        period.end_date = "21071231"
        period.day_of_week[0] = True
        self.ValidateAndExpectDateOutsideValidRange(
            period, "end_date", "21071231"
        )

        # regular service, no exceptions, start_date invalid
        period2 = transitfeed.ServicePeriod()
        period2.service_id = "SUNDAY"
        period2.start_date = "18990101"
        period2.end_date = "19991231"
        period2.day_of_week[6] = True
        self.ValidateAndExpectDateOutsideValidRange(
            period2, "start_date", "18990101"
        )

        # regular service, no exceptions, both start_date and end_date invalid
        period3 = transitfeed.ServicePeriod()
        period3.service_id = "SATURDAY"
        period3.start_date = "18990101"
        period3.end_date = "29991231"
        period3.day_of_week[5] = True
        period3.Validate(self.problems)
        e = self.accumulator.PopDateOutsideValidRange("start_date")
        self.assertEqual("18990101", e.value)
        e.FormatProblem()  # should not throw any exceptions
        e.FormatContext()  # should not throw any exceptions
        e = self.accumulator.PopDateOutsideValidRange("end_date")
        self.assertEqual("29991231", e.value)
        e.FormatProblem()  # should not throw any exceptions
        e.FormatContext()  # should not throw any exceptions
        self.accumulator.AssertNoMoreExceptions()

    def testServicePeriodExceptionDateOutsideValidRange(self):
        """ date exceptions of ServicePeriod must be in [1900,2100] """
        # regular service, 3 exceptions, date of 1st and 3rd invalid
        period = transitfeed.ServicePeriod()
        period.service_id = "WEEKDAY"
        period.start_date = "20070101"
        period.end_date = "20071231"
        period.day_of_week[0] = True
        period.SetDateHasService(
            "21070101", False
        )  # removed service exception
        period.SetDateHasService(
            "20070205", False
        )  # removed service exception
        period.SetDateHasService("10070102", True)  # added service exception
        period.Validate(self.problems)

        # check for error from first date exception
        e = self.accumulator.PopDateOutsideValidRange("date")
        self.assertEqual("10070102", e.value)
        e.FormatProblem()  # should not throw any exceptions
        e.FormatContext()  # should not throw any exceptions

        # check for error from third date exception
        e = self.accumulator.PopDateOutsideValidRange("date")
        self.assertEqual("21070101", e.value)
        e.FormatProblem()  # should not throw any exceptions
        e.FormatContext()  # should not throw any exceptions
        self.accumulator.AssertNoMoreExceptions()


class ServicePeriodDateRangeTestCase(util.ValidationTestCase):
    def runTest(self):
        period = transitfeed.ServicePeriod()
        period.service_id = "WEEKDAY"
        period.start_date = "20070101"
        period.end_date = "20071231"
        period.SetWeekdayService(True)
        period.SetDateHasService("20071231", False)
        period.Validate(self.problems)
        self.assertEqual(("20070101", "20071231"), period.GetDateRange())

        period2 = transitfeed.ServicePeriod()
        period2.service_id = "HOLIDAY"
        period2.SetDateHasService("20071225", True)
        period2.SetDateHasService("20080101", True)
        period2.SetDateHasService("20080102", False)
        period2.Validate(self.problems)
        self.assertEqual(("20071225", "20080101"), period2.GetDateRange())

        period2.start_date = "20071201"
        period2.end_date = "20071225"
        period2.Validate(self.problems)
        self.assertEqual(("20071201", "20080101"), period2.GetDateRange())

        period3 = transitfeed.ServicePeriod()
        self.assertEqual((None, None), period3.GetDateRange())

        period4 = transitfeed.ServicePeriod()
        period4.service_id = "halloween"
        period4.SetDateHasService("20051031", True)
        self.assertEqual(("20051031", "20051031"), period4.GetDateRange())
        period4.Validate(self.problems)

        schedule = transitfeed.Schedule(problem_reporter=self.problems)
        self.assertEqual((None, None), schedule.GetDateRange())
        schedule.AddServicePeriodObject(period)
        self.assertEqual(("20070101", "20071231"), schedule.GetDateRange())
        schedule.AddServicePeriodObject(period2)
        self.assertEqual(("20070101", "20080101"), schedule.GetDateRange())
        schedule.AddServicePeriodObject(period4)
        self.assertEqual(("20051031", "20080101"), schedule.GetDateRange())
        self.accumulator.AssertNoMoreExceptions()


class ServicePeriodTestCase(util.TestCase):
    def testActive(self):
        """Test IsActiveOn and ActiveDates"""
        period = transitfeed.ServicePeriod()
        period.service_id = "WEEKDAY"
        period.start_date = "20071226"
        period.end_date = "20071231"
        period.SetWeekdayService(True)
        period.SetDateHasService("20071230", True)
        period.SetDateHasService("20071231", False)
        period.SetDateHasService("20080102", True)
        #      December  2007
        #  Su Mo Tu We Th Fr Sa
        #  23 24 25 26 27 28 29
        #  30 31

        # Some tests have named arguments and others do not to ensure that any
        # (possibly unwanted) changes to the API get caught

        # calendar_date exceptions near start date
        self.assertFalse(period.IsActiveOn(date="20071225"))
        self.assertFalse(
            period.IsActiveOn(date="20071225", date_object=date(2007, 12, 25))
        )
        self.assertTrue(period.IsActiveOn(date="20071226"))
        self.assertTrue(
            period.IsActiveOn(date="20071226", date_object=date(2007, 12, 26))
        )

        # calendar_date exceptions near end date
        self.assertTrue(period.IsActiveOn("20071230"))
        self.assertTrue(period.IsActiveOn("20071230", date(2007, 12, 30)))
        self.assertFalse(period.IsActiveOn("20071231"))
        self.assertFalse(period.IsActiveOn("20071231", date(2007, 12, 31)))

        # date just outside range, both weekday and an exception
        self.assertFalse(period.IsActiveOn("20080101"))
        self.assertFalse(period.IsActiveOn("20080101", date(2008, 1, 1)))
        self.assertTrue(period.IsActiveOn("20080102"))
        self.assertTrue(period.IsActiveOn("20080102", date(2008, 1, 2)))

        self.assertEqual(
            period.ActiveDates(),
            ["20071226", "20071227", "20071228", "20071230", "20080102"],
        )

        # Test of period without start_date, end_date
        period_dates = transitfeed.ServicePeriod()
        period_dates.SetDateHasService("20071230", True)
        period_dates.SetDateHasService("20071231", False)

        self.assertFalse(period_dates.IsActiveOn(date="20071229"))
        self.assertFalse(
            period_dates.IsActiveOn(
                date="20071229", date_object=date(2007, 12, 29)
            )
        )
        self.assertTrue(period_dates.IsActiveOn("20071230"))
        self.assertTrue(
            period_dates.IsActiveOn("20071230", date(2007, 12, 30))
        )
        self.assertFalse(period_dates.IsActiveOn("20071231"))
        self.assertFalse(
            period_dates.IsActiveOn("20071231", date(2007, 12, 31))
        )
        self.assertEqual(period_dates.ActiveDates(), ["20071230"])

        # Test with an invalid ServicePeriod; one of start_date, end_date is set
        period_no_end = transitfeed.ServicePeriod()
        period_no_end.start_date = "20071226"
        self.assertFalse(period_no_end.IsActiveOn(date="20071231"))
        self.assertFalse(
            period_no_end.IsActiveOn(
                date="20071231", date_object=date(2007, 12, 31)
            )
        )
        self.assertEqual(period_no_end.ActiveDates(), [])
        period_no_start = transitfeed.ServicePeriod()
        period_no_start.end_date = "20071230"
        self.assertFalse(period_no_start.IsActiveOn("20071229"))
        self.assertFalse(
            period_no_start.IsActiveOn("20071229", date(2007, 12, 29))
        )
        self.assertEqual(period_no_start.ActiveDates(), [])

        period_empty = transitfeed.ServicePeriod()
        self.assertFalse(period_empty.IsActiveOn("20071231"))
        self.assertFalse(
            period_empty.IsActiveOn("20071231", date(2007, 12, 31))
        )
        self.assertEqual(period_empty.ActiveDates(), [])


class OnlyCalendarDatesTestCase(util.LoadTestCase):
    def runTest(self):
        self.Load("only_calendar_dates"),
        self.accumulator.AssertNoMoreExceptions()


class DuplicateServiceIdDateWarningTestCase(util.MemoryZipTestCase):
    def runTest(self):
        # Two lines with the same value of service_id and date.
        # Test for the warning.
        self.SetArchiveContents(
            "calendar_dates.txt",
            "service_id,date,exception_type\n"
            "FULLW,20100604,1\n"
            "FULLW,20100604,2\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("DuplicateID")
        self.assertEqual("(service_id, date)", e.column_name)
        self.assertEqual("(FULLW, 20100604)", e.value)


class ExpirationDateTestCase(util.TestCase):
    def runTest(self):
        accumulator = util.RecordingProblemAccumulator(
            self, ("NoServiceExceptions")
        )
        problems = transitfeed.ProblemReporter(accumulator)
        schedule = transitfeed.Schedule(problem_reporter=problems)

        now = time.mktime(time.localtime())
        seconds_per_day = 60 * 60 * 24
        two_weeks_ago = time.localtime(now - 14 * seconds_per_day)
        two_weeks_from_now = time.localtime(now + 14 * seconds_per_day)
        two_months_from_now = time.localtime(now + 60 * seconds_per_day)
        date_format = "%Y%m%d"

        service_period = schedule.GetDefaultServicePeriod()
        service_period.SetWeekdayService(True)
        service_period.SetStartDate("20070101")

        service_period.SetEndDate(
            time.strftime(date_format, two_months_from_now)
        )
        schedule.Validate()  # should have no problems
        accumulator.AssertNoMoreExceptions()

        service_period.SetEndDate(
            time.strftime(date_format, two_weeks_from_now)
        )
        schedule.Validate()
        e = accumulator.PopException("ExpirationDate")
        self.assertTrue(e.FormatProblem().index("will soon expire"))
        accumulator.AssertNoMoreExceptions()

        service_period.SetEndDate(time.strftime(date_format, two_weeks_ago))
        schedule.Validate()
        e = accumulator.PopException("ExpirationDate")
        self.assertTrue(e.FormatProblem().index("expired"))
        accumulator.AssertNoMoreExceptions()


class FutureServiceStartDateTestCase(util.TestCase):
    def runTest(self):
        accumulator = util.RecordingProblemAccumulator(self)
        problems = transitfeed.ProblemReporter(accumulator)
        schedule = transitfeed.Schedule(problem_reporter=problems)

        today = datetime.date.today()
        yesterday = today - datetime.timedelta(days=1)
        tomorrow = today + datetime.timedelta(days=1)
        two_months_from_today = today + datetime.timedelta(days=60)

        service_period = schedule.GetDefaultServicePeriod()
        service_period.SetWeekdayService(True)
        service_period.SetWeekendService(True)
        service_period.SetEndDate(two_months_from_today.strftime("%Y%m%d"))

        service_period.SetStartDate(yesterday.strftime("%Y%m%d"))
        schedule.Validate()
        accumulator.AssertNoMoreExceptions()

        service_period.SetStartDate(today.strftime("%Y%m%d"))
        schedule.Validate()
        accumulator.AssertNoMoreExceptions()

        service_period.SetStartDate(tomorrow.strftime("%Y%m%d"))
        schedule.Validate()
        accumulator.PopException("FutureService")
        accumulator.AssertNoMoreExceptions()


class CalendarTxtIntegrationTestCase(util.MemoryZipTestCase):
    def testBadEndDateFormat(self):
        # A badly formatted end_date used to generate an InvalidValue report from
        # Schedule.Validate and ServicePeriod.Validate. Test for the bug.
        self.SetArchiveContents(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\n"
            "FULLW,1,1,1,1,1,1,1,20070101,20101232\n"
            "WE,0,0,0,0,0,1,1,20070101,20101231\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopInvalidValue("end_date")
        self.accumulator.AssertNoMoreExceptions()

    def testBadStartDateFormat(self):
        self.SetArchiveContents(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\n"
            "FULLW,1,1,1,1,1,1,1,200701xx,20101231\n"
            "WE,0,0,0,0,0,1,1,20070101,20101231\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopInvalidValue("start_date")
        self.accumulator.AssertNoMoreExceptions()

    def testNoStartDateAndEndDate(self):
        """Regression test for calendar.txt with empty start_date and end_date.

        See https://github.com/google/transitfeed/issues/41
        """
        self.SetArchiveContents(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\n"
            "FULLW,1,1,1,1,1,1,1,    ,\t\n"
            "WE,0,0,0,0,0,1,1,20070101,20101231\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("MissingValue")
        self.assertEqual(2, e.row_num)
        self.assertEqual("start_date", e.column_name)
        e = self.accumulator.PopException("MissingValue")
        self.assertEqual(2, e.row_num)
        self.assertEqual("end_date", e.column_name)
        self.accumulator.AssertNoMoreExceptions()

    def testNoStartDateAndBadEndDate(self):
        self.SetArchiveContents(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\n"
            "FULLW,1,1,1,1,1,1,1,,abc\n"
            "WE,0,0,0,0,0,1,1,20070101,20101231\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("MissingValue")
        self.assertEqual(2, e.row_num)
        self.assertEqual("start_date", e.column_name)
        e = self.accumulator.PopInvalidValue("end_date")
        self.assertEqual(2, e.row_num)
        self.accumulator.AssertNoMoreExceptions()

    def testMissingEndDateColumn(self):
        self.SetArchiveContents(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date\n"
            "FULLW,1,1,1,1,1,1,1,20070101\n"
            "WE,0,0,0,0,0,1,1,20070101\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("MissingColumn")
        self.assertEqual("end_date", e.column_name)
        self.accumulator.AssertNoMoreExceptions()

    def testDateOutsideValidRange(self):
        """ start_date and end_date values must be in [1900,2100] """
        self.SetArchiveContents(
            "calendar.txt",
            "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
            "start_date,end_date\n"
            "FULLW,1,1,1,1,1,1,1,20070101,21101231\n"
            "WE,0,0,0,0,0,1,1,18990101,20101231\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopDateOutsideValidRange(
            "end_date", "calendar.txt"
        )
        self.assertEqual("21101231", e.value)
        e = self.accumulator.PopDateOutsideValidRange(
            "start_date", "calendar.txt"
        )
        self.assertEqual("18990101", e.value)
        self.accumulator.AssertNoMoreExceptions()


class CalendarDatesTxtIntegrationTestCase(util.MemoryZipTestCase):
    def testDateOutsideValidRange(self):
        """ exception date values in must be in [1900,2100] """
        self.SetArchiveContents(
            "calendar_dates.txt",
            "service_id,date,exception_type\n" "WE,18990815,2\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopDateOutsideValidRange(
            "date", "calendar_dates.txt"
        )
        self.assertEqual("18990815", e.value)
        self.accumulator.AssertNoMoreExceptions()


class DefaultServicePeriodTestCase(util.TestCase):
    def test_SetDefault(self):
        schedule = transitfeed.Schedule()
        service1 = transitfeed.ServicePeriod()
        service1.SetDateHasService("20070101", True)
        service1.service_id = "SERVICE1"
        schedule.SetDefaultServicePeriod(service1)
        self.assertEqual(service1, schedule.GetDefaultServicePeriod())
        self.assertEqual(
            service1, schedule.GetServicePeriod(service1.service_id)
        )

    def test_NewDefault(self):
        schedule = transitfeed.Schedule()
        service1 = schedule.NewDefaultServicePeriod()
        self.assertTrue(service1.service_id)
        schedule.GetServicePeriod(service1.service_id)
        service1.SetDateHasService("20070101", True)  # Make service1 different
        service2 = schedule.NewDefaultServicePeriod()
        schedule.GetServicePeriod(service2.service_id)
        self.assertTrue(service1.service_id)
        self.assertTrue(service2.service_id)
        self.assertNotEqual(service1, service2)
        self.assertNotEqual(service1.service_id, service2.service_id)

    def test_NoServicesMakesNewDefault(self):
        schedule = transitfeed.Schedule()
        service1 = schedule.GetDefaultServicePeriod()
        self.assertEqual(
            service1, schedule.GetServicePeriod(service1.service_id)
        )

    def test_AssumeSingleServiceIsDefault(self):
        schedule = transitfeed.Schedule()
        service1 = transitfeed.ServicePeriod()
        service1.SetDateHasService("20070101", True)
        service1.service_id = "SERVICE1"
        schedule.AddServicePeriodObject(service1)
        self.assertEqual(service1, schedule.GetDefaultServicePeriod())
        self.assertEqual(
            service1.service_id, schedule.GetDefaultServicePeriod().service_id
        )

    def test_MultipleServicesCausesNoDefault(self):
        schedule = transitfeed.Schedule()
        service1 = transitfeed.ServicePeriod()
        service1.service_id = "SERVICE1"
        service1.SetDateHasService("20070101", True)
        schedule.AddServicePeriodObject(service1)
        service2 = transitfeed.ServicePeriod()
        service2.service_id = "SERVICE2"
        service2.SetDateHasService("20070201", True)
        schedule.AddServicePeriodObject(service2)
        service_d = schedule.GetDefaultServicePeriod()
        self.assertEqual(service_d, None)
