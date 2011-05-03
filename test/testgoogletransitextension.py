#!/usr/bin/python2.5

# Copyright (C) 2011 Google Inc.
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

# Unit tests for the googletransit extension (extensions.googletransit)

import extensions.googletransit

import os
import re
import time
import transitfeed
import transitfeed.problems as problems_module
from util import MemoryZipTestCase
from util import RecordingProblemAccumulator
from testfeedvalidator import FullTests
from testtransitfeed import ValidationTestCase


class ExtensionFullTests(FullTests):
  """Inherits FullTests from testfeedvalidator.py to test the extension
  executable feedvalidator_googletransit.py. Tests the extension executable with
  new good_feed test data which uses extension capabilities."""

  feedvalidator_executable = 'feedvalidator_googletransit.py'
  extension_name = 'extensions.googletransit'
  additional_arguments = ['--error_types_ignore_list',
                                      'DeprecatedColumn']

  def testGoogleTransitGoodFeed(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath(self.feedvalidator_executable), '-n', '--latest_version',
         transitfeed.__version__] + self.additional_arguments +
         [self.GetPath('test', 'data', 'googletransit', 'good_feed')])
    self.assertTrue(re.search(r'feed validated successfully', out))
    self.assertFalse(re.search(r'ERROR', out))
    htmlout = open('validation-results.html').read()
    self.assertMatchesRegex(
        self.extension_message + self.extension_name, htmlout)
    self.assertTrue(re.search(r'feed validated successfully', htmlout))
    self.assertFalse(re.search(r'ERROR', htmlout))
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))


class ExtensionMemoryZipTestCase(MemoryZipTestCase):
  """ExtendMemoryZipTestCase to also ignore DeprecatedColumn errors.

     In this extension a couple of columns are set to be 'Deprecated'.
     The 'normal' transitfeed test data used in some of the test cases here
     however still uses these columns. As we can/should not modify the 'normal'
     test data we are adding the 'DeprecatedColumn' to the _IGNORE_TYPES list.
  """

  _IGNORE_TYPES = MemoryZipTestCase._IGNORE_TYPES + ["DeprecatedColumn"]


class FareAttributeAgencyIdTestCase(ExtensionMemoryZipTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  def testNoErrorsWithOneAgencyAndNoIdAndAgencyIdColumnNotPresent(self):
    self.SetArchiveContents(
        "fare_attributes.txt",
        "fare_id,price,currency_type,payment_method,transfers\n"
        "fare1,1,EUR,1,0\n")
    self.SetArchiveContents(
        "agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
        ",Demo Agency,http://google.com,America/Los_Angeles,en\n")
    self.SetArchiveContents(
        "routes.txt",
        "route_id,agency_id,route_short_name,route_long_name,route_type\n"
        "AB,,,Airport Bullfrog,3\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.AssertNoMoreExceptions()

  def testNoErrorsWithOneAgencyAndNoIdAndAgencyIdColumnPresent(self):
    self.SetArchiveContents(
        "fare_attributes.txt",
        "fare_id,price,currency_type,payment_method,transfers,agency_id\n"
        "fare1,1,EUR,1,0,\n")
    self.SetArchiveContents(
        "agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
        ",Demo Agency,http://google.com,America/Los_Angeles,en\n")
    self.SetArchiveContents(
        "routes.txt",
        "route_id,agency_id,route_short_name,route_long_name,route_type\n"
        "AB,,,Airport Bullfrog,3\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.AssertNoMoreExceptions()

  def testNoErrorsWithSeveralAgencies(self):
    self.SetArchiveContents(
        "fare_attributes.txt",
        "fare_id,price,currency_type,payment_method,transfers,agency_id\n"
        "fare1,1,EUR,1,0,DTA\n"
        "fare2,2,EUR,0,0,ATD\n")
    self.SetArchiveContents(
        "agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
        "DTA,Demo Agency,http://google.com,America/Los_Angeles,en\n"
        "ATD,Another Demo Agency,http://example.com,America/Los_Angeles,en\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.AssertNoMoreExceptions()

  def testWrongIdWithOneAgencyWithNoId(self):
    self.SetArchiveContents(
        "fare_attributes.txt",
        "fare_id,price,currency_type,payment_method,transfers,agency_id\n"
        "fare1,1,EUR,1,0,DOESNOTEXIST\n")
    self.SetArchiveContents(
        "routes.txt",
        "route_id,agency_id,route_short_name,route_long_name,route_type\n"
        "AB,,,Airport Bullfrog,3\n")
    self.SetArchiveContents(
          "agency.txt",
          "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
          ",Demo Agency,http://google.com,America/Los_Angeles,en\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("InvalidAgencyID")
    self.assertEquals('agency_id', e.column_name)
    self.accumulator.AssertNoMoreExceptions()

  def testWrongIdWithOneAgencyWithId(self):
    self.SetArchiveContents("fare_attributes.txt",
        "fare_id,price,currency_type,payment_method,transfers,agency_id\n"
        "fare1,1,EUR,1,0,DOESNOTEXIST\n")
    self.SetArchiveContents(
        "agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
        "DTA,Demo Agency,http://google.com,America/Los_Angeles,en\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("InvalidAgencyID")
    self.assertEquals('agency_id', e.column_name)
    self.accumulator.AssertNoMoreExceptions()

  def testWrongIdWithSeveralAgencies(self):
    self.SetArchiveContents(
        "fare_attributes.txt",
        "fare_id,price,currency_type,payment_method,transfers,"
        "agency_id\n"
        "fare1,1,EUR,1,0,DTA\n"
        "fare2,2,EUR,0,1,ATD\n"
        "fare3,2,EUR,0,2,DOESNOTEXIST\n")
    self.SetArchiveContents(
        "agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
        "DTA,Demo Agency,http://google.com,America/Los_Angeles,en\n"
        "ATD,Another Demo Agency,http://example.com,America/Los_Angeles,en\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("InvalidAgencyID")
    self.assertEquals('agency_id', e.column_name)
    self.accumulator.AssertNoMoreExceptions()


class FeedInfoExtensionTestCase(ExtensionMemoryZipTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  def setUp(self):
    super(FeedInfoExtensionTestCase, self).setUp()
    # Modify agency.txt for all tests in this test case
    self.SetArchiveContents("agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
        "DTA,Demo Agency,http://google.com,America/Los_Angeles,en\n")

  def testNoErrors(self):
    self.SetArchiveContents("feed_info.txt",
        "feed_publisher_name,feed_publisher_url,feed_timezone,feed_lang\n"
        "DTA,http://google.com,America/Los_Angeles,en")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.AssertNoMoreExceptions()


  def testDifferentTimezone(self):
    self.SetArchiveContents("feed_info.txt",
        "feed_publisher_name,feed_publisher_url,feed_timezone,feed_lang\n"
        "DTA,http://google.com,UTC,en")
    self.MakeLoaderAndLoad(self.problems,  gtfs_factory=self.gtfs_factory)
    self.accumulator.PopInvalidValue("feed_timezone")
    self.accumulator.AssertNoMoreExceptions()

  def testDifferentLanguage(self):
    self.SetArchiveContents("feed_info.txt",
        "feed_publisher_name,feed_publisher_url,feed_timezone,feed_lang\n"
        "DTA,http://google.com,America/Los_Angeles,pt")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.PopInvalidValue("feed_lang")
    self.accumulator.AssertNoMoreExceptions()

  def testDifferentTimezoneAndLanguage(self):
    self.SetArchiveContents("feed_info.txt",
        "feed_publisher_name,feed_publisher_url,feed_timezone,feed_lang\n"
        "DTA,http://google.com,UTC,pt")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.PopInvalidValue("feed_timezone")
    self.accumulator.PopInvalidValue("feed_lang")
    self.accumulator.AssertNoMoreExceptions()

  def testInvalidPublisherUrl(self):
    self.SetArchiveContents("feed_info.txt",
        "feed_publisher_name,feed_publisher_url,feed_timezone,feed_lang\n"
        "DTA,htttp://google.com,America/Los_Angeles,en")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.PopInvalidValue("feed_publisher_url")
    self.accumulator.AssertNoMoreExceptions()

  def testValidityDatesNoErrors(self):
    self.SetArchiveContents("feed_info.txt",
        "feed_publisher_name,feed_publisher_url,feed_timezone,feed_lang,"
        "feed_valid_from,feed_valid_until\n"
        "DTA,http://google.com,America/Los_Angeles,en,20101201,20101231")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.AssertNoMoreExceptions()

  def testValidityDatesInvalid(self):
    self.SetArchiveContents("feed_info.txt",
        "feed_publisher_name,feed_publisher_url,feed_timezone,feed_lang,"
        "feed_valid_from,feed_valid_until\n"
        "DTA,http://google.com,America/Los_Angeles,en,10/01/12,10/31/12")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.PopInvalidValue("feed_valid_from")
    self.accumulator.PopInvalidValue("feed_valid_until")
    self.accumulator.AssertNoMoreExceptions()

  def testValidityDatesInverted(self):
    self.SetArchiveContents("feed_info.txt",
        "feed_publisher_name,feed_publisher_url,feed_timezone,feed_lang,"
        "feed_valid_from,feed_valid_until\n"
        "DTA,http://google.com,America/Los_Angeles,en,20101231,20101201")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.PopInvalidValue("feed_valid_until")
    self.accumulator.AssertNoMoreExceptions()


class ScheduleTestCase(ExtensionMemoryZipTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  def testNoErrorsWithAgenciesHavingSameTimeZone(self):
    self.SetArchiveContents("agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
        "DTA,Demo Agency,http://google.com,America/Los_Angeles,en\n"
        "DTA2,Demo Agency 2,http://google.com,America/Los_Angeles,en\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.AssertNoMoreExceptions()

  def testAgenciesWithDifferentTimeZone(self):
    self.SetArchiveContents("agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
        "DTA,Demo Agency,http://google.com,America/Los_Angeles,en\n"
        "DTA2,Demo Agency 2,http://google.com,America/New_York,en\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.PopInvalidValue("agency_timezone")
    self.accumulator.AssertNoMoreExceptions()


class ScheduleStartAndExpirationDatesTestCase(ExtensionMemoryZipTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  # Remove "ExpirationDate" from the accumulator _IGNORE_TYPES to get the
  # expiration errors.
  _IGNORE_TYPES = ExtensionMemoryZipTestCase._IGNORE_TYPES[:]
  _IGNORE_TYPES.remove("ExpirationDate")

  # Init dates to be close to now
  now = time.mktime(time.localtime())
  seconds_per_day = 60 * 60 * 24
  date_format = "%Y%m%d"
  two_weeks_ago = time.strftime(date_format,
                                time.localtime(now - 14 * seconds_per_day))
  one_week_ago = time.strftime(date_format,
                               time.localtime(now - 7 * seconds_per_day))
  one_week = time.strftime(date_format,
                            time.localtime(now + 7 * seconds_per_day))
  two_weeks = time.strftime(date_format,
                            time.localtime(now + 14 * seconds_per_day))
  two_months = time.strftime(date_format,
                             time.localtime(now + 60 * seconds_per_day))

  def prepareArchiveContents(self, calendar_start, calendar_end,
                             exception_date, feed_info_start, feed_info_end):
    self.SetArchiveContents(
        "calendar.txt",
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
        "start_date,end_date\n"
        "FULLW,1,1,1,1,1,1,1,%s,%s\n"
        "WE,0,0,0,0,0,1,1,%s,%s\n" % (calendar_start, calendar_end,
                                      calendar_start, calendar_end))
    self.SetArchiveContents(
        "calendar_dates.txt",
        "service_id,date,exception_type\n"
        "FULLW,%s,1\n" % (exception_date))
    from_column = ""
    if feed_info_start:
      from_column = ",feed_valid_from"
      feed_info_start = "," + feed_info_start
    until_column = ""
    if feed_info_end:
      until_column = ",feed_valid_until"
      feed_info_end = "," + feed_info_end
    self.SetArchiveContents("feed_info.txt",
        "feed_publisher_name,feed_publisher_url,feed_timezone,feed_lang%s%s\n"
        "DTA,http://google.com,America/Los_Angeles,en%s%s" % (
          from_column, until_column, feed_info_start, feed_info_end))

  def testNoErrors(self):
    self.prepareArchiveContents(
        self.two_weeks_ago, self.two_months, # calendar
        self.two_weeks,                      # calendar_dates
        "", "")                              # feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.AssertNoMoreExceptions()

  def testExpirationDateCausedByServicePeriod(self):
    # test with no validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks_ago, self.two_weeks, # calendar
        self.one_week,                      # calendar_dates
        "", "")                             # feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("ExpirationDate")
    self.assertTrue("calendar.txt" in e.expiration_origin_file)
    self.accumulator.AssertNoMoreExceptions()
    # test with good validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks_ago, self.two_weeks,  # calendar
        self.one_week,                       # calendar_dates
        self.two_weeks_ago, self.two_months) # feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("ExpirationDate")
    self.assertTrue("calendar.txt" in e.expiration_origin_file)
    self.accumulator.AssertNoMoreExceptions()

  def testFutureServiceCausedByServicePeriod(self):
    # test with no validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.one_week, self.two_months, # calendar
        self.two_weeks,                 # calendar_dates
        "", "")                         # feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("FutureService")
    self.assertTrue("calendar.txt" in e.start_date_origin_file)
    self.accumulator.AssertNoMoreExceptions()
    # Test with good validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.one_week, self.two_months,      # calendar
        self.two_weeks,                      # calendar_dates
        self.two_weeks_ago, self.two_months) # feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("FutureService")
    self.assertTrue("calendar.txt" in e.start_date_origin_file)
    self.accumulator.AssertNoMoreExceptions()

  def testExpirationDateCausedByServicePeriodDateException(self):
    # Test with no validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks_ago, self.one_week, # calendar
        self.two_weeks,                    # calendar_dates
        "", "")                            # feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("ExpirationDate")
    self.assertTrue("calendar_dates.txt" in e.expiration_origin_file)
    self.accumulator.AssertNoMoreExceptions()
    # Test with good validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks_ago, self.one_week,   # calendar
        self.two_weeks,                      # calendar_dates
        self.two_weeks_ago, self.two_months) # feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("ExpirationDate")
    self.assertTrue("calendar_dates.txt" in e.expiration_origin_file)
    self.accumulator.AssertNoMoreExceptions()

  def testFutureServiceCausedByServicePeriodDateException(self):
    # Test with no validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks, self.two_months, # calendar
        self.one_week,                   # calendar_dates
        "", "")                          # feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("FutureService")
    self.assertTrue("calendar_dates.txt" in e.start_date_origin_file)
    self.accumulator.AssertNoMoreExceptions()
    # Test with good validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks, self.two_months,     # calendar
        self.one_week,                       # calendar_dates
        self.two_weeks_ago, self.two_months) # feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("FutureService")
    self.assertTrue("calendar_dates.txt" in e.start_date_origin_file)
    self.accumulator.AssertNoMoreExceptions()

  def testExpirationDateCausedByFeedInfo(self):
    self.prepareArchiveContents(
        self.two_weeks_ago, self.two_months, # calendar
        self.one_week,                       # calendar_dates
        "", self.two_weeks)                  # feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("ExpirationDate")
    self.assertTrue("feed_info.txt" in e.expiration_origin_file)
    self.accumulator.AssertNoMoreExceptions()

  def testFutureServiceCausedByFeedInfo(self):
    self.prepareArchiveContents(
        self.two_weeks_ago, self.two_months, # calendar
        self.one_week_ago,                   # calendar_dates
        self.one_week, self.two_months)      # feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("FutureService")
    self.assertTrue("feed_info.txt" in e.start_date_origin_file)
    self.accumulator.AssertNoMoreExceptions()


class FrequencyExtensionTestCase(ValidationTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  def testExactTimesStringValueConversion(self):
    frequency_class = self.gtfs_factory.Frequency
    # Test that no exact_times converts to 0
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800"})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 0)
    # Test that empty exact_times converts to 0
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": ""})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 0)
    # Test that exact_times "0" converts to 0
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": "0"})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 0)
    # Test that exact_times "1" converts to 1
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": "1"})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 1)
    self.accumulator.AssertNoMoreExceptions()

  def testExactTimesAsIntValue(self):
    frequency_class = self.gtfs_factory.Frequency
    # Test that exact_times None converts to 0
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": None})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 0)
    # Test that exact_times 0 remains 0
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": 0})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 0)
    # Test that exact_times 1 remains 1
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": 1})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 1)
    self.accumulator.AssertNoMoreExceptions()

  def testExactTimesInvalidValues(self):
    frequency_class = self.gtfs_factory.Frequency
    # Test that exact_times 15 raises error
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": 15})
    frequency.ValidateBeforeAdd(self.problems)
    self.accumulator.PopInvalidValue("exact_times")
    self.accumulator.AssertNoMoreExceptions()
    # Test that exact_times "yes" raises error
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": "yes"})
    frequency.ValidateBeforeAdd(self.problems)
    self.accumulator.PopInvalidValue("exact_times")
    self.accumulator.AssertNoMoreExceptions()


class StopExtensionIntegrationTestCase(ExtensionMemoryZipTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  def testNoErrors(self):
    self.SetArchiveContents("stops.txt",
        "stop_id,stop_name,stop_lat,stop_lon,stop_timezone,location_type,"
        "parent_station,vehicle_type,wheelchair_boarding\n"
        "BEATTY,Beatty,36.868446,-116.784582,,1,,1100,\n"
        "BEATTY_AIRPORT,Airport West,36.868446,-116.784582,,2,BEATTY,,\n"
        "BULLFROG,Bullfrog,36.88108,-116.81797,,,,3,\n"
        "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,America/Los_Angeles,"
        ",,204,\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.AssertNoMoreExceptions()

  def testInvalidVehicleType(self):
    self.SetArchiveContents("stops.txt",
        "stop_id,stop_name,stop_lat,stop_lon,stop_timezone,location_type,"
        "parent_station,vehicle_type,wheelchair_boarding\n"
        "BEATTY,Beatty,36.868446,-116.784582,,1,,2557,\n"  # bad vehicle type
        "BEATTY_AIRPORT,Airport West,36.868446,-116.784582,,2,BEATTY,,\n"
        "BULLFROG,Bullfrog,36.88108,-116.81797,,,,3,\n"
        "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,America/Los_Angeles,"
        ",,204,\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.PopInvalidValue("vehicle_type")
    self.accumulator.AssertNoMoreExceptions()


class StopExtensionTestCase(ValidationTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  def setUp(self):
    super(StopExtensionTestCase, self).setUp()

    self._stop = self.gtfs_factory.Stop(
        lng=1.00, lat=48.1, name="a stop", stop_id="stop")
    self._stop._gtfs_factory = self.gtfs_factory

    self._parent_stop = self.gtfs_factory.Stop(
        lng=1.00, lat=48.2, name="parent stop", stop_id="parent_stop")
    self._parent_stop._gtfs_factory = self.gtfs_factory

    self._child_stop = self.gtfs_factory.Stop(
         lng=1.00, lat=48.2, name="child stop", stop_id="child stop")
    self._child_stop.parent_station = self._parent_stop.stop_id
    self._child_stop._gtfs_factory = self.gtfs_factory

    self._entrance = self.gtfs_factory.Stop(
         lng=1.00, lat=48.2, name="an entrance", stop_id="entrance")
    self._entrance.location_type = 2
    self._entrance.parent_station = self._parent_stop.stop_id
    self._entrance._gtfs_factory = self.gtfs_factory

  def testValidateStopTimezone(self):
    self._stop.stop_timezone = 'TestTimeZone/Wrong_Place'
    self._stop.Validate(self.problems)
    e = self.accumulator.PopInvalidValue('stop_timezone')
    self.accumulator.AssertNoMoreExceptions()

  def testValidateVehicleType(self):
    # Test with non-integer value
    self._stop.vehicle_type = 'abc'
    self._stop.Validate(self.problems)
    e = self.accumulator.PopInvalidValue('vehicle_type')
    self.accumulator.AssertNoMoreExceptions()

    # Test with not known value
    self._stop.vehicle_type = 2547
    self._stop.Validate(self.problems)
    e = self.accumulator.PopInvalidValue('vehicle_type')
    self.accumulator.AssertNoMoreExceptions()

  def testEntranceExceptions(self):
    # There should be no error validating _entrance
    self._entrance.Validate(self.problems)
    self.accumulator.AssertNoMoreExceptions()

    # An entrance must not have a stop_timezone
    self._entrance.stop_timezone = 'America/Los_Angeles'
    self._entrance.Validate(self.problems)
    e = self.accumulator.PopInvalidValue('location_type')
    self.assertMatchesRegex(r'stop_timezone', e.FormatProblem())
    self.accumulator.AssertNoMoreExceptions()
    self._entrance.stop_timezone = None

    # An entrance must not have a vehicle type
    self._entrance.vehicle_type = 200
    self._entrance.Validate(self.problems)
    e = self.accumulator.PopInvalidValue('vehicle_type')
    self.accumulator.AssertNoMoreExceptions()
    self._entrance.vehicle_type = None

    # An entrance should have a parent station
    self._entrance.parent_station = None
    self._entrance.Validate(self.problems)
    e = self.accumulator.PopInvalidValue('location_type')
    self.assertMatchesRegex(r'parent_station', e.FormatProblem())
    self.accumulator.AssertNoMoreExceptions()

  def testChildExceptions(self):
    # There should be no error validating _child_stop
    self._child_stop.Validate(self.problems)
    self.accumulator.AssertNoMoreExceptions()

    # A _child_stop must not have a stop_timezone
    self._child_stop.stop_timezone = 'America/Los_Angeles'
    self._child_stop.Validate(self.problems)
    e = self.accumulator.PopInvalidValue('location_type')
    self.assertMatchesRegex(r'stop_timezone', e.FormatProblem())
    self.assertTrue(e.IsWarning())
    self.accumulator.AssertNoMoreExceptions()
    self._child_stop.stop_timezone = None

    # Adding vehicle_type, Google transit doesn't read child stop vehicle types
    self._child_stop.vehicle_type = 200
    self._child_stop.Validate(self.problems)
    e = self.accumulator.PopInvalidValue('vehicle_type')
    self.assertTrue(e.IsWarning())
    self.accumulator.AssertNoMoreExceptions()
    self._child_stop.vehicle_type = None

  def testAllowEmptyStopNameIfEntrance(self):
    # Empty stop_name with default location_type=0 should report MissingValue
    self._stop.stop_name = ''
    self._stop.Validate(self.problems)
    e = self.accumulator.PopMissingValue('stop_name')
    self.accumulator.AssertNoMoreExceptions()

    # Empty stop_name in a child stop should report MissingValue
    self._child_stop.stop_name = ''
    self._child_stop.Validate(self.problems)
    e = self.accumulator.PopMissingValue('stop_name')
    self.accumulator.AssertNoMoreExceptions()

    # Empty stop_name with location_type=2 should report no errors
    self._entrance.stop_name = ''
    self._entrance.Validate(self.problems)
    self.accumulator.AssertNoMoreExceptions()

  def testValidateValidateWheelchairBoarding(self):
    # Test empty value defaults to 0
    self._stop.wheelchair_boarding = ''
    self._stop.Validate(self.problems)
    self.assertEquals(self._stop.wheelchair_boarding, 0)
    self.accumulator.AssertNoMoreExceptions()

    # Test with non-integer value
    self._stop.wheelchair_boarding = 'abc'
    self._stop.Validate(self.problems)
    e = self.accumulator.PopInvalidValue('wheelchair_boarding')
    self.accumulator.AssertNoMoreExceptions()

    # Test with not known value
    self._stop.wheelchair_boarding = 11
    self._stop.Validate(self.problems)
    e = self.accumulator.PopInvalidValue('wheelchair_boarding')
    self.accumulator.AssertNoMoreExceptions()


class RouteExtensionIntegrationTestCase(ExtensionMemoryZipTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  def testNoErrors(self):
    self.SetArchiveContents(
        "routes.txt",
        "route_id,agency_id,route_short_name,route_long_name,route_type,"
        "co2_per_km\n"
        "AB,DTA,,Airport Bullfrog,201,15.5\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.AssertNoMoreExceptions()

  def testInvalidCo2PerKm(self):
    self.SetArchiveContents(
        "routes.txt",
        "route_id,route_short_name,route_long_name,route_type,co2_per_km\n"
        "AB,,Airport Bullfrog,201,15.5mg\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.PopInvalidValue("co2_per_km")
    self.accumulator.AssertNoMoreExceptions()

  def testInvalidRouteType(self):
    self.SetArchiveContents(
        "routes.txt",
        "route_id,route_short_name,route_long_name,route_type,co2_per_km\n"
        "AB,,Airport Bullfrog,2557,15.5\n")
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.PopInvalidValue("route_type")
    self.accumulator.AssertNoMoreExceptions()


class AgencyLangTestCase(ExtensionMemoryZipTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  def testNotWellFormedAgencyLang(self):
    self.SetArchiveContents(
        "agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
        "DTA,Demo Agency,http://google.com,America/Los_Angeles,lang123456789\n")
    self.MakeLoaderAndLoad(self.problems,
                           gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopInvalidValue("agency_lang")
    e_msg = e.FormatProblem()
    self.assertTrue(e_msg.find('not well-formed') != -1,
                    '%s should not be well-formed, is: %s' % (e.value, e_msg))
    self.accumulator.AssertNoMoreExceptions()

  def testNotValidAgencyLang(self):
    self.SetArchiveContents(
        "agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
        "DTA,Demo Agency,http://google.com,America/Los_Angeles,fra-XY\n")

    self.MakeLoaderAndLoad(self.problems,
                           gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopInvalidValue("agency_lang")
    e_msg = e.FormatProblem()
    self.assertTrue(e_msg.find('not valid') != -1,
                    '%s should not be valid, is: %s' % (e.value, e_msg))
    self.accumulator.AssertNoMoreExceptions()


class DeprecatedColumnsTestCase(MemoryZipTestCase):
  """Check that Deprecated Fields are getting handled correctly."""
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  def testAgencyDeprecatedColumns(self):
    self.SetArchiveContents(
        "agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
        "DTA,Demo Agency,http://google.com,America/Los_Angeles,en\n")

    self.MakeLoaderAndLoad(self.problems,
                           gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("DeprecatedColumn")
    self.assertEquals(e.column_name, 'agency_lang')
    e = self.accumulator.PopException("DeprecatedColumn")
    self.assertEquals(e.column_name, 'agency_timezone')
    self.accumulator.AssertNoMoreExceptions()
