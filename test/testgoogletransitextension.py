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

import time
import transitfeed
from util import MemoryZipTestCase
from util import RecordingProblemAccumulator
from testtransitfeed import ValidationTestCase


class FareAttributeAgencyIdTestCase(MemoryZipTestCase):
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


class FeedInfoExtensionTestCase(MemoryZipTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  def setUp(self):
    super(FeedInfoExtensionTestCase, self).setUp()
    #modify agency.txt for all tests in this test case
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


class ScheduleTestCase(MemoryZipTestCase):
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


class ScheduleStartAndExpirationDatesTestCase(MemoryZipTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  #init dates to be close to now
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

  def setUp(self):
    super(ScheduleStartAndExpirationDatesTestCase, self).setUp()
    #re-init the accumulator without ignore_types = ("ExpirationDate",)
    self.accumulator = RecordingProblemAccumulator(self)
    self.problems = transitfeed.ProblemReporter(self.accumulator)

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
        self.two_weeks_ago, self.two_months, #calendar
        self.two_weeks,                      #calendar_dates
        "", "")                              #feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    self.accumulator.AssertNoMoreExceptions()

  def testExpirationDateCausedByServicePeriod(self):
    # test with no validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks_ago, self.two_weeks, #calendar
        self.one_week,                      #calendar_dates
        "", "")                             #feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("ExpirationDate")
    self.assertTrue("calendar.txt" in e.expiration_origin_file)
    self.accumulator.AssertNoMoreExceptions()
    # test with good validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks_ago, self.two_weeks,  #calendar
        self.one_week,                       #calendar_dates
        self.two_weeks_ago, self.two_months) #feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("ExpirationDate")
    self.assertTrue("calendar.txt" in e.expiration_origin_file)
    self.accumulator.AssertNoMoreExceptions()

  def testFutureServiceCausedByServicePeriod(self):
    # test with no validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.one_week, self.two_months, #calendar
        self.two_weeks,                 #calendar_dates
        "", "")                         #feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("FutureService")
    self.assertTrue("calendar.txt" in e.start_date_origin_file)
    self.accumulator.AssertNoMoreExceptions()
    # test with good validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.one_week, self.two_months,      #calendar
        self.two_weeks,                      #calendar_dates
        self.two_weeks_ago, self.two_months) #feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("FutureService")
    self.assertTrue("calendar.txt" in e.start_date_origin_file)
    self.accumulator.AssertNoMoreExceptions()

  def testExpirationDateCausedByServicePeriodDateException(self):
    # test with no validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks_ago, self.one_week, #calendar
        self.two_weeks,                    #calendar_dates
        "", "")                            #feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("ExpirationDate")
    self.assertTrue("calendar_dates.txt" in e.expiration_origin_file)
    self.accumulator.AssertNoMoreExceptions()
    # test with good validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks_ago, self.one_week,   #calendar
        self.two_weeks,                      #calendar_dates
        self.two_weeks_ago, self.two_months) #feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("ExpirationDate")
    self.assertTrue("calendar_dates.txt" in e.expiration_origin_file)
    self.accumulator.AssertNoMoreExceptions()

  def testFutureServiceCausedByServicePeriodDateException(self):
    # test with no validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks, self.two_months, #calendar
        self.one_week,                   #calendar_dates
        "", "")                          #feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("FutureService")
    self.assertTrue("calendar_dates.txt" in e.start_date_origin_file)
    self.accumulator.AssertNoMoreExceptions()
    # test with good validity dates specified in feed_info.txt
    self.prepareArchiveContents(
        self.two_weeks, self.two_months,     #calendar
        self.one_week,                       #calendar_dates
        self.two_weeks_ago, self.two_months) #feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("FutureService")
    self.assertTrue("calendar_dates.txt" in e.start_date_origin_file)
    self.accumulator.AssertNoMoreExceptions()

  def testExpirationDateCausedByFeedInfo(self):
    self.prepareArchiveContents(
        self.two_weeks_ago, self.two_months, #calendar
        self.one_week,                       #calendar_dates
        "", self.two_weeks)                  #feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("ExpirationDate")
    self.assertTrue("feed_info.txt" in e.expiration_origin_file)
    self.accumulator.AssertNoMoreExceptions()

  def testFutureServiceCausedByFeedInfo(self):
    self.prepareArchiveContents(
        self.two_weeks_ago, self.two_months, #calendar
        self.one_week_ago,                   #calendar_dates
        self.one_week, self.two_months)      #feed_info
    self.MakeLoaderAndLoad(self.problems, gtfs_factory=self.gtfs_factory)
    e = self.accumulator.PopException("FutureService")
    self.assertTrue("feed_info.txt" in e.start_date_origin_file)
    self.accumulator.AssertNoMoreExceptions()


class FrequencyExtensionTestCase(ValidationTestCase):
  gtfs_factory = extensions.googletransit.GetGtfsFactory()

  def testExactTimesStringValueConversion(self):
    frequency_class = self.gtfs_factory.Frequency
    # test that no exact_times converts to 0
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800"})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 0)
    # test that empty exact_times converts to 0
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": ""})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 0)
    # test that exact_times "0" converts to 0
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": "0"})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 0)
    # test that exact_times "1" converts to 1
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": "1"})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 1)
    self.accumulator.AssertNoMoreExceptions()

  def testExactTimesAsIntValue(self):
    frequency_class = self.gtfs_factory.Frequency
    # test that exact_times None converts to 0
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": None})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 0)
    # test that exact_times 0 remains 0
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": 0})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 0)
    # test that exact_times 1 remains 1
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": 1})
    frequency.ValidateBeforeAdd(self.problems)
    self.assertEquals(frequency.ExactTimes(), 1)
    self.accumulator.AssertNoMoreExceptions()

  def testExactTimesInvalidValues(self):
    frequency_class = self.gtfs_factory.Frequency
    # test that exact_times 15 raises error
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": 15})
    frequency.ValidateBeforeAdd(self.problems)
    self.accumulator.PopInvalidValue("exact_times")
    self.accumulator.AssertNoMoreExceptions()
    # test that exact_times "yes" raises error
    frequency = frequency_class(
        field_dict={"trip_id": "AB1,10", "start_time": "10:00:00",
                    "end_time": "23:01:00", "headway_secs": "1800",
                    "exact_times": "yes"})
    frequency.ValidateBeforeAdd(self.problems)
    self.accumulator.PopInvalidValue("exact_times")
    self.accumulator.AssertNoMoreExceptions()
