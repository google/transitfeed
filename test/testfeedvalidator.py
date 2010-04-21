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

# Smoke tests feed validator. Make sure it runs and returns the right things
# for a valid feed and a feed with errors.

import datetime
import feedvalidator
import os.path
import re
import StringIO
import transitfeed
import unittest
from urllib2 import HTTPError, URLError
import urllib2
import util
import zipfile


class FullTests(util.TempDirTestCaseBase):
  def testGoodFeed(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__, self.GetPath('test', 'data', 'good_feed')])
    self.assertTrue(re.search(r'feed validated successfully', out))
    self.assertFalse(re.search(r'ERROR', out))
    htmlout = open('validation-results.html').read()
    self.assertTrue(re.search(r'feed validated successfully', htmlout))
    self.assertFalse(re.search(r'ERROR', htmlout))
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testGoodFeedConsoleOutput(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__,
         '--output=CONSOLE', self.GetPath('test', 'data', 'good_feed')])
    self.assertTrue(re.search(r'feed validated successfully', out))
    self.assertFalse(re.search(r'ERROR', out))
    self.assertFalse(os.path.exists('validation-results.html'))
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testMissingStops(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__,
         self.GetPath('test', 'data', 'missing_stops')],
        expected_retcode=1)
    self.assertTrue(re.search(r'ERROR', out))
    self.assertFalse(re.search(r'feed validated successfully', out))
    htmlout = open('validation-results.html').read()
    self.assertTrue(re.search(r'Invalid value BEATTY_AIRPORT', htmlout))
    self.assertFalse(re.search(r'feed validated successfully', htmlout))
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testMissingStopsConsoleOutput(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '-o', 'console',
         '--latest_version', transitfeed.__version__,
         self.GetPath('test', 'data', 'missing_stops')],
        expected_retcode=1)
    self.assertTrue(re.search(r'ERROR', out))
    self.assertFalse(re.search(r'feed validated successfully', out))
    self.assertTrue(re.search(r'Invalid value BEATTY_AIRPORT', out))
    self.assertFalse(os.path.exists('validation-results.html'))
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testLimitedErrors(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-l', '2', '-n',
         '--latest_version', transitfeed.__version__,
         self.GetPath('test', 'data', 'missing_stops')],
        expected_retcode=1)
    self.assertTrue(re.search(r'ERROR', out))
    self.assertFalse(re.search(r'feed validated successfully', out))
    htmlout = open('validation-results.html').read()
    self.assertEquals(2, len(re.findall(r'class="problem">stop_id<', htmlout)))
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testBadDateFormat(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__,
         self.GetPath('test', 'data', 'bad_date_format')],
        expected_retcode=1)
    self.assertTrue(re.search(r'ERROR', out))
    self.assertFalse(re.search(r'feed validated successfully', out))
    htmlout = open('validation-results.html').read()
    self.assertTrue(re.search(r'in field <code>start_date', htmlout))
    self.assertTrue(re.search(r'in field <code>date', htmlout))
    self.assertFalse(re.search(r'feed validated successfully', htmlout))
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testBadUtf8(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__, self.GetPath('test', 'data', 'bad_utf8')],
        expected_retcode=1)
    self.assertTrue(re.search(r'ERROR', out))
    self.assertFalse(re.search(r'feed validated successfully', out))
    htmlout = open('validation-results.html').read()
    self.assertTrue(re.search(r'Unicode error', htmlout))
    self.assertFalse(re.search(r'feed validated successfully', htmlout))
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testFileNotFound(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__, 'file-not-found.zip'],
        expected_retcode=1)
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testBadOutputPath(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__, '-o', 'path/does/not/exist.html',
         self.GetPath('test', 'data', 'good_feed')],
        expected_retcode=2)
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testCrashHandler(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__, 'IWantMyvalidation-crash.txt'],
        expected_retcode=127)
    self.assertTrue(re.search(r'Yikes', out))
    self.assertFalse(re.search(r'feed validated successfully', out))
    crashout = open('transitfeedcrash.txt').read()
    self.assertTrue(re.search(r'For testing the feed validator crash handler',
                              crashout))

  def testCheckVersionIsRun(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         '100.100.100', self.GetPath('test', 'data', 'good_feed')])
    self.assertTrue(re.search(r'feed validated successfully', out))
    self.assertTrue(re.search(r'A new version 100.100.100', out))
    htmlout = open('validation-results.html').read()
    self.assertTrue(re.search(r'A new version 100.100.100', htmlout))
    self.assertFalse(re.search(r'ERROR', htmlout))
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testCheckVersionIsRunConsoleOutput(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '-o', 'console',
         '--latest_version=100.100.100',
         self.GetPath('test', 'data', 'good_feed')])
    self.assertTrue(re.search(r'feed validated successfully', out))
    self.assertTrue(re.search(r'A new version 100.100.100', out))
    self.assertFalse(os.path.exists('validation-results.html'))
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testUsage(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '--invalid_opt'], expected_retcode=2)
    self.assertMatchesRegex(r'[Uu]sage: feedvalidator.py \[options\]', err)
    self.assertMatchesRegex(r'wiki/FeedValidator', err)
    self.assertMatchesRegex(r'--output', err)  # output includes all usage info
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))
    self.assertFalse(os.path.exists('validation-results.html'))


# Regression tests to ensure that CalendarSummary works properly
# even when the feed starts in the future or expires in less than
# 60 days
# See http://code.google.com/p/googletransitdatafeed/issues/detail?id=204
class CalendarSummaryTestCase(util.TestCase):
  
  # Test feeds starting in the future
  def testFutureFeedDoesNotCrashCalendarSummary(self):
      today = datetime.date.today()
      start_date = today + datetime.timedelta(days=20)
      end_date = today + datetime.timedelta(days=80)
      
      schedule = transitfeed.Schedule()
      service_period = schedule.GetDefaultServicePeriod()

      service_period.SetStartDate(start_date.strftime("%Y%m%d"))
      service_period.SetEndDate(end_date.strftime("%Y%m%d"))
      service_period.SetWeekdayService(True)
      
      result = feedvalidator.CalendarSummary(schedule)
      
      self.assertEquals(0, result['max_trips'])
      self.assertEquals(0, result['min_trips'])
      self.assertTrue(re.search("40 service dates", result['max_trips_dates']))

  # Test feeds ending in less than 60 days
  def testShortFeedDoesNotCrashCalendarSummary(self):
      start_date = datetime.date.today()
      end_date = start_date + datetime.timedelta(days=15)

      schedule = transitfeed.Schedule()
      service_period = schedule.GetDefaultServicePeriod()

      service_period.SetStartDate(start_date.strftime("%Y%m%d"))
      service_period.SetEndDate(end_date.strftime("%Y%m%d"))
      service_period.SetWeekdayService(True)

      result = feedvalidator.CalendarSummary(schedule)

      self.assertEquals(0, result['max_trips'])
      self.assertEquals(0, result['min_trips'])
      self.assertTrue(re.search("15 service dates", result['max_trips_dates']))

  # Test feeds starting in the future *and* ending in less than 60 days
  def testFutureAndShortFeedDoesNotCrashCalendarSummary(self):
      today = datetime.date.today()
      start_date = today + datetime.timedelta(days=2)
      end_date = today + datetime.timedelta(days=3)
      
      schedule = transitfeed.Schedule()
      service_period = schedule.GetDefaultServicePeriod()

      service_period.SetStartDate(start_date.strftime("%Y%m%d"))
      service_period.SetEndDate(end_date.strftime("%Y%m%d"))
      service_period.SetWeekdayService(True)
      
      result = feedvalidator.CalendarSummary(schedule)
      
      self.assertEquals(0, result['max_trips'])
      self.assertEquals(0, result['min_trips'])
      self.assertTrue(re.search("1 service date", result['max_trips_dates']))

  # Test feeds without service days
  def testFeedWithNoDaysDoesNotCrashCalendarSummary(self):
      schedule = transitfeed.Schedule()
      result = feedvalidator.CalendarSummary(schedule)

      self.assertEquals({}, result)


class MockOptions:
  """Pretend to be an optparse options object suitable for testing."""
  def __init__(self):
    self.limit_per_type = 5
    self.memory_db = True
    self.check_duplicate_trips = True
    self.latest_version = transitfeed.__version__
    self.output = 'fake-filename.zip'
    self.manual_entry = False
    self.service_gap_interval = None
    self.extension = None


class FeedValidatorTestCase(util.TempDirTestCaseBase):
  def testBadEolContext(self):
    """Make sure the filename is included in the report of a bad eol."""
    zipfile_mem = StringIO.StringIO(open(
        self.GetPath('test', 'data', 'good_feed.zip'), 'rb').read())
    zip = zipfile.ZipFile(zipfile_mem, 'a')
    routes_txt = zip.read('routes.txt')
    # routes_txt_modified is invalid because the first line ends with \r\n.
    routes_txt_modified = routes_txt.replace('\n', '\r\n', 1)
    self.assertNotEquals(routes_txt_modified, routes_txt)
    zip.writestr('routes.txt', routes_txt_modified)
    zip.close()
    options = MockOptions()
    output_file = StringIO.StringIO()
    feedvalidator.RunValidationOutputToFile(zipfile_mem, options, output_file)
    self.assertMatchesRegex("routes.txt", output_file.getvalue())


class LimitPerTypeProblemReporterTestCase(util.TestCase):
  def assertProblemsAttribute(self, problem_type, class_name, attribute_name,
                              expected):
    """Join the value of each exception's attribute_name in order."""
    problem_attribute_list = []
    for e in self.problems.ProblemList(problem_type, class_name).problems:
      problem_attribute_list.append(getattr(e, attribute_name))
    self.assertEquals(expected, " ".join(problem_attribute_list))

  def testLimitOtherProblems(self):
    """The first N of each type should be kept."""
    self.problems = feedvalidator.LimitPerTypeProblemReporter(2)
    self.problems.OtherProblem("e1", type=transitfeed.TYPE_ERROR)
    self.problems.OtherProblem("w1", type=transitfeed.TYPE_WARNING)
    self.problems.OtherProblem("e2", type=transitfeed.TYPE_ERROR)
    self.problems.OtherProblem("e3", type=transitfeed.TYPE_ERROR)
    self.problems.OtherProblem("w2", type=transitfeed.TYPE_WARNING)
    self.assertEquals(2, self.problems.WarningCount())
    self.assertEquals(3, self.problems.ErrorCount())

    # These are BoundedProblemList objects
    warning_bounded_list = self.problems.ProblemList(
        transitfeed.TYPE_WARNING, "OtherProblem")
    error_bounded_list = self.problems.ProblemList(
        transitfeed.TYPE_ERROR, "OtherProblem")
   
    self.assertEquals(2, warning_bounded_list.count)
    self.assertEquals(3, error_bounded_list.count)

    self.assertEquals(0, warning_bounded_list.dropped_count)
    self.assertEquals(1, error_bounded_list.dropped_count)

    self.assertProblemsAttribute(transitfeed.TYPE_ERROR,  "OtherProblem",
        "description", "e1 e2")
    self.assertProblemsAttribute(transitfeed.TYPE_WARNING,  "OtherProblem",
        "description", "w1 w2")

  def testKeepUnsorted(self):
    """An imperfect test that insort triggers ExceptionWithContext.__cmp__."""
    # If ExceptionWithContext.__cmp__ doesn't trigger TypeError in
    # bisect.insort then the default comparison of object id will be used. The
    # id values tend to be given out in order of creation so call
    # problems._Report with objects in a different order. This test should
    # break if ExceptionWithContext.__cmp__ is removed or changed to return 0
    # or cmp(id(self), id(y)).
    exceptions = []
    for i in range(20):
      exceptions.append(transitfeed.OtherProblem(description="e%i" % i))
    exceptions = exceptions[10:] + exceptions[:10]
    self.problems = feedvalidator.LimitPerTypeProblemReporter(3)
    for e in exceptions:
      self.problems._Report(e)

    self.assertEquals(0, self.problems.WarningCount())
    self.assertEquals(20, self.problems.ErrorCount())

    bounded_list = self.problems.ProblemList(
        transitfeed.TYPE_ERROR, "OtherProblem")
    self.assertEquals(20, bounded_list.count)
    self.assertEquals(17, bounded_list.dropped_count)
    self.assertProblemsAttribute(transitfeed.TYPE_ERROR,  "OtherProblem",
        "description", "e10 e11 e12")

  def testLimitSortedTooFastTravel(self):
    """Sort by decreasing distance, keeping the N greatest."""
    self.problems = feedvalidator.LimitPerTypeProblemReporter(3)
    self.problems.TooFastTravel("t1", "prev stop", "next stop", 11230.4, 5,
        None)
    self.problems.TooFastTravel("t2", "prev stop", "next stop", 1120.4, 5, None)
    self.problems.TooFastTravel("t3", "prev stop", "next stop", 1130.4, 5, None)
    self.problems.TooFastTravel("t4", "prev stop", "next stop", 1230.4, 5, None)
    self.assertEquals(0, self.problems.WarningCount())
    self.assertEquals(4, self.problems.ErrorCount())
    self.assertProblemsAttribute(transitfeed.TYPE_ERROR, "TooFastTravel",
        "trip_id", "t1 t4 t3")

  def testLimitSortedStopTooFarFromParentStation(self):
    """Sort by decreasing distance, keeping the N greatest."""
    self.problems = feedvalidator.LimitPerTypeProblemReporter(3)
    for i, distance in enumerate((1000, 3002.0, 1500, 2434.1, 5023.21)):
      self.problems.StopTooFarFromParentStation(
          "s%d" % i, "S %d" % i, "p%d" % i, "P %d" % i, distance)
    self.assertEquals(5, self.problems.WarningCount())
    self.assertEquals(0, self.problems.ErrorCount())
    self.assertProblemsAttribute(transitfeed.TYPE_WARNING,
        "StopTooFarFromParentStation", "stop_id", "s4 s1 s3")

  def testLimitSortedStopsTooClose(self):
    """Sort by increasing distance, keeping the N closest."""
    self.problems = feedvalidator.LimitPerTypeProblemReporter(3)
    for i, distance in enumerate((4.0, 3.0, 2.5, 2.2, 1.0, 0.0)):
      self.problems.StopsTooClose(
          "Sa %d" % i, "sa%d" % i, "Sb %d" % i, "sb%d" % i, distance)
    self.assertEquals(6, self.problems.WarningCount())
    self.assertEquals(0, self.problems.ErrorCount())
    self.assertProblemsAttribute(transitfeed.TYPE_WARNING,
        "StopsTooClose", "stop_id_a", "sa5 sa4 sa3")
    

class CheckVersionTestCase(util.TempDirTestCaseBase):
  def setUp(self):
    self.mock = MockURLOpen()

  def tearDown(self):
    self.mock = None
    feedvalidator.urlopen = urllib2.urlopen

  def testAssignedDifferentVersion(self):
    problems = feedvalidator.CheckVersion('100.100.100')
    self.assertTrue(re.search(r'A new version 100.100.100', problems))

  def testAssignedSameVersion(self):
    problems = feedvalidator.CheckVersion(transitfeed.__version__)
    self.assertEquals(problems, None)

  def testGetCorrectReturns(self):
    feedvalidator.urlopen = self.mock.mockedConnectSuccess
    problems = feedvalidator.CheckVersion()
    self.assertTrue(re.search(r'A new version 100.0.1', problems))

  def testPageNotFound(self):
    feedvalidator.urlopen = self.mock.mockedPageNotFound
    problems = feedvalidator.CheckVersion()
    self.assertTrue(re.search(r'The server couldn\'t', problems))
    self.assertTrue(re.search(r'Error code: 404', problems))

  def testConnectionTimeOut(self):
    feedvalidator.urlopen = self.mock.mockedConnectionTimeOut
    problems = feedvalidator.CheckVersion()
    self.assertTrue(re.search(r'We failed to reach', problems))
    self.assertTrue(re.search(r'Reason: Connection timed', problems))

  def testGetAddrInfoFailed(self):
    feedvalidator.urlopen = self.mock.mockedGetAddrInfoFailed
    problems = feedvalidator.CheckVersion()
    self.assertTrue(re.search(r'We failed to reach', problems))
    self.assertTrue(re.search(r'Reason: Getaddrinfo failed', problems))

  def testEmptyIsReturned(self):
    feedvalidator.urlopen = self.mock.mockedEmptyIsReturned
    problems = feedvalidator.CheckVersion()
    self.assertTrue(re.search(r'We had trouble parsing', problems))


class MockURLOpen:
  """Pretend to be a urllib2.urlopen suitable for testing."""
  def mockedConnectSuccess(self, request):
    return StringIO.StringIO('<li><a href="transitfeed-1.0.0/">transitfeed-'
                             '1.0.0/</a></li><li><a href=transitfeed-100.0.1/>'
                             'transitfeed-100.0.1/</a></li>')

  def mockedPageNotFound(self, request):
    raise HTTPError(request.get_full_url(), 404, 'Not Found',
                    request.header_items(), None)

  def mockedConnectionTimeOut(self, request):
    raise URLError('Connection timed out')

  def mockedGetAddrInfoFailed(self, request):
    raise URLError('Getaddrinfo failed')

  def mockedEmptyIsReturned(self, request):
    return StringIO.StringIO()


if __name__ == '__main__':
  unittest.main()
