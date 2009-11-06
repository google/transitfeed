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
    self.assertFalse(os.path.exists('validation-crash.txt'))

  def testGoodFeedConsoleOutput(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__,
         '--output=CONSOLE', self.GetPath('test', 'data', 'good_feed')])
    self.assertTrue(re.search(r'feed validated successfully', out))
    self.assertFalse(re.search(r'ERROR', out))
    self.assertFalse(os.path.exists('validation-results.html'))
    self.assertFalse(os.path.exists('validation-crash.txt'))

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
    self.assertFalse(os.path.exists('validation-crash.txt'))

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
    self.assertFalse(os.path.exists('validation-crash.txt'))

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
    self.assertFalse(os.path.exists('validation-crash.txt'))

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
    self.assertFalse(os.path.exists('validation-crash.txt'))

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
    self.assertFalse(os.path.exists('validation-crash.txt'))

  def testFileNotFound(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__, 'file-not-found.zip'],
        expected_retcode=1)
    self.assertFalse(os.path.exists('validation-crash.txt'))

  def testBadOutputPath(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__, '-o', 'path/does/not/exist.html',
         self.GetPath('test', 'data', 'good_feed')],
        expected_retcode=2)
    self.assertFalse(os.path.exists('validation-crash.txt'))

  def testCrashHandler(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '--latest_version',
         transitfeed.__version__, 'IWantMyvalidation-crash.txt'],
        expected_retcode=127)
    self.assertTrue(re.search(r'Yikes', out))
    self.assertFalse(re.search(r'feed validated successfully', out))
    crashout = open('validation-crash.txt').read()
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
    self.assertFalse(os.path.exists('validation-crash.txt'))

  def testCheckVersionIsRunConsoleOutput(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n', '-o', 'console',
         '--latest_version=100.100.100',
         self.GetPath('test', 'data', 'good_feed')])
    self.assertTrue(re.search(r'feed validated successfully', out))
    self.assertTrue(re.search(r'A new version 100.100.100', out))
    self.assertFalse(os.path.exists('validation-results.html'))
    self.assertFalse(os.path.exists('validation-crash.txt'))


class MockOptions:
  """Pretend to be an optparse options object suitable for testing."""
  def __init__(self):
    self.limit_per_type = 5
    self.memory_db = True
    self.check_duplicate_trips = True
    self.latest_version = transitfeed.__version__
    self.output = 'fake-filename.zip'
    self.manual_entry = False


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


class LimitPerTypeProblemReporterTestCase(unittest.TestCase):
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

    self.assertProblemsAttribute(transitfeed.TYPE_ERROR,  "OtherProblem",
        "description", "e1 e2")
    self.assertProblemsAttribute(transitfeed.TYPE_WARNING,  "OtherProblem",
        "description", "w1 w2")


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
