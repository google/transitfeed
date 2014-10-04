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

# Unit tests for transitfeed/util.py

import re
import StringIO
import tests.util as test_util
from transitfeed.problems import ProblemReporter
from transitfeed import util
from transitfeed import version
import unittest
from urllib2 import HTTPError, URLError
import urllib2

class CheckVersionTestCase(test_util.TempDirTestCaseBase):
  def setUp(self):
    self.orig_urlopen = urllib2.urlopen
    self.mock = MockURLOpen()
    self.accumulator = test_util.RecordingProblemAccumulator(self)
    self.problems = ProblemReporter(self.accumulator)

  def tearDown(self):
    self.mock = None
    urllib2.urlopen = self.orig_urlopen

  def testAssignedDifferentVersion(self):
    util.CheckVersion(self.problems, '100.100.100')
    e = self.accumulator.PopException('NewVersionAvailable')
    self.assertEqual(e.version, '100.100.100')
    self.assertEqual(e.url, 'https://github.com/google/transitfeed')
    self.accumulator.AssertNoMoreExceptions()

  def testAssignedSameVersion(self):
    util.CheckVersion(self.problems, version.__version__)
    self.accumulator.AssertNoMoreExceptions()

  def testGetCorrectReturns(self):
    urllib2.urlopen = self.mock.mockedConnectSuccess
    util.CheckVersion(self.problems)
    self.accumulator.PopException('NewVersionAvailable')

  def testPageNotFound(self):
    urllib2.urlopen = self.mock.mockedPageNotFound
    util.CheckVersion(self.problems)
    e = self.accumulator.PopException('OtherProblem')
    self.assertTrue(re.search(r'we failed to reach', e.description))
    self.assertTrue(re.search(r'Reason: Not Found \[404\]', e.description))

  def testConnectionTimeOut(self):
    urllib2.urlopen = self.mock.mockedConnectionTimeOut
    util.CheckVersion(self.problems)
    e = self.accumulator.PopException('OtherProblem')
    self.assertTrue(re.search(r'we failed to reach', e.description))
    self.assertTrue(re.search(r'Reason: Connection timed', e.description))

  def testGetAddrInfoFailed(self):
    urllib2.urlopen = self.mock.mockedGetAddrInfoFailed
    util.CheckVersion(self.problems)
    e = self.accumulator.PopException('OtherProblem')
    self.assertTrue(re.search(r'we failed to reach', e.description))
    self.assertTrue(re.search(r'Reason: Getaddrinfo failed', e.description))

  def testEmptyIsReturned(self):
    urllib2.urlopen = self.mock.mockedEmptyIsReturned
    util.CheckVersion(self.problems)
    e = self.accumulator.PopException('OtherProblem')
    self.assertTrue(re.search(r'we had trouble parsing', e.description))


class MockURLOpen:
  """Pretend to be a urllib2.urlopen suitable for testing."""
  def mockedConnectSuccess(self, request):
    return StringIO.StringIO('latest_version=100.0.1')

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
