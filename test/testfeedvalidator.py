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

import re
import unittest
import util

class good_feed(util.TempDirTestCaseBase):
  def runTest(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n',
         self.GetPath('test', 'data', 'good_feed')])
    self.assertTrue(re.search(r'feed validated successfully', out))
    self.assertFalse(re.search(r'ERROR', out))
    htmlout = open('validation-results.html').read()
    self.assertTrue(re.search(r'feed validated successfully', htmlout))
    self.assertFalse(re.search(r'ERROR', htmlout))

class missing_stops(util.TempDirTestCaseBase):
  def runTest(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n',
         self.GetPath('test', 'data', 'missing_stops')],
        expected_retcode=1)
    self.assertTrue(re.search(r'ERROR', out))
    self.assertFalse(re.search(r'feed validated successfully', out))
    htmlout = open('validation-results.html').read()
    self.assertTrue(re.search(r'Invalid value FUR_CREEK_RES', htmlout))
    self.assertFalse(re.search(r'feed validated successfully', htmlout))


class bad_date_format(util.TempDirTestCaseBase):
  def runTest(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n',
         self.GetPath('test', 'data', 'bad_date_format')],
        expected_retcode=1)
    self.assertTrue(re.search(r'ERROR', out))
    self.assertFalse(re.search(r'feed validated successfully', out))
    htmlout = open('validation-results.html').read()
    self.assertTrue(re.search(r'in field <code>start_date', htmlout))
    self.assertTrue(re.search(r'in field <code>date', htmlout))
    self.assertFalse(re.search(r'feed validated successfully', htmlout))


class crash_handler(util.TempDirTestCaseBase):
  def runTest(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('feedvalidator.py'), '-n',
         'IWantMyvalidation-crash.txt'],
        expected_retcode=127)
    self.assertTrue(re.search(r'Yikes', out))
    self.assertFalse(re.search(r'feed validated successfully', out))
    crashout = open('validation-crash.txt').read()
    self.assertTrue(re.search(r'For testing the feed validator crash handler',
                              crashout))

if __name__ == '__main__':
  unittest.main()
