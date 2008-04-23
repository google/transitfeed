#!/usr/bin/python2.5

# Test the examples to make sure they are not broken

import os
import urllib
import unittest
import re
import util

class WikiExample(util.TempDirTestCaseBase):
  # Download example from wiki and run it
  def runTest(self):
    wiki_source = urllib.urlopen(
        'http://googletransitdatafeed.googlecode.com/svn/wiki/TransitFeed.wiki'
        ).read()
    m = re.search(r'{{{(.*import transitfeed.*)}}}', wiki_source, re.DOTALL)
    if not m:
      raise Exception("Failed to find source code on wiki page")
    wiki_code = m.group(1)
    exec wiki_code


class shuttle_from_xmlfeed(util.TempDirTestCaseBase):
  def runTest(self):
    self.CheckCallWithPath(
        [self.GetExamplePath('shuttle_from_xmlfeed.py'),
         '--input', 'file:' + self.GetExamplePath('shuttle_from_xmlfeed.xml'),
         '--output', 'shuttle-YYYYMMDD.zip',
         # save the path of the dated output to tempfilepath
         '--execute', 'echo %(path)s > outputpath'])

    dated_path = open('outputpath').read().strip()
    self.assertTrue(re.match(r'shuttle-20\d\d[01]\d[0123]\d.zip$', dated_path))
    if not os.path.exists(dated_path):
      raise Exception('did not create expected file')


class table(util.TempDirTestCaseBase):
  def runTest(self):
    self.CheckCallWithPath(
        [self.GetExamplePath('table.py'),
         '--input', self.GetExamplePath('table.txt'),
         '--output', 'google_transit.zip'])
    if not os.path.exists('google_transit.zip'):
      raise Exception('should have created output')


class small_builder(util.TempDirTestCaseBase):
  def runTest(self):
    self.CheckCallWithPath(
        [self.GetExamplePath('small_builder.py'),
         '--output', 'google_transit.zip'])
    if not os.path.exists('google_transit.zip'):
      raise Exception('should have created output')


class google_random_queries(util.TempDirTestCaseBase):
  def testNormalRun(self):
    self.CheckCallWithPath(
        [self.GetExamplePath('google_random_queries.py'),
         '--output', 'queries.html',
         '--limit', '5',
         self.GetPath('test', 'data', 'good_feed')])
    if not os.path.exists('queries.html'):
      raise Exception('should have created output')

  def testBadArgs(self):
    self.CheckCallWithPath(
        [self.GetExamplePath('google_random_queries.py'),
         '--output', 'queries.html',
         '--limit', '5'],
        expected_retcode=2)
    if os.path.exists('queries.html'):
      raise Exception('should not have created output')


if __name__ == '__main__':
  unittest.main()
