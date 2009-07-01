#!/usr/bin/python2.5

# Test the examples to make sure they are not broken

import os
import re
import transitfeed
import unittest
import urllib
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

  def testInvalidFeedStillWorks(self):
    self.CheckCallWithPath(
        [self.GetExamplePath('google_random_queries.py'),
         '--output', 'queries.html',
         '--limit', '5',
         self.GetPath('test', 'data', 'invalid_route_agency')])
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


class filter_unused_stops(util.TempDirTestCaseBase):
  def testNormalRun(self):
    unused_stop_path = self.GetPath('test', 'data', 'unused_stop')
    # Make sure load fails for input
    problem_reporter = transitfeed.ExceptionProblemReporter(raise_warnings=True)
    try:
      transitfeed.Loader(
          unused_stop_path,
          problems=problem_reporter, extra_validation=True).Load()
      self.fail('UnusedStop exception expected')
    except transitfeed.UnusedStop, e:
      pass
    (stdout, stderr) = self.CheckCallWithPath(
        [self.GetExamplePath('filter_unused_stops.py'),
         '--list_removed',
         unused_stop_path, 'output.zip'])
    # Extra stop was listed on stdout
    self.assertNotEqual(stdout.find('Bogus Stop'), -1)
    # Make sure unused stop was removed and another stop wasn't
    schedule = transitfeed.Loader(
        'output.zip', problems=problem_reporter, extra_validation=True).Load()
    schedule.GetStop('STAGECOACH')


if __name__ == '__main__':
  unittest.main()
