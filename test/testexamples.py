#!/usr/bin/python2.4

# Test the examples to make sure they are not broken

import os
import subprocess
import tempfile
import urllib
import unittest
import re


def check_call(cmd, **kwargs):
  """Convenience function that is in the docs for subprocess but not
  installed on my system."""
  retcode = subprocess.call(cmd, **kwargs)
  if retcode < 0:
    raise Exception("Child '%s' was terminated by signal %d" % (cmd,
      -retcode))
  elif retcode != 0:
    raise Exception("Child '%s' returned %d" % (cmd, retcode))


class TempDirTestCaseBase(unittest.TestCase):
  """Make a temporary directory the current directory before running the test
  and remove it after the test.
  """
  def setUp(self):
    self.tempdirpath = tempfile.mkdtemp()
    self._oldcwd = os.getcwd()
    os.chdir(self.tempdirpath)

  def tearDown(self):
    os.chdir(self._oldcwd)
    # Remove everything in self.tempdirpath
    for root, dirs, files in os.walk(self.tempdirpath, topdown=False):
      for name in files:
        os.remove(os.path.join(root, name))
      for name in dirs:
        os.rmdir(os.path.join(root, name))

  def GetExamplePath(self, name):
    """Return the full path of a file in the examples directory"""
    here = os.path.dirname(__file__)  # Relative to _oldcwd
    return os.path.join(self._oldcwd, here, '..', 'examples', name)

  def CheckCallWithPath(self, cmd):
    """Run cmd[0] with args cmd[1:], pointing PYTHONPATH to the root of this
    source tree."""
    env = {'PYTHONPATH': self.GetExamplePath('..')}
    check_call(cmd, shell=False, env=env)


class WikiExample(TempDirTestCaseBase):
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


class shuttle_from_xmlfeed(TempDirTestCaseBase):
  def runTest(self):
    self.CheckCallWithPath(
        [self.GetExamplePath('shuttle_from_xmlfeed.py'),
         '--input', self.GetExamplePath('shuttle_from_xmlfeed.xml'),
         '--output', 'shuttle-YYYYMMDD.zip',
         # save the path of the dated output to tempfilepath
         '--execute', 'echo %(path)s > outputpath'])
  
    dated_path = open('outputpath').read().strip()
    self.assertTrue(re.match(r'shuttle-20\d\d[01]\d[0123]\d.zip$', dated_path))
    if not os.path.exists(dated_path):
      raise Exception('did not create expected file')
  

class table(TempDirTestCaseBase):
  def runTest(self):
    self.CheckCallWithPath(
        [self.GetExamplePath('table.py'),
         '--input', self.GetExamplePath('table.txt'),
         '--output', 'google_transit.zip'])
    if not os.path.exists('google_transit.zip'):
      raise Exception('should have created output')


class small_builder(TempDirTestCaseBase):
  def runTest(self):
    self.CheckCallWithPath(
        [self.GetExamplePath('small_builder.py'),
         '--output', 'google_transit.zip'])
    if not os.path.exists('google_transit.zip'):
      raise Exception('should have created output')


if __name__ == '__main__':
  unittest.main()
