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

# Code shared between tests.

import os
import os.path
import re
import cStringIO as StringIO
import subprocess
import sys
import tempfile
import transitfeed
import unittest


def check_call(cmd, expected_retcode=0, **kwargs):
  """Convenience function that is in the docs for subprocess but not
  installed on my system. Raises an Exception if the return code is not
  expected_retcode. Returns a tuple of strings, (stdout, stderr)."""
  try:
    if 'stdout' in kwargs or 'stderr' in kwargs:
      raise Exception("Don't pass stdout or stderr")
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, **kwargs)
    (out, err) = p.communicate()
    retcode = p.returncode
  except Exception, e:
    raise Exception("When running %s: %s" % (cmd, e))
  if retcode < 0:
    raise Exception(
        "Child '%s' was terminated by signal %d. Output:\n%s\n%s\n" %
        (cmd, -retcode, out, err))
  elif retcode != expected_retcode:
    raise Exception(
        "Child '%s' returned %d. Output:\n%s\n%s\n" %
        (cmd, retcode, out, err))
  return (out, err)

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
    return self.GetPath('examples', name)

  def GetPath(self, *path):
    """Return the absolute path of path, relative to cwd when test was
    started."""
    here = os.path.dirname(__file__)  # Relative to _oldcwd
    return os.path.join(self._oldcwd, here, '..', *path)

  def CheckCallWithPath(self, cmd, expected_retcode=0):
    """Run python script cmd[0] with args cmd[1:], making sure 'import
    transitfeed' will use the module in this source tree. Raises an Exception
    if the return code is not expected_retcode. Returns a tuple of strings,
    (stdout, stderr)."""
    tf_path = transitfeed.__file__
    # Path of the directory containing transitfeed. When this is added to
    # sys.path importing transitfeed should work independent of if
    # transitfeed.__file__ is <parent>/transitfeed.py or
    # <parent>/transitfeed/__init__.py
    transitfeed_parent = tf_path[:tf_path.rfind("transitfeed")]
    transitfeed_parent = transitfeed_parent.replace("\\", "/").rstrip("/")
    script_path = cmd[0].replace("\\", "/")
    script_args = cmd[1:]

    cmd = [sys.executable,
           "-c",
           "import sys; sys.path.insert(0,'%s');exec(open('%s'))" %
           (transitfeed_parent, script_path)] + script_args
    return check_call(cmd, expected_retcode=expected_retcode, shell=False)
