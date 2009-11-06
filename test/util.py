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
import shutil
import subprocess
import sys
import tempfile
import traceback
import transitfeed
import unittest


def check_call(cmd, expected_retcode=0, stdin_str="", **kwargs):
  """Convenience function that is in the docs for subprocess but not
  installed on my system. Raises an Exception if the return code is not
  expected_retcode. Returns a tuple of strings, (stdout, stderr)."""
  try:
    if 'stdout' in kwargs or 'stderr' in kwargs or 'stdin' in kwargs:
      raise Exception("Don't pass stdout or stderr")
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE,
                         stderr=subprocess.PIPE, stdin=subprocess.PIPE,
                         **kwargs)
    (out, err) = p.communicate(stdin_str)
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


class TestCaseAsserts(unittest.TestCase):
  def assertMatchesRegex(self, regex, string):
    """Assert that regex is found in string."""
    if not re.search(regex, string):
      self.fail("string %r did not match regex %r" % (string, regex))


class GetPathTestCase(TestCaseAsserts):
  """TestCase with method to get paths to files in the distribution."""
  def setUp(self):
    TestCaseAsserts.setUp(self)
    self._origcwd = os.getcwd()

  def GetExamplePath(self, name):
    """Return the full path of a file in the examples directory"""
    return self.GetPath('examples', name)

  def GetTestDataPath(self, *path):
    """Return the full path of a file in the test/data directory"""
    return self.GetPath('test', 'data', *path)

  def GetPath(self, *path):
    """Return absolute path of path. path is relative main source directory."""
    here = os.path.dirname(__file__)  # Relative to _origcwd
    return os.path.join(self._origcwd, here, '..', *path)


class TempDirTestCaseBase(GetPathTestCase):
  """Make a temporary directory the current directory before running the test
  and remove it after the test.
  """
  def setUp(self):
    GetPathTestCase.setUp(self)
    self.tempdirpath = tempfile.mkdtemp()
    os.chdir(self.tempdirpath)

  def tearDown(self):
    os.chdir(self._origcwd)
    shutil.rmtree(self.tempdirpath)
    GetPathTestCase.tearDown(self)

  def CheckCallWithPath(self, cmd, expected_retcode=0, stdin_str=""):
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

    # Propogate sys.path of this process to the subprocess. This is done
    # because I assume that if this process has a customized sys.path it is
    # meant to be used for all processes involved in the tests.  The downside
    # of this is that the subprocess is no longer a clean version of what you
    # get when running "python" after installing transitfeed. Hopefully if this
    # process uses a customized sys.path you know what you are doing.
    env = {"PYTHONPATH": ":".join(sys.path)}

    # Instead of directly running the script make sure that the transitfeed
    # module in this source directory is at the front of sys.path. Then
    # adjust sys.argv so it looks like the script was run directly. This lets
    # OptionParser use the correct value for %proj.
    cmd = [sys.executable, "-c",
           "import sys; "
           "sys.path.insert(0,'%s'); "
           "sys.argv = ['%s'] + sys.argv[1:]; "
           "exec(open('%s'))" %
           (transitfeed_parent, script_path, script_path)] + script_args
    return check_call(cmd, expected_retcode=expected_retcode, shell=False,
                      env=env, stdin_str=stdin_str)


class RecordingProblemReporter(transitfeed.ProblemReporterBase):
  """Save all problems for later inspection.

  Args:
    test_case: a unittest.TestCase object on which to report problems
    ignore_types: sequence of string type names that will be ignored by the
    ProblemReporter"""
  def __init__(self, test_case, ignore_types=None):
    transitfeed.ProblemReporterBase.__init__(self)
    self.exceptions = []
    self._test_case = test_case
    self._ignore_types = ignore_types or set()

  def _Report(self, e):
    # Ensure that these don't crash
    e.FormatProblem()
    e.FormatContext()
    if e.__class__.__name__ in self._ignore_types:
      return
    # Keep the 7 nearest stack frames. This should be enough to identify
    # the code path that created the exception while trimming off most of the
    # large test framework's stack.
    traceback_list = traceback.format_list(traceback.extract_stack()[-7:-1])
    self.exceptions.append((e, ''.join(traceback_list)))

  def PopException(self, type_name):
    """Return the first exception, which must be a type_name."""
    e = self.exceptions.pop(0)
    e_name = e[0].__class__.__name__
    self._test_case.assertEqual(e_name, type_name,
                                "%s != %s\n%s" %
                                (e_name, type_name, self.FormatException(*e)))
    return e[0]

  def FormatException(self, exce, tb):
    return ("%s\nwith gtfs file context %s\nand traceback\n%s" %
            (exce.FormatProblem(), exce.FormatContext(), tb))

  def AssertNoMoreExceptions(self):
    exceptions_as_text = []
    for e, tb in self.exceptions:
      exceptions_as_text.append(self.FormatException(e, tb))
    self._test_case.assertFalse(self.exceptions, "\n".join(exceptions_as_text))

  def PopInvalidValue(self, column_name, file_name=None):
    e = self.PopException("InvalidValue")
    self._test_case.assertEquals(column_name, e.column_name)
    if file_name:
      self._test_case.assertEquals(file_name, e.file_name)
    return e

  def PopMissingValue(self, column_name, file_name=None):
    e = self.PopException("MissingValue")
    self._test_case.assertEquals(column_name, e.column_name)
    if file_name:
      self._test_case.assertEquals(file_name, e.file_name)
    return e
