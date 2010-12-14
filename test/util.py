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
import zipfile


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


class TestCase(unittest.TestCase):
  """Base of every TestCase class in this project.

  This adds some methods that perhaps should be in unittest.TestCase.
  """
  # Note from Tom, Dec 9 2009: Be careful about adding setUp or tearDown
  # because they will be run a few hundred times.

  def assertMatchesRegex(self, regex, string):
    """Assert that regex is found in string."""
    if not re.search(regex, string):
      self.fail("string %r did not match regex %r" % (string, regex))


class GetPathTestCase(TestCase):
  """TestCase with method to get paths to files in the distribution."""
  def setUp(self):
    super(GetPathTestCase, self).setUp()
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

  def ConvertZipToDict(self, zip):
    """Converts a zip file into a dictionary.

    Arguments:
        zip: The zipfile whose contents are to be converted to a dictionary.

    Returns:
        A dictionary mapping filenames to file contents."""

    zip_dict = {}
    for archive_name in zip.namelist():
      zip_dict[archive_name] = zip.read(archive_name)
    zip.close()
    return zip_dict

  def ConvertDictToZip(self, dict):
    """Converts a dictionary to an in-memory zipfile.

    Arguments:
        dict: A dictionary mapping file names to file contents

    Returns:
        The new file's in-memory contents as a file-like object."""
    zipfile_mem = StringIO.StringIO()
    zip = zipfile.ZipFile(zipfile_mem, 'a')
    for arcname, contents in dict.items():
      zip.writestr(arcname, contents)
    zip.close()
    return zipfile_mem


class MemoryZipTestCase(TestCase):
  """Base for TestCase classes which read from an in-memory zip file.

  A test that loads data from this zip file exercises almost all the code used
  when the feedvalidator runs, but does not touch disk. Unfortunately it is very
  difficult to add new stops to the default stops.txt because a new stop will
  break tests in StopHierarchyTestCase and StopsNearEachOther."""

  def setUp(self):
    self.accumulator = RecordingProblemAccumulator(self, ("ExpirationDate",))
    self.problems = transitfeed.ProblemReporter(self.accumulator)
    self.zip_contents = {}
    self.SetArchiveContents(
        "agency.txt",
        "agency_id,agency_name,agency_url,agency_timezone\n"
        "DTA,Demo Agency,http://google.com,America/Los_Angeles\n")
    self.SetArchiveContents(
        "calendar.txt",
        "service_id,monday,tuesday,wednesday,thursday,friday,saturday,sunday,"
        "start_date,end_date\n"
        "FULLW,1,1,1,1,1,1,1,20070101,20101231\n"
        "WE,0,0,0,0,0,1,1,20070101,20101231\n")
    self.SetArchiveContents(
        "calendar_dates.txt",
        "service_id,date,exception_type\n"
        "FULLW,20070101,1\n")
    self.SetArchiveContents(
        "routes.txt",
        "route_id,agency_id,route_short_name,route_long_name,route_type\n"
        "AB,DTA,,Airport Bullfrog,3\n")
    self.SetArchiveContents(
        "trips.txt",
        "route_id,service_id,trip_id\n"
        "AB,FULLW,AB1\n")
    self.SetArchiveContents(
        "stops.txt",
        "stop_id,stop_name,stop_lat,stop_lon\n"
        "BEATTY_AIRPORT,Airport,36.868446,-116.784582\n"
        "BULLFROG,Bullfrog,36.88108,-116.81797\n"
        "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677\n")
    self.SetArchiveContents(
        "stop_times.txt",
        "trip_id,arrival_time,departure_time,stop_id,stop_sequence\n"
        "AB1,10:00:00,10:00:00,BEATTY_AIRPORT,1\n"
        "AB1,10:20:00,10:20:00,BULLFROG,2\n"
        "AB1,10:25:00,10:25:00,STAGECOACH,3\n")

  def MakeLoaderAndLoad(self,
                        problems=None,
                        extra_validation=True,
                        gtfs_factory=None):
    """Returns a Schedule loaded with the contents of the file dict."""

    if gtfs_factory is None:
      gtfs_factory = transitfeed.GetGtfsFactory()
    if problems is None:
      problems = self.problems
    self.CreateZip()
    self.loader = transitfeed.Loader(
        problems=problems,
        extra_validation=extra_validation,
        zip=self.zip,
        gtfs_factory=gtfs_factory)
    return self.loader.Load()

  def AppendToArchiveContents(self, arcname, s):
    """Append string s to file arcname in the file dict.

    All calls to this function, if any, should be made before calling
    MakeLoaderAndLoad."""
    current_contents = self.zip_contents[arcname]
    self.zip_contents[arcname] = current_contents + s

  def SetArchiveContents(self, arcname, contents):
    """Set the contents of file arcname in the file dict.

    All calls to this function, if any, should be made before calling
    MakeLoaderAndLoad."""
    self.zip_contents[arcname] = contents

  def GetArchiveContents(self, arcname):
    """Get the contents of file arcname in the file dict."""
    return self.zip_contents[arcname]

  def RemoveArchive(self, arcname):
    """Remove file arcname from the file dict.

    All calls to this function, if any, should be made before calling
    MakeLoaderAndLoad."""
    del self.zip_contents[arcname]

  def GetArchiveNames(self):
    """Get a list of all the archive names in the file dict."""
    return self.zip_contents.keys()

  def CreateZip(self):
    """Create an in-memory GTFS zipfile from the contents of the file dict."""
    self.zipfile = StringIO.StringIO()
    self.zip = zipfile.ZipFile(self.zipfile, 'a')
    for (arcname, contents) in self.zip_contents.items():
      self.zip.writestr(arcname, contents)

  def DumpZipFile(self, zf):
    """Print the contents of something zipfile can open, such as a StringIO."""
    # Handy for debugging
    z = zipfile.ZipFile(zf)
    for n in z.namelist():
      print "--\n%s\n%s" % (n, z.read(n))


#TODO(anog): Revisit this after we implement proper per-exception level change
class RecordingProblemAccumulator(transitfeed.ProblemAccumulatorInterface):
  """Save all problems for later inspection.

  Args:
    test_case: a unittest.TestCase object on which to report problems
    ignore_types: sequence of string type names that will be ignored by the
    ProblemAccumulator"""
  def __init__(self, test_case, ignore_types=None):
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

  def TearDownAssertNoMoreExceptions(self):
    """Assert that there are no unexpected problems left after a test has run.

       This function should be called on a test's tearDown. For more information
       please see AssertNoMoreExceptions"""
    assert len(self.exceptions) == 0, \
        "see util.RecordingProblemAccumulator.AssertNoMoreExceptions"

  def AssertNoMoreExceptions(self):
    """Check that no unexpected problems were reported.

    Every test that uses a RecordingProblemReporter should end with a call to
    this method. If setUp creates a RecordingProblemReporter it is good for
    tearDown to double check that the exceptions list was emptied.
    """
    exceptions_as_text = []
    for e, tb in self.exceptions:
      exceptions_as_text.append(self.FormatException(e, tb))
    # If the assertFalse below fails the test will abort and tearDown is
    # called. Some tearDown methods assert that self.exceptions is empty as
    # protection against a test that doesn't end with AssertNoMoreExceptions
    # and has exceptions remaining in the RecordingProblemReporter. It would
    # be nice to trigger a normal test failure in tearDown but the idea was
    # rejected (http://bugs.python.org/issue5531).
    self.exceptions = []
    self._test_case.assertFalse(exceptions_as_text,
                                "\n".join(exceptions_as_text))

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

  def PopDuplicateColumn(self, file_name, header, count):
    e = self.PopException("DuplicateColumn")
    self._test_case.assertEquals(file_name, e.file_name)
    self._test_case.assertEquals(header, e.header)
    self._test_case.assertEquals(count, e.count)
    return e
