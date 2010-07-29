#!/usr/bin/python2.5

# Copyright (C) 2009 Google Inc.
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

import codecs
import csv
import datetime
import math
import optparse
import random
import re
import sys

import problems
from trip import Trip

class OptionParserLongError(optparse.OptionParser):
  """OptionParser subclass that includes list of options above error message."""
  def error(self, msg):
    print >>sys.stderr, self.format_help()
    print >>sys.stderr, '\n\n%s: error: %s\n\n' % (self.get_prog_name(), msg)
    sys.exit(2)


def RunWithCrashHandler(f):
  try:
    exit_code = f()
    sys.exit(exit_code)
  except (SystemExit, KeyboardInterrupt):
    raise
  except:
    import inspect
    import traceback

    # Save trace and exception now. These calls look at the most recently
    # raised exception. The code that makes the report might trigger other
    # exceptions.
    original_trace = inspect.trace(3)[1:]
    formatted_exception = traceback.format_exception_only(*(sys.exc_info()[:2]))

    apology = """Yikes, the program threw an unexpected exception!

Hopefully a complete report has been saved to transitfeedcrash.txt,
though if you are seeing this message we've already disappointed you once
today. Please include the report in a new issue at
http://code.google.com/p/googletransitdatafeed/issues/entry
or an email to the public group googletransitdatafeed@googlegroups.com. Sorry!

"""
    dashes = '%s\n' % ('-' * 60)
    dump = []
    dump.append(apology)
    dump.append(dashes)
    try:
      import transitfeed
      dump.append("transitfeed version %s\n\n" % transitfeed.__version__)
    except NameError:
      # Oh well, guess we won't put the version in the report
      pass

    for (frame_obj, filename, line_num, fun_name, context_lines,
         context_index) in original_trace:
      dump.append('File "%s", line %d, in %s\n' % (filename, line_num,
                                                   fun_name))
      if context_lines:
        for (i, line) in enumerate(context_lines):
          if i == context_index:
            dump.append(' --> %s' % line)
          else:
            dump.append('     %s' % line)
      for local_name, local_val in frame_obj.f_locals.items():
        try:
          truncated_val = str(local_val)[0:500]
        except Exception, e:
          dump.append('    Exception in str(%s): %s' % (local_name, e))
        else:
          if len(truncated_val) >= 500:
            truncated_val = '%s...' % truncated_val[0:499]
          dump.append('    %s = %s\n' % (local_name, truncated_val))
      dump.append('\n')

    dump.append(''.join(formatted_exception))

    open('transitfeedcrash.txt', 'w').write(''.join(dump))

    print ''.join(dump)
    print
    print dashes
    print apology

    try:
      raw_input('Press enter to continue...')
    except EOFError:
      # Ignore stdin being closed. This happens during some tests.
      pass
    sys.exit(127)


# Pick one of two defaultdict implementations. A native version was added to
# the collections library in python 2.5. If that is not available use Jason's
# pure python recipe. He gave us permission to distribute it.

# On Mon, Nov 30, 2009 at 07:27, jason kirtland <jek at discorporate.us> wrote:
# >
# > Hi Tom, sure thing!  It's not easy to find on the cookbook site, but the
# > recipe is under the Python license.
# >
# > Cheers,
# > Jason
# >
# > On Thu, Nov 26, 2009 at 3:03 PM, Tom Brown <tom.brown.code@gmail.com> wrote:
# >
# >> I would like to include http://code.activestate.com/recipes/523034/ in
# >> http://code.google.com/p/googletransitdatafeed/wiki/TransitFeedDistribution
# >> which is distributed under the Apache License, Version 2.0 with Copyright
# >> Google. May we include your code with a comment in the source pointing at
# >> the original URL?  Thanks, Tom Brown

try:
  # Try the native implementation first
  from collections import defaultdict
except:
  # Fallback for python2.4, which didn't include collections.defaultdict
  class defaultdict(dict):
    def __init__(self, default_factory=None, *a, **kw):
      if (default_factory is not None and
        not hasattr(default_factory, '__call__')):
        raise TypeError('first argument must be callable')
      dict.__init__(self, *a, **kw)
      self.default_factory = default_factory
    def __getitem__(self, key):
      try:
        return dict.__getitem__(self, key)
      except KeyError:
        return self.__missing__(key)
    def __missing__(self, key):
      if self.default_factory is None:
        raise KeyError(key)
      self[key] = value = self.default_factory()
      return value
    def __reduce__(self):
      if self.default_factory is None:
        args = tuple()
      else:
        args = self.default_factory,
      return type(self), args, None, None, self.items()
    def copy(self):
      return self.__copy__()
    def __copy__(self):
      return type(self)(self.default_factory, self)
    def __deepcopy__(self, memo):
      import copy
      return type(self)(self.default_factory,
                        copy.deepcopy(self.items()))
    def __repr__(self):
      return 'defaultdict(%s, %s)' % (self.default_factory,
                                      dict.__repr__(self))



OUTPUT_ENCODING = 'utf-8'

def EncodeUnicode(text):
  """
  Optionally encode text and return it. The result should be safe to print.
  """
  if type(text) == type(u''):
    return text.encode(OUTPUT_ENCODING)
  else:
    return text

def IsValidURL(url):
  """Checks the validity of a URL value."""
  # TODO: Add more thorough checking of URL
  return url.startswith(u'http://') or url.startswith(u'https://')


def IsValidColor(color):
  """Checks the validity of a hex color value."""
  return not re.match('^[0-9a-fA-F]{6}$', color) == None


def ColorLuminance(color):
  """Compute the brightness of an sRGB color using the formula from
  http://www.w3.org/TR/2000/WD-AERT-20000426#color-contrast.

  Args:
    color: a string of six hex digits in the format verified by IsValidColor().

  Returns:
    A floating-point number between 0.0 (black) and 255.0 (white). """
  r = int(color[0:2], 16)
  g = int(color[2:4], 16)
  b = int(color[4:6], 16)
  return (299*r + 587*g + 114*b) / 1000.0


def IsEmpty(value):
  return value is None or (isinstance(value, basestring) and not value.strip())


def FindUniqueId(dic):
  """Return a string not used as a key in the dictionary dic"""
  name = str(len(dic))
  while name in dic:
    # Use bigger numbers so it is obvious when an id is picked randomly.
    name = str(random.randint(1000000, 999999999))
  return name


def TimeToSecondsSinceMidnight(time_string):
  """Convert HHH:MM:SS into seconds since midnight.

  For example "01:02:03" returns 3723. The leading zero of the hours may be
  omitted. HH may be more than 23 if the time is on the following day."""
  m = re.match(r'(\d{1,3}):([0-5]\d):([0-5]\d)$', time_string)
  # ignored: matching for leap seconds
  if not m:
    raise problems.Error, 'Bad HH:MM:SS "%s"' % time_string
  return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))


def FormatSecondsSinceMidnight(s):
  """Formats an int number of seconds past midnight into a string
  as "HH:MM:SS"."""
  return "%02d:%02d:%02d" % (s / 3600, (s / 60) % 60, s % 60)


def DateStringToDateObject(date_string):
  """Return a date object for a string "YYYYMMDD"."""
  # If this becomes a bottleneck date objects could be cached
  return datetime.date(int(date_string[0:4]), int(date_string[4:6]),
                       int(date_string[6:8]))


def FloatStringToFloat(float_string, problems=None):
  """Convert a float as a string to a float or raise an exception"""
  # Will raise TypeError unless a string
  match = re.match(r"^[+-]?\d+(\.\d+)?$", float_string)
  # Will raise TypeError if the string can't be parsed
  parsed_value = float(float_string)

  if "x" in float_string:
    # This is needed because Python 2.4 does not complain about float("0x20").
    # But it does complain about float("0b10"), so this should be enough.
    raise ValueError()

  if not match and problems is not None:
    # Does not match the regex, but it's a float according to Python
    problems.InvalidFloatValue(float_string)
  return parsed_value

def NonNegIntStringToInt(int_string, problems=None):
  """Convert an non-negative integer string to an int or raise an exception"""
  # Will raise TypeError unless a string
  match = re.match(r"^(?:0|[1-9]\d*)$", int_string)
  # Will raise ValueError if the string can't be parsed
  parsed_value = int(int_string)

  if parsed_value < 0:
    raise ValueError()
  elif not match and problems is not None:
    # Does not match the regex, but it's an int according to Python
    problems.InvalidNonNegativeIntegerValue(int_string)

  return parsed_value

EARTH_RADIUS = 6378135          # in meters
def ApproximateDistance(degree_lat1, degree_lng1, degree_lat2, degree_lng2):
  """Compute approximate distance between two points in meters. Assumes the
  Earth is a sphere."""
  # TODO: change to ellipsoid approximation, such as
  # http://www.codeguru.com/Cpp/Cpp/algorithms/article.php/c5115/
  lat1 = math.radians(degree_lat1)
  lng1 = math.radians(degree_lng1)
  lat2 = math.radians(degree_lat2)
  lng2 = math.radians(degree_lng2)
  dlat = math.sin(0.5 * (lat2 - lat1))
  dlng = math.sin(0.5 * (lng2 - lng1))
  x = dlat * dlat + dlng * dlng * math.cos(lat1) * math.cos(lat2)
  return EARTH_RADIUS * (2 * math.atan2(math.sqrt(x),
      math.sqrt(max(0.0, 1.0 - x))))


def ApproximateDistanceBetweenStops(stop1, stop2):
  """Compute approximate distance between two stops in meters. Assumes the
  Earth is a sphere."""
  return ApproximateDistance(stop1.stop_lat, stop1.stop_lon,
                             stop2.stop_lat, stop2.stop_lon)

class CsvUnicodeWriter:
  """
  Create a wrapper around a csv writer object which can safely write unicode
  values. Passes all arguments to csv.writer.
  """
  def __init__(self, *args, **kwargs):
    self.writer = csv.writer(*args, **kwargs)

  def writerow(self, row):
    """Write row to the csv file. Any unicode strings in row are encoded as
    utf-8."""
    encoded_row = []
    for s in row:
      if isinstance(s, unicode):
        encoded_row.append(s.encode("utf-8"))
      else:
        encoded_row.append(s)
    try:
      self.writer.writerow(encoded_row)
    except Exception, e:
      print 'error writing %s as %s' % (row, encoded_row)
      raise e

  def writerows(self, rows):
    """Write rows to the csv file. Any unicode strings in rows are encoded as
    utf-8."""
    for row in rows:
      self.writerow(row)

  def __getattr__(self, name):
    return getattr(self.writer, name)

# Map from literal string that should never be found in the csv data to a human
# readable description
INVALID_LINE_SEPARATOR_UTF8 = {
    "\x0c": "ASCII Form Feed 0x0C",
    # May be part of end of line, but not found elsewhere
    "\x0d": "ASCII Carriage Return 0x0D, \\r",
    "\xe2\x80\xa8": "Unicode LINE SEPARATOR U+2028",
    "\xe2\x80\xa9": "Unicode PARAGRAPH SEPARATOR U+2029",
    "\xc2\x85": "Unicode NEXT LINE SEPARATOR U+0085",
}

class EndOfLineChecker:
  """Wrapper for a file-like object that checks for consistent line ends.

  The check for consistent end of lines (all CR LF or all LF) only happens if
  next() is called until it raises StopIteration.
  """
  def __init__(self, f, name, problems):
    """Create new object.

    Args:
      f: file-like object to wrap
      name: name to use for f. StringIO objects don't have a name attribute.
      problems: a ProblemReporterBase object
    """
    self._f = f
    self._name = name
    self._crlf = 0
    self._crlf_examples = []
    self._lf = 0
    self._lf_examples = []
    self._line_number = 0  # first line will be number 1
    self._problems = problems

  def __iter__(self):
    return self

  def next(self):
    """Return next line without end of line marker or raise StopIteration."""
    try:
      next_line = self._f.next()
    except StopIteration:
      self._FinalCheck()
      raise

    self._line_number += 1
    m_eol = re.search(r"[\x0a\x0d]*$", next_line)
    if m_eol.group() == "\x0d\x0a":
      self._crlf += 1
      if self._crlf <= 5:
        self._crlf_examples.append(self._line_number)
    elif m_eol.group() == "\x0a":
      self._lf += 1
      if self._lf <= 5:
        self._lf_examples.append(self._line_number)
    elif m_eol.group() == "":
      # Should only happen at the end of the file
      try:
        self._f.next()
        raise RuntimeError("Unexpected row without new line sequence")
      except StopIteration:
        # Will be raised again when EndOfLineChecker.next() is next called
        pass
    else:
      self._problems.InvalidLineEnd(
        codecs.getencoder('string_escape')(m_eol.group())[0],
        (self._name, self._line_number))
    next_line_contents = next_line[0:m_eol.start()]
    for seq, name in INVALID_LINE_SEPARATOR_UTF8.items():
      if next_line_contents.find(seq) != -1:
        self._problems.OtherProblem(
          "Line contains %s" % name,
          context=(self._name, self._line_number))
    return next_line_contents

  def _FinalCheck(self):
    if self._crlf > 0 and self._lf > 0:
      crlf_plural = self._crlf > 1 and "s" or ""
      crlf_lines = ", ".join(["%s" % e for e in self._crlf_examples])
      if self._crlf > len(self._crlf_examples):
        crlf_lines += ", ..."
      lf_plural = self._lf > 1 and "s" or ""
      lf_lines = ", ".join(["%s" % e for e in self._lf_examples])
      if self._lf > len(self._lf_examples):
        lf_lines += ", ..."

      self._problems.OtherProblem(
          "Found %d CR LF \"\\r\\n\" line end%s (line%s %s) and "
          "%d LF \"\\n\" line end%s (line%s %s). A file must use a "
          "consistent line end." % (self._crlf, crlf_plural, crlf_plural,
                                   crlf_lines, self._lf, lf_plural,
                                   lf_plural, lf_lines),
          (self._name,))
      # Prevent _FinalCheck() from reporting the problem twice, in the unlikely
      # case that it is run twice
      self._crlf = 0
      self._lf = 0

def SortListOfTripByTime(trips):
  trips.sort(key=Trip.GetStartTime)
