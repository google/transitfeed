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

from __future__ import print_function
from __future__ import absolute_import
import codecs
import csv
import datetime
import math
import optparse
import random
import re
import socket
import sys
import time
import urllib2

from . import errors
from .version import __version__

# URL which identifies the latest release version of the transitfeed library.
LATEST_RELEASE_VERSION_URL = 'https://raw.githubusercontent.com/wiki/google/transitfeed/LatestReleaseVersion.md'


class OptionParserLongError(optparse.OptionParser):
  """OptionParser subclass that includes list of options above error message."""
  def error(self, msg):
    print(self.format_help(), file=sys.stderr)
    print('\n\n%s: error: %s\n\n' % (self.get_prog_name(), msg), file=sys.stderr)
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
https://github.com/google/transitfeed/issues
or an email to the public group transitfeed@googlegroups.com. Sorry!

"""
    dashes = '%s\n' % ('-' * 60)
    dump = []
    dump.append(apology)
    dump.append(dashes)
    try:
      import transitfeed
      dump.append("transitfeed version %s\n\n" % __version__)
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
        except Exception as e:
          dump.append('    Exception in str(%s): %s' % (local_name, e))
        else:
          if len(truncated_val) >= 500:
            truncated_val = '%s...' % truncated_val[0:499]
          dump.append('    %s = %s\n' % (local_name, truncated_val))
      dump.append('\n')

    dump.append(''.join(formatted_exception))

    open('transitfeedcrash.txt', 'w').write(''.join(dump))

    print(''.join(dump))
    print()
    print(dashes)
    print(apology)

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

def CheckVersion(problems, latest_version=None):
  """
  Check if there is a newer version of transitfeed available.

  Args:
    problems: if a new version is available, a NewVersionAvailable problem will
      be added
    latest_version: if specified, override the latest version read from the
      project page
  """
  if not latest_version:
    timeout = 20
    socket.setdefaulttimeout(timeout)
    request = urllib2.Request(LATEST_RELEASE_VERSION_URL)

    try:
      response = urllib2.urlopen(request)
      content = response.read()
      m = re.search(r'version=(\d+\.\d+\.\d+)', content)
      if m:
        latest_version = m.group(1)

    except urllib2.HTTPError as e:
      description = ('During the new-version check, we failed to reach '
                     'transitfeed server: Reason: %s [%s].' %
                     (e.reason, e.code))
      problems.OtherProblem(
        description=description, type=errors.TYPE_NOTICE)
      return
    except urllib2.URLError as e:
      description = ('During the new-version check, we failed to reach '
                     'transitfeed server. Reason: %s.' % e.reason)
      problems.OtherProblem(
        description=description, type=errors.TYPE_NOTICE)
      return

  if not latest_version:
    description = ('During the new-version check, we had trouble parsing the '
                   'contents of %s.' % LATEST_RELEASE_VERSION_URL)
    problems.OtherProblem(
      description=description, type=errors.TYPE_NOTICE)
    return

  newest_version = _MaxVersion([latest_version, __version__])
  if __version__ != newest_version:
    problems.NewVersionAvailable(newest_version)


def _MaxVersion(versions):
  versions = filter(None, versions)
  versions.sort(lambda x,y: -cmp([int(item) for item in x.split('.')],
                                 [int(item) for item in y.split('.')]))
  if len(versions) > 0:
    return versions[0]


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
  """
  Checks the validity of a URL value:
    - only checks whether the URL starts with 'http://' or 'https://'
  """
  # TODO: Add more thorough checking of URL
  return url.startswith(u'http://') or url.startswith(u'https://')

def ValidateURL(url, column_name=None, problems=None):
  """
  Validates a non-required URL value using IsValidURL():
    - if invalid adds InvalidValue error (if problems accumulator is provided)
    - an empty URL is considered valid and no error or warning is issued.
  """
  if IsEmpty(url) or IsValidURL(url):
    return True
  else:
    if problems:
      problems.InvalidValue(column_name, url)
    return False

def ValidateEmail(email, column_name=None, problems=None):
  """
  checks the basic validity of email:
    - an empty email is considered valid and no error or warning is issued.
    - should start with any string not including @
    - then should match a single @
    - then matches any string not including @
    - then contains a single dot
    - then again matches any string after dot.
  """
  if IsEmpty(email) or re.match(r'[^@]+@[^@]+\.[^@]+', email):
    return True
  else:
    if problems:
      problems.InvalidValue(column_name, email)
    return False

def IsValidHexColor(color):
  """
  Checks the validity of a hex color value:
    - the color string must consist of 6 hexadecimal digits
  """
  return not re.match('^[0-9a-fA-F]{6}$', color) == None

def IsValidLanguageCode(lang):
  """
  Checks the validity of a language code value:
    - checks whether the code, as lower case, is in the ISO639 codes list
  """
  return lang.lower() in ISO639.codes_2letter

def ValidateLanguageCode(lang, column_name=None, problems=None):
  """
  Validates a non-required language code value using IsValidLanguageCode():
    - if invalid adds InvalidValue error (if problems accumulator is provided)
    - an empty language code is regarded as valid! Otherwise we might end up
      with many duplicate errors because of the required field checks.
  """
  if IsEmpty(lang) or IsValidLanguageCode(lang):
    return True
  else:
    if problems:
      problems.InvalidValue(column_name, lang)
    return False

def IsValidTimezone(timezone):
  """
  Checks the validity of a timezone string value:
    - checks whether the timezone is in the pytz common_timezones list
    - assumes the timezone to be valid if the pytz module is not available
  """
  try:
    import pytz
    return timezone in pytz.common_timezones
  except ImportError:  # no pytz
    print ("Timezone not checked "
           "(install pytz package for timezone validation)")
    return True

def ValidateTimezone(timezone, column_name=None, problems=None):
  """
  Validates a non-required timezone string value using IsValidTimezone():
    - if invalid adds InvalidValue error (if problems accumulator is provided)
    - an empty timezone string is regarded as valid! Otherwise we might end up
      with many duplicate errors because of the required field checks.
  """
  if IsEmpty(timezone) or IsValidTimezone(timezone):
    return True
  else:
    if problems:
      # if we get here pytz has already been imported successfully in
      # IsValidTimezone(). So a try-except block is not needed here.
      import pytz
      problems.InvalidValue(
          column_name, timezone,
          '"%s" is not a common timezone name according to pytz version %s' %
          (timezone, pytz.VERSION))
    return False

def IsValidDate(date):
  """
  Checks the validity of a date string value:
    - checks whether the date string consists of 8 digits in the form "YYYYMMDD"
    - checks whether the date string can be converted to a valid datetime object
  """
  return DateStringToDateObject(date) is not None

def ValidateDate(date, column_name=None, problems=None):
  """
  Validates a non-required date string value using IsValidDate():
    - if invalid adds InvalidValue error (if problems accumulator is provided)
    - an empty date string is regarded as valid! Otherwise we might end up
      with many duplicate errors because of the required field checks.
  """
  if IsEmpty(date) or IsValidDate(date):
    return True
  else:
    if problems:
      problems.InvalidValue(column_name, date)
    return False

def ValidateRequiredFieldsAreNotEmpty(gtfs_object, required_field_names,
                                      problems=None):
  """
  Validates whether all required fields of an object have a value:
    - if value empty adds MissingValue errors (if problems accumulator is
      provided)
  """
  no_missing_value = True
  for name in required_field_names:
    if IsEmpty(getattr(gtfs_object, name, None)):
      if problems:
        problems.MissingValue(name)
      no_missing_value = False
  return no_missing_value

# TODO(neun): use it everywhere it makes sense (stop_times, transfers ...)
def ValidateAndReturnIntValue(value, allowed_values, default, allow_empty,
                              column_name=None, problems=None):
  """
  Validates a value to be a valid integer in the list of allowed values:
    - if no integer value adds InvalidValue error and returns the default value
    - if integer but not in allowed_values addes InvalidValue warning
    - returns the default value if empty and allow_empty = True

  Args:
    value: a string or integer value
    allowed_values: a list of allowed integer values
    default: a default value for invalid string vlaues
    allow_empty: a bool indicating whether an empty value is allowed and
                 defaults to the default value
    column_name: the column name the value comes from
    problems: the problems accumulator

  Returns:
    An integer value.
  """
  if allow_empty and IsEmpty(value):
    return default
  try:
    int_value = int(value)
  except (ValueError, TypeError):
    if problems and column_name:
      problems.InvalidValue(column_name, value)
    return default
  else:
    if int_value not in allowed_values:
      if problems and column_name:
        problems.InvalidValue(column_name, value,
                              type=errors.TYPE_WARNING)
    return int_value

def ColorLuminance(color):
  """Compute the brightness of an sRGB color using the formula from
  http://www.w3.org/TR/2000/WD-AERT-20000426#color-contrast.

  Args:
    color: a string of 6 hex digits in the format verified by IsValidHexColor().

  Returns:
    A floating-point number between 0.0 (black) and 255.0 (white). """
  r = int(color[0:2], 16)
  g = int(color[2:4], 16)
  b = int(color[4:6], 16)
  return (299*r + 587*g + 114*b) / 1000.0

def IsValidYesNoUnknown(value):
  return value in ['0', '1', '2'];

def ValidateYesNoUnknown(value, column_name=None, problems=None):
  """Validates a value "0" for uknown, "1" for yes, and "2" for no."""
  if IsEmpty(value) or IsValidYesNoUnknown(value):
    return True
  else:
    if problems:
      problems.InvalidValue(column_name, value)
    return False

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
    raise errors.Error('Bad HH:MM:SS "%s"' % time_string)
  return int(m.group(1)) * 3600 + int(m.group(2)) * 60 + int(m.group(3))

def FormatSecondsSinceMidnight(s):
  """Formats an int number of seconds past midnight into a string
  as "HH:MM:SS"."""
  return "%02d:%02d:%02d" % (s / 3600, (s / 60) % 60, s % 60)

def DateStringToDateObject(date_string):
  """Return a date object for a string "YYYYMMDD"."""
  # If this becomes a bottleneck date objects could be cached
  if re.match('^\d{8}$', date_string) == None:
    return None
  try:
    return datetime.date(int(date_string[0:4]), int(date_string[4:6]),
                         int(date_string[6:8]))
  except ValueError:
    return None

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
  if (stop1.stop_lat is None or stop1.stop_lon is None or
      stop2.stop_lat is None or stop2.stop_lon is None):
    return None
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
    except Exception as e:
      print('error writing %s as %s' % (row, encoded_row))
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
      next_line = next(self._f)
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
        next(self._f)
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


class ISO639(object):
  # Set of all the 2-letter ISO 639-1 language codes.
  codes_2letter = set([
    'aa', 'ab', 'ae', 'af', 'ak', 'am', 'an', 'ar', 'as', 'av', 'ay', 'az',
    'ba', 'be', 'bg', 'bh', 'bi', 'bm', 'bn', 'bo', 'br', 'bs', 'ca', 'ce',
    'ch', 'co', 'cr', 'cs', 'cu', 'cv', 'cy', 'da', 'de', 'dv', 'dz', 'ee',
    'el', 'en', 'eo', 'es', 'et', 'eu', 'fa', 'ff', 'fi', 'fj', 'fo', 'fr',
    'fy', 'ga', 'gd', 'gl', 'gn', 'gu', 'gv', 'ha', 'he', 'hi', 'ho', 'hr',
    'ht', 'hu', 'hy', 'hz', 'ia', 'id', 'ie', 'ig', 'ii', 'ik', 'io', 'is',
    'it', 'iu', 'ja', 'jv', 'ka', 'kg', 'ki', 'kj', 'kk', 'kl', 'km', 'kn',
    'ko', 'kr', 'ks', 'ku', 'kv', 'kw', 'ky', 'la', 'lb', 'lg', 'li', 'ln',
    'lo', 'lt', 'lu', 'lv', 'mg', 'mh', 'mi', 'mk', 'ml', 'mn', 'mo', 'mr',
    'ms', 'mt', 'my', 'na', 'nb', 'nd', 'ne', 'ng', 'nl', 'nn', 'no', 'nr',
    'nv', 'ny', 'oc', 'oj', 'om', 'or', 'os', 'pa', 'pi', 'pl', 'ps', 'pt',
    'qu', 'rm', 'rn', 'ro', 'ru', 'rw', 'sa', 'sc', 'sd', 'se', 'sg', 'si',
    'sk', 'sl', 'sm', 'sn', 'so', 'sq', 'sr', 'ss', 'st', 'su', 'sv', 'sw',
    'ta', 'te', 'tg', 'th', 'ti', 'tk', 'tl', 'tn', 'to', 'tr', 'ts', 'tt',
    'tw', 'ty', 'ug', 'uk', 'ur', 'uz', 've', 'vi', 'vo', 'wa', 'wo', 'xh',
    'yi', 'yo', 'za', 'zh', 'zu',
  ])


class ISO4217(object):
  """Represents the set of currencies recognized by the ISO-4217 spec."""
  codes = {  # map of alpha code to numerical code
    'AED': 784, 'AFN': 971, 'ALL':   8, 'AMD':  51, 'ANG': 532, 'AOA': 973,
    'ARS':  32, 'AUD':  36, 'AWG': 533, 'AZN': 944, 'BAM': 977, 'BBD':  52,
    'BDT':  50, 'BGN': 975, 'BHD':  48, 'BIF': 108, 'BMD':  60, 'BND':  96,
    'BOB':  68, 'BOV': 984, 'BRL': 986, 'BSD':  44, 'BTN':  64, 'BWP':  72,
    'BYR': 974, 'BZD':  84, 'CAD': 124, 'CDF': 976, 'CHE': 947, 'CHF': 756,
    'CHW': 948, 'CLF': 990, 'CLP': 152, 'CNY': 156, 'COP': 170, 'COU': 970,
    'CRC': 188, 'CUP': 192, 'CVE': 132, 'CYP': 196, 'CZK': 203, 'DJF': 262,
    'DKK': 208, 'DOP': 214, 'DZD':  12, 'EEK': 233, 'EGP': 818, 'ERN': 232,
    'ETB': 230, 'EUR': 978, 'FJD': 242, 'FKP': 238, 'GBP': 826, 'GEL': 981,
    'GHC': 288, 'GIP': 292, 'GMD': 270, 'GNF': 324, 'GTQ': 320, 'GYD': 328,
    'HKD': 344, 'HNL': 340, 'HRK': 191, 'HTG': 332, 'HUF': 348, 'IDR': 360,
    'ILS': 376, 'INR': 356, 'IQD': 368, 'IRR': 364, 'ISK': 352, 'JMD': 388,
    'JOD': 400, 'JPY': 392, 'KES': 404, 'KGS': 417, 'KHR': 116, 'KMF': 174,
    'KPW': 408, 'KRW': 410, 'KWD': 414, 'KYD': 136, 'KZT': 398, 'LAK': 418,
    'LBP': 422, 'LKR': 144, 'LRD': 430, 'LSL': 426, 'LTL': 440, 'LVL': 428,
    'LYD': 434, 'MAD': 504, 'MDL': 498, 'MGA': 969, 'MKD': 807, 'MMK': 104,
    'MNT': 496, 'MOP': 446, 'MRO': 478, 'MTL': 470, 'MUR': 480, 'MVR': 462,
    'MWK': 454, 'MXN': 484, 'MXV': 979, 'MYR': 458, 'MZN': 943, 'NAD': 516,
    'NGN': 566, 'NIO': 558, 'NOK': 578, 'NPR': 524, 'NZD': 554, 'OMR': 512,
    'PAB': 590, 'PEN': 604, 'PGK': 598, 'PHP': 608, 'PKR': 586, 'PLN': 985,
    'PYG': 600, 'QAR': 634, 'ROL': 642, 'RON': 946, 'RSD': 941, 'RUB': 643,
    'RWF': 646, 'SAR': 682, 'SBD':  90, 'SCR': 690, 'SDD': 736, 'SDG': 938,
    'SEK': 752, 'SGD': 702, 'SHP': 654, 'SKK': 703, 'SLL': 694, 'SOS': 706,
    'SRD': 968, 'STD': 678, 'SYP': 760, 'SZL': 748, 'THB': 764, 'TJS': 972,
    'TMM': 795, 'TND': 788, 'TOP': 776, 'TRY': 949, 'TTD': 780, 'TWD': 901,
    'TZS': 834, 'UAH': 980, 'UGX': 800, 'USD': 840, 'USN': 997, 'USS': 998,
    'UYU': 858, 'UZS': 860, 'VEB': 862, 'VND': 704, 'VUV': 548, 'WST': 882,
    'XAF': 950, 'XAG': 961, 'XAU': 959, 'XBA': 955, 'XBB': 956, 'XBC': 957,
    'XBD': 958, 'XCD': 951, 'XDR': 960, 'XFO': None, 'XFU': None, 'XOF': 952,
    'XPD': 964, 'XPF': 953, 'XPT': 962, 'XTS': 963, 'XXX': 999, 'YER': 886,
    'ZAR': 710, 'ZMK': 894, 'ZWD': 716,
  }

