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

# Validates a Google Transit Feed Specification feed.
#
#
# usage: feedvalidator.py [options] feed_filename
#
# options:
#   --version             show program's version number and exit
#   -h, --help            show this help message and exit
#   -n, --noprompt        do not prompt for feed location or load output in
#                         browser
#   -o FILE, --output=FILE
#                         write html output to FILE

import codecs
from collections import defaultdict
import optparse
import os
import os.path
import re
import time
import transitfeed
from transitfeed import TYPE_ERROR, TYPE_WARNING
import sys
import webbrowser


def MaybePluralizeWord(count, word):
  if count == 1:
    return word
  else:
    return word + 's'


def PrettyNumberWord(count, word):
  return '%d %s' % (count, MaybePluralizeWord(count, word))


def UnCamelCase(camel):
  return re.sub(r'([a-z])([A-Z])', r'\1 \2', camel)


def ProblemCountText(error_count, warning_count):
  results = []
  if error_count:
    results.append(PrettyNumberWord(error_count, 'error'))
  if warning_count:
    results.append(PrettyNumberWord(warning_count, 'warning'))

  return ' and '.join(results)


class ErrorSet(object):
  """A collection error exceptions of one type with bounded size."""
  def __init__(self, set_size_bound):
    self._count = 0
    self._exceptions = []
    self._set_size_bound = set_size_bound

  def Add(self, error):
    self._count += 1
    if self._count <= self._set_size_bound:
      self._exceptions.append(error)

  def _GetDroppedCount(self):
    return max(self._count - len(self._exceptions), 0)

  count = property(lambda s: s._count)
  dropped_count = property(_GetDroppedCount)
  errors = property(lambda s: s._exceptions)


class HTMLCountingProblemReporter(transitfeed.ProblemReporter):
  def __init__(self, limit_per_type):
    transitfeed.ProblemReporter.__init__(self)
    self.unused_stops = []  # [(stop_id, stop_name)...]

    # {TYPE_WARNING: {"ClassName": ErrorSet()}}
    self._type_to_name_to_errorset = {
      TYPE_WARNING: defaultdict(lambda: ErrorSet(limit_per_type)),
      TYPE_ERROR: defaultdict(lambda: ErrorSet(limit_per_type))
    }

  def HasIssues(self):
    return (self._type_to_name_to_errorset[TYPE_ERROR] or
            self._type_to_name_to_errorset[TYPE_WARNING])

  def _Report(self, e):
    self._type_to_name_to_errorset[e.GetType()][e.__class__.__name__].Add(e)

  def FormatType(self, f, level_name, class_errorsets):
    """Write the HTML dumping all problems of one type.

    Args:
      f: file object open for writing
      level_name: string such as "Error" or "Warning"
      class_errorsets: sequence of tuples (class name, ErrorSet object)

    Returns:
      None
    """
    class_errorsets.sort()
    output = []
    for classname, errorset in class_errorsets:
      output.append('<h4 class="issueHeader"><a name="%s%s">%s</a></h4><ul>\n' %
                    (level_name, classname, UnCamelCase(classname)))
      for e in errorset.errors:
        self.FormatException(e, output)
      if errorset.dropped_count:
        output.append('<li>and %d more of this type.' %
                      (errorset.dropped_count))
      output.append('</ul>\n')
    f.write(''.join(output))

  def FormatTypeSummaryTable(self, level_name, name_to_errorset):
    """Return an HTML table listing the number of errors by class name.

    Args:
      level_name: string such as "Error" or "Warning"
      name_to_errorset: dict mapping class name to an ErrorSet object

    Returns:
      HTML in a string
    """
    output = []
    output.append('<table>')
    for classname in sorted(name_to_errorset.keys()):
      errorset = name_to_errorset[classname]
      human_name = MaybePluralizeWord(errorset.count, UnCamelCase(classname))
      output.append('<tr><td>%d</td><td><a href="#%s%s">%s</a></td></tr>\n' %
                    (errorset.count, level_name, classname, human_name))
    output.append('</table>\n')
    return ''.join(output)

  def FormatException(self, e, output):
    """Append HTML version of e to list output."""
    d = e.GetDictToFormat()
    for k in ('file_name', 'feedname', 'column_name'):
      if k in d.keys():
        d[k] = '<code>%s</code>' % d[k]
    problem_text = e.FormatProblem(d).replace('\n', '<br>')
    output.append('<li>')
    output.append('<div class="problem">%s</div>' %
                  transitfeed.EncodeUnicode(problem_text))
    try:
      output.append('in line %d of <code>%s</code><br>\n' %
                    (e.row_num, e.file_name))
      row = e.row
      headers = e.headers
      column_name = e.column_name
      table_header = ''  # HTML
      table_data = ''  # HTML
      for header, value in zip(headers, row):
        attributes = ''
        if header == column_name:
          attributes = ' class="problem"'
        table_header += '<th%s>%s</th>' % (attributes, header)
        table_data += '<td%s>%s</td>' % (attributes, value)
      # Make sure output is encoded into UTF-8
      output.append('<table class="dump"><tr>%s</tr>\n' %
                    transitfeed.EncodeUnicode(table_header))
      output.append('<tr>%s</tr></table>\n' %
                    transitfeed.EncodeUnicode(table_data))
    except AttributeError, e:
      pass  # Hope this was getting an attribute from e ;-)
    output.append('<br></li>\n')

  def ErrorCount(self):
    error_sets = self._type_to_name_to_errorset[TYPE_ERROR].values()
    return sum(map(lambda v: v.count, error_sets))

  def WarningCount(self):
    warning_sets = self._type_to_name_to_errorset[TYPE_WARNING].values()
    return sum(map(lambda v: v.count, warning_sets))

  def FormatCount(self):
    return ProblemCountText(self.ErrorCount(), self.WarningCount())

  def CountTable(self):
    output = []
    output.append('<table class="count_outside">\n')
    output.append('<tr>')
    if self._type_to_name_to_errorset[TYPE_ERROR]:
      output.append('<td><span class="fail">%s</span></td>' %
                    PrettyNumberWord(self.ErrorCount(), "error"))
    if self._type_to_name_to_errorset[TYPE_WARNING]:
      output.append('<td><span class="fail">%s</span></td>' %
                    PrettyNumberWord(self.WarningCount(), "warning"))
    output.append('</tr>\n<tr>')
    if self._type_to_name_to_errorset[TYPE_ERROR]:
      output.append('<td>\n')
      output.append(self.FormatTypeSummaryTable("Error",
                    self._type_to_name_to_errorset[TYPE_ERROR]))
      output.append('</td>\n')
    if self._type_to_name_to_errorset[TYPE_WARNING]:
      output.append('<td>\n')
      output.append(self.FormatTypeSummaryTable("Warning",
                    self._type_to_name_to_errorset[TYPE_WARNING]))
      output.append('</td>\n')
    output.append('</table>')
    return ''.join(output)

  def WriteOutput(self, feed_location, f, schedule, problems):
    """Write the html output to f."""
    if problems.HasIssues():
      if self.ErrorCount() + self.WarningCount() == 1:
        summary = ('<span class="fail">Found this problem:</span>\n%s' %
                   self.CountTable())
      else:
        summary = ('<span class="fail">Found these problems:</span>\n%s' %
                   self.CountTable())
    else:
      summary = '<span class="pass">feed validated successfully</span>'

    basename = os.path.basename(feed_location)
    feed_path = (feed_location[:feed_location.rfind(basename)], basename)

    agencies = ', '.join(['<a href="%s">%s</a>' % (a.agency_url, a.agency_name)
                          for a in schedule.GetAgencyList()])
    if not agencies:
      agencies = '?'

    dates = "No valid service dates found"
    (start, end) = schedule.GetDateRange()
    if start and end:
      def FormatDate(yyyymmdd):
        src_format = "%Y%m%d"
        dst_format = "%B %d, %Y"
        try:
          return time.strftime(dst_format,
                               time.strptime(yyyymmdd, src_format))
        except ValueError:
          return yyyymmdd

      formatted_start = FormatDate(start)
      formatted_end = FormatDate(end)
      dates = "%s to %s" % (formatted_start, formatted_end)

    output_prefix = """
<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
<title>FeedValidator: %(feed_file)s</title>
<style>
body {font-family: Georgia, serif; background-color: white}
.path {color: gray}
div.problem {max-width: 500px}
table.dump td,th {background-color: khaki; padding: 2px; font-family:monospace}
table.dump td.problem,th.problem {background-color: dc143c; color: white; padding: 2px; font-family:monospace}
table.count_outside td {vertical-align: top}
table.count_outside {border-spacing: 0px; }
table {border-spacing: 5px 0px; margin-top: 3px}
h3.issueHeader {padding-left: 0.5em}
h4.issueHeader {padding-left: 1em}
.pass {background-color: lightgreen}
.fail {background-color: yellow}
.pass, .fail {font-size: 16pt}
.header {background-color: white; font-family: Georgia, serif; padding: 0px}
th.header {text-align: right; font-weight: normal; color: gray}
.footer {font-size: 10pt}
</style>
</head>
<body>
GTFS validation results for feed:<br>
<code><span class="path">%(feed_dir)s</span><b>%(feed_file)s</b></code>
<br><br>
<table>
<tr><th class="header">Agencies:</th><td class="header">%(agencies)s</td></tr>
<tr><th class="header">Routes:</th><td class="header">%(routes)s</td></tr>
<tr><th class="header">Stops:</th><td class="header">%(stops)s</td></tr>
<tr><th class="header">Trips:</th><td class="header">%(trips)s</td></tr>
<tr><th class="header">Shapes:</th><td class="header">%(shapes)s</td></tr>
<tr><th class="header">Effective:</th><td class="header">%(dates)s</td></tr>
</table>
<br>
%(summary)s
<br><br>
""" % { "feed_file": feed_path[1],
            "feed_dir": feed_path[0],
            "agencies": agencies,
            "routes": len(schedule.GetRouteList()),
            "stops": len(schedule.GetStopList()),
            "trips": len(schedule.GetTripList()),
            "shapes": len(schedule.GetShapeList()),
            "dates": dates,
            "summary": summary }

# In output_suffix string
# time.strftime() returns a regular local time string (not a Unicode one) with
# default system encoding. And decode() will then convert this time string back
# into a Unicode string. We use decode() here because we don't want the operating
# system to do any system encoding (which may cause some problem if the string
# contains some non-English characters) for the string. Therefore we decode it
# back to its original Unicode code print.

    time_unicode = (time.strftime('%B %d, %Y at %I:%M %p %Z').
                    decode(sys.getfilesystemencoding()))
    output_suffix = """
<div class="footer">
Generated by <a href="http://code.google.com/p/googletransitdatafeed/wiki/FeedValidator">
FeedValidator</a> version %s on %s.
</div>
</body>
</html>""" % (transitfeed.__version__, time_unicode)

    f.write(transitfeed.EncodeUnicode(output_prefix))
    if self._type_to_name_to_errorset[TYPE_ERROR]:
      f.write('<h3 class="issueHeader">Errors:</h3>')
      self.FormatType(f, "Error",
                      self._type_to_name_to_errorset[TYPE_ERROR].items())
    if self._type_to_name_to_errorset[TYPE_WARNING]:
      f.write('<h3 class="issueHeader">Warnings:</h3>')
      self.FormatType(f, "Warning",
                      self._type_to_name_to_errorset[TYPE_WARNING].items())
    f.write(transitfeed.EncodeUnicode(output_suffix))

def main():
  parser = optparse.OptionParser(usage='usage: %prog [options] feed_filename',
                                 version='%prog '+transitfeed.__version__)
  parser.add_option('-n', '--noprompt', action='store_false',
                    dest='manual_entry',
                    help='do not prompt for feed location or load output in '
                    'browser')
  parser.add_option('-o', '--output', dest='output', metavar='FILE',
                    help='write html output to FILE')
  parser.add_option('-p', '--performance', action='store_true',
                    dest='performance',
                    help='output memory and time performance (Availability: '
                    'Unix')
  parser.add_option('-m', '--memory_db', dest='memory_db',  action='store_true',
                    help='Use in-memory sqlite db instead of a temporary file. '
                         'It is faster but uses more RAM.')
  parser.add_option('-d', '--duplicate_trip_check',
                    dest='check_duplicate_trips', action='store_true',
                    help='Check for duplicate trips which go through the same '
                    'stops with same service and start times')
  parser.add_option('-l', '--limit_per_type',
                    dest='limit_per_type', action='store', type='int',
                    help='Maximum number of errors and warnings to keep of '
                    'each type')
               
  parser.set_defaults(manual_entry=True, output='validation-results.html',
                      memory_db=False, check_duplicate_trips=False,
                      limit_per_type=5)
  (options, args) = parser.parse_args()
  manual_entry = options.manual_entry
  if not len(args) == 1:
    if manual_entry:
      feed = raw_input('Enter Feed Location: ')
    else:
      print >>sys.stderr, parser.format_help()
      print >>sys.stderr, '\n\nYou must provide the path of a single feed\n\n'
      sys.exit(2)
  else:
    feed = args[0]

  feed = feed.strip('"')
  print 'validating %s' % feed
  problems = HTMLCountingProblemReporter(options.limit_per_type)
  loader = transitfeed.Loader(feed, problems=problems, extra_validation=True,
                              memory_db=options.memory_db,
                              check_duplicate_trips=\
                              options.check_duplicate_trips)
  schedule = loader.Load()

  if feed == 'IWantMyvalidation-crash.txt':
    # See test/testfeedvalidator.py
    raise Exception('For testing the feed validator crash handler.')

  exit_code = 0
  if problems.HasIssues():
    print 'ERROR: %s found' % problems.FormatCount()
    exit_code = 1
  else:
    print 'feed validated successfully'

  output_filename = options.output
  try:
    output_file = open(output_filename, 'w')
    problems.WriteOutput(os.path.abspath(feed), output_file, schedule, problems)
    output_file.close()
  except IOError, e:
    print 'Error while writing %s: %s' % (output_filename, e)
    output_filename = None
    exit_code = 2

  if manual_entry and output_filename:
    webbrowser.open('file://%s' % os.path.abspath(output_filename))

  if options.performance:
    # Only available on Unix, http://docs.python.org/lib/module-resource.html
    import resource
    print "Time: %d seconds" % (
        resource.getrusage(resource.RUSAGE_SELF).ru_utime +
        resource.getrusage(resource.RUSAGE_SELF).ru_stime)

    # http://aspn.activestate.com/ASPN/Cookbook/Python/Recipe/286222
    # http://aspn.activestate.com/ASPN/Cookbook/ "The recipes are freely
    # available for review and use."
    def _VmB(VmKey):
      """Return size from proc status in bytes."""
      _proc_status = '/proc/%d/status' % os.getpid()
      _scale = {'kB': 1024.0, 'mB': 1024.0*1024.0,
                'KB': 1024.0, 'MB': 1024.0*1024.0}

       # get pseudo file  /proc/<pid>/status
      try:
          t = open(_proc_status)
          v = t.read()
          t.close()
      except:
          raise Exception("no proc file %s" % _proc_status)
          return 0  # non-Linux?
       # get VmKey line e.g. 'VmRSS:  9999  kB\n ...'
      i = v.index(VmKey)
      v = v[i:].split(None, 3)  # whitespace
      if len(v) < 3:
          raise Exception("%s" % v)
          return 0  # invalid format?
       # convert Vm value to bytes
      return int(float(v[1]) * _scale[v[2]])

    # I ran this on over a hundred GTFS files, comparing VmSize to VmRSS
    # (resident set size). The difference was always under 2% or 3MB.
    print "Virtual Memory Size: %d bytes" % _VmB('VmSize:')

  return exit_code


def ProfileMain():
  import cProfile
  import pstats
  cProfile.run('exit_code = main()', 'validate-stats')
  p = pstats.Stats('validate-stats')
  p.strip_dirs()
  p.sort_stats('cumulative').print_stats(30)
  p.sort_stats('cumulative').print_callers(30)
  return exit_code


if __name__ == '__main__':
  try:
    if '-p' in sys.argv or '--performance' in sys.argv:
      exit_code = ProfileMain()
    else:
      exit_code = main()
    sys.exit(exit_code)
  except (SystemExit, KeyboardInterrupt):
    raise
  except:
    import inspect
    import sys
    import traceback

    # Save trace and exception now. These calls look at the most recently
    # raised exception. The code that makes the report might trigger other
    # exceptions.
    original_trace = inspect.trace(3)[1:]
    formatted_exception = traceback.format_exception_only(*(sys.exc_info()[:2]))

    apology = """Yikes, the validator threw an unexpected exception!

Hopefully a complete report has been saved to validation-crash.txt,
though if you are seeing this message we've already disappointed you once
today. Please include the report in a new issue at
http://code.google.com/p/googletransitdatafeed/issues/entry
or an email to tom.brown.code@gmail.com. Sorry!

"""
    dashes = '%s\n' % ('-' * 60)
    dump = []
    dump.append(apology)
    dump.append(dashes)
    try:
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

    open('validation-crash.txt', 'w').write(''.join(dump))

    print ''.join(dump)
    print
    print dashes
    print apology

    if '-n' not in sys.argv and '--noprompt' not in sys.argv:
      raw_input('Press enter to continue')
    sys.exit(127)
