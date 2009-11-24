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


import optparse
import sys


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
