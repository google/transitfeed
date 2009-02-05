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

"""
Wrapper that makes more useful stack dumps when your script crashes.

Normal use, so that your script works if with or without traceplus:
if __name__ == '__main__':
  try:
    import traceplus
    traceplus.RunWithExpandedTrace(main)
  except ImportError:
    main()
"""

def MakeExpandedTrace(frame_records):
  """Return a list of text lines for the given list of frame records."""
  dump = []
  for (frame_obj, filename, line_num, fun_name, context_lines,
       context_index) in frame_records:
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
        truncated_val = repr(local_val)[0:500]
      except Exception, e:
        dump.append('    Exception in str(%s): %s\n' % (local_name, e))
      else:
        if len(truncated_val) >= 500:
          truncated_val = '%s...' % truncated_val[0:499]
        dump.append('    %s = %s\n' % (local_name, truncated_val))
    dump.append('\n')
  return dump


def RunWithExpandedTrace(closure):
  try:
    return closure()
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

    dashes = '%s\n' % ('-' * 60)
    dump = []
    dump.append(dashes)
    dump.extend(MakeExpandedTrace(original_trace))


    dump.append(''.join(formatted_exception))

    print ''.join(dump)
    print
    print dashes
    sys.exit(127)
