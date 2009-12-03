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
