#!/usr/bin/python2.4

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
# Usage:
# feed_validator.py [--noprompt] <feed zip file or directory>

import optparse
import transitfeed
import sys

OUTPUT_ENCODING = 'utf-8'

class CountingProblemReporter(transitfeed.ProblemReporter):
  def __init__(self):
    transitfeed.ProblemReporter.__init__(self)
    self.count = 0

  def _Report(self, problem_text):
    self.count += 1
    print problem_text.encode(OUTPUT_ENCODING)
    if self._context:
      print self._context.encode(OUTPUT_ENCODING)
    print

if __name__ == '__main__':
  parser = optparse.OptionParser()
  parser.add_option('-n', '--noprompt', action='store_false',
                    dest='manual_entry')
  parser.set_defaults(manual_entry=True)
  (options, args) = parser.parse_args()
  manual_entry = options.manual_entry
  if not len(args) == 1:
    if manual_entry:
      feed = raw_input('Enter Feed Location: ')
    else:
      print 'Usage: feedvalidator [--noprompt] <feed_name>'
      sys.exit(1)
  else:
    feed = args[0]

  feed = feed.strip('"')
  print 'validating %s\n' % feed
  problems = CountingProblemReporter()
  loader = transitfeed.Loader(feed, problems=problems, extra_validation=True)
  loader.Load()
  
  exit_code = 0
  if problems.count:
    print 'ERROR: %d validation problem(s) found' % problems.count
    exit_code = 1
  else:
    print 'feed validated successfully'
    
  if manual_entry:
    raw_input('Press Enter to quit.')
  sys.exit(exit_code)
