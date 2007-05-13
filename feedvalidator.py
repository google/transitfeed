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
DEFAULT_UNUSED_LIMIT = 5  # number of unused stops to print

class CountingProblemReporter(transitfeed.ProblemReporter):
  def __init__(self):
    transitfeed.ProblemReporter.__init__(self)
    self.count = 0
    self.unused_stops = []  # [(stop_id, stop_name)...]

  def UnusedStop(self, stop_id, stop_name):
    self.count += 1
    self.unused_stops.append((stop_id, stop_name))  
	
  def _Report(self, problem_text):
    self.count += 1
    transitfeed.ProblemReporter._Report(self, problem_text)
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
  
  if problems.unused_stops:
    print ('%d stop(s) were found that weren\'t used in any trips:' %
           len(problems.unused_stops))
    if len(problems.unused_stops) > DEFAULT_UNUSED_LIMIT:
      problems.unused_stops = problems.unused_stops[:DEFAULT_UNUSED_LIMIT]
      print '(the first %d are shown below)' % len(problems.unused_stops)
    for stop_id, stop_name in problems.unused_stops:
      print '"%s" (ID "%s")' % (stop_name, stop_id)
    print
  
  exit_code = 0
  if problems.count:
    print 'ERROR: %d validation problem(s) found' % problems.count
    exit_code = 1
  else:
    print 'feed validated successfully'
    
  if manual_entry:
    raw_input('Press Enter to quit.')
  sys.exit(exit_code)
