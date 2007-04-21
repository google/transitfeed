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
# feed_validator.py [zip file or directory]

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
  (options, args) = parser.parse_args()
  if not len(args) == 1:
    print """Usage: feedvalidator.py [feed file or directory name]"""
    sys.exit(1)

  print 'validating %s' % args[0]
  problems = CountingProblemReporter()
  loader = transitfeed.Loader(args[0],
                              problems=problems,
                              extra_validation = True)
  loader.Load()
  if problems.count:
    print 'ERROR: %d validation problem(s) found' % problems.count
    sys.exit(1)

  print 'feed validated successfully'
  sys.exit(0)
