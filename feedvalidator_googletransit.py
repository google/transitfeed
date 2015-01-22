#!/usr/bin/python

# Copyright (C) 2011 Google Inc.
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

"""Extension of feedvalidator.py using the googletransit extension by default.
"""

import extensions.googletransit  # needed for py2exe
import feedvalidator
from transitfeed import util


def main():
  (feed, options) = feedvalidator.ParseCommandLineArguments()
  options.extension = 'extensions.googletransit'
  return feedvalidator.RunValidationFromOptions(feed, options)


if __name__ == '__main__':
  util.RunWithCrashHandler(main)
