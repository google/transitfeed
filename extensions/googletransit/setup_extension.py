#!/usr/bin/python2.5

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

import transitfeed

import fareattribute
import feedinfo
import frequency
import schedule

def GetGtfsFactory(factory = None):
  if not factory:
    factory = transitfeed.GetGtfsFactory()

  # FareAttribute class extension
  factory.UpdateClass('FareAttribute', fareattribute.FareAttribute)

  # FeedInfo class
  factory.AddMapping('feed_info.txt',
                     {'required': False, 'loading_order': 100,
                      'classes': ['FeedInfo']})
  factory.AddClass('FeedInfo', feedinfo.FeedInfo)

  # Frequency class extension
  factory.UpdateClass('Frequency', frequency.Frequency)

  # Schedule class extension
  factory.UpdateClass('Schedule', schedule.Schedule)

  return factory
