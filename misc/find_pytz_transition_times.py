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
Search the zoneinfo database for DST transitions which are close to noon and
midnight local time. This is to help reassure me that my suggestion in
http://groups.google.com/group/gtfs-changes/browse_thread/thread/cd1ac522472fc0e
to use noon as a fixed point will work.
This script depends on internals of pytz. I considered hacking up something
based on zic.c or zdump.c in tzcode2009k.tar.gz but this seemed much easier.
"""

import pytz
import datetime

def show_tran(distance_from_noon, tz_name, tran, inf):
  tzinfo = pytz.timezone(tz_name)
  closest_tran_utc = pytz.utc.localize(tran)
  before = closest_tran_utc + datetime.timedelta(seconds=-1)
  after = closest_tran_utc + datetime.timedelta(seconds=1)
  print "from %s to %s %s" % (before.astimezone(tzinfo),
                              after.astimezone(tzinfo), tz_name)

from_noon = []
from_midnight = []
for tz_name in pytz.all_timezones:
  tzinfo = pytz.timezone(tz_name)
  # pytz.UTC is a defined as a class and overwritten with an object
  if isinstance(tzinfo, (pytz.tzinfo.StaticTzInfo, pytz.UTC.__class__)):
    continue

  for tran, inf in zip(tzinfo._utc_transition_times, tzinfo._transition_info):
    if tran < datetime.datetime(2009, 6, 1) or tran > datetime.datetime(2010, 9, 1):
      # avoid bunch of 'date value out of range' due to tran values such as 0001-01-01 00:00:00 
      # also avoids some transitions which are close to noon but ancient history for our purposes.
      continue

    # tran is UTC time of transition
    # inf is (utcoffset, dstoffset, tzname) of transition. if tran + utcoffset is near noon we have trouble
    try:
      local_tran_hour = (tran + inf[0]).time().hour
      distance_from_noon = abs(abs(local_tran_hour % 24) - 12)
      from_noon.append((distance_from_noon, tz_name, tran, inf))
      distance_from_midnight = abs(abs((local_tran_hour + 12) % 24) - 12)
      from_midnight.append((distance_from_midnight, tz_name, tran, inf))
    except Exception, e:
       print "Trouble with %s %s %s: %s" % (tz_name, tran, inf, e)


print "Near noon"
from_noon.sort()
for t in from_noon[0:10]:
  show_tran(*t)

print "Near midnight"
from_midnight.sort()
for t in from_midnight[0:30]:
  show_tran(*t)
