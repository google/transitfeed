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
from __future__ import print_function

import pytz
import datetime

def hour_dist(h1, h2):
  abs_dist = abs((h1 % 24) - (h2 % 24))
  if abs_dist > 12:
    return 24 - abs_dist
  else:
    return abs_dist


def hour_avg(h1, h2):
  h1 = h1 % 24
  h2 = h2 % 24
  diff = abs(h1 - h2)
  if diff <=12:
    return (h1 + h2) / 2.0
  else:
    ((24 - diff) / 2.0 + max(h1, h2)) % 24


def show_tran(dist, tz_name, tran, inf):
  tzinfo = pytz.timezone(tz_name)
  closest_tran_utc = pytz.utc.localize(tran)
  before = closest_tran_utc + datetime.timedelta(seconds=-1)
  after = closest_tran_utc + datetime.timedelta(seconds=1)
  print("%d from %s to %s %s" % (dist, before.astimezone(tzinfo),
                                 after.astimezone(tzinfo), tz_name))

from_noon = []
from_midnight = []
for tz_name in pytz.common_timezones:
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
    # inf is (utcoffset, dstoffset, tzname) of transition.
    try:
      closest_tran_utc = pytz.utc.localize(tran)
      before = closest_tran_utc + datetime.timedelta(seconds=-1)
      after = closest_tran_utc + datetime.timedelta(seconds=1)

      tran_beforelocal = before.astimezone(tzinfo) + datetime.timedelta(seconds=1)
      tran_afterlocal = after.astimezone(tzinfo) + datetime.timedelta(seconds=-1)

      average = hour_avg(tran_beforelocal.hour, tran_afterlocal.hour)

      distance_from_noon = hour_dist(average, 12)
      from_noon.append((distance_from_noon, tz_name, tran, inf))
      distance_from_midnight = hour_dist(average, 0)
      from_midnight.append((distance_from_midnight, tz_name, tran, inf))
    except Exception as e:
       print("Trouble with %s %s %s: %s" % (tz_name, tran, inf, e))


print("Near noon")
from_noon.sort()
for t in from_noon[0:10]:
  show_tran(*t)

print("Near midnight")
from_midnight.sort()
for t in from_midnight[0:30]:
  show_tran(*t)
