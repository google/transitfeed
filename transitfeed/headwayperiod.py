#!/usr/bin/python2.5

# Copyright (C) 2010 Google Inc.
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

class HeadwayPeriod(object):

    _REQUIRED_FIELD_NAMES = ['trip_id', 'start_time', 'end_time', 
                             'headway_secs']
    _FIELD_NAMES = _REQUIRED_FIELD_NAMES
    

    def __init__(self, field_dict=None):
      if not field_dict:
        pass
      self._trip_id = field_dict['trip_id']
      self._start_time = field_dict['start_time']
      self._end_time = field_dict['end_time']
      self._headway_secs = field_dict['headway_secs']
    
    def StartTime(self):
      return self._start_time
    
    def EndTime(self):
      return self._end_time
    
    def TripId(self):
      return self._trip_id
    
    def HeadwaySecs(self):
      return self._headway_secs
    
    def ValidateBeforeAdd(self, problems):
      return True
    
    def ValidateAfterAdd(self, problems):
      return
    
    def Validate(self, problems=None):
      return
    
    def AddToSchedule(self, schedule=None, problems=None):
      if schedule is None:
        return
      try:
        trip = schedule.GetTrip(self._trip_id)
      except KeyError:
        problems.InvalidValue('trip_id', self._trip_id)
        return
      trip.AddHeadwayPeriodObject(self, problems)
