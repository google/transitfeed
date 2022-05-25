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
import transitfeed.util as util

class Transfer(transitfeed.Transfer):
  """Extension of transitfeed.Transfer:
  - Adding fields 'from_route_id', to_route_id', 'from_trip_id', 'to_trip_id'  
    See propposal at 
    https://developers.google.com/transit/gtfs/reference/gtfs-extensions#TripToTripTransfers
  """

  _FIELD_NAMES = transitfeed.Transfer._FIELD_NAMES + [ 'from_route_id', 'to_route_id', 'from_trip_id', 'to_trip_id' ]
  _ID_COLUMNS = transitfeed.Transfer._ID_COLUMNS + [ 'from_route_id', 'to_route_id', 'from_trip_id', 'to_trip_id' ]

  def ValidateFromRouteIdIsValid(self, problems):
    if not util.IsEmpty(self.from_route_id) and self.from_route_id not in self._schedule.routes.keys():
      problems.InvalidValue('from_route_id', self.from_route_id)
      return False
    return True

  def ValidateToRouteIdIsValid(self, problems):
    if not util.IsEmpty(self.to_route_id) and self.to_route_id not in self._schedule.routes.keys():
      problems.InvalidValue('to_route_id', self.to_route_id)
      return False
    return True 

  def ValidateFromTripIdIsValid(self, problems):
    if not util.IsEmpty(self.from_trip_id) and self.from_trip_id not in self._schedule.trips.keys():
      problems.InvalidValue('from_trip_id', self.from_trip_id)
      return False
    return True

  def ValidateToTripIdIsValid(self, problems):
    if not util.IsEmpty(self.to_trip_id) and self.to_trip_id not in self._schedule.trips.keys():
      problems.InvalidValue('to_trip_id', self.to_trip_id)
      return False
    return True 

  def ValidateAfterAdd(self, problems):
    result = super(Transfer, self).ValidateAfterAdd(problems)
    result = self.ValidateFromRouteIdIsValid(problems) and result
    result = self.ValidateToRouteIdIsValid(problems) and result
    result = self.ValidateFromTripIdIsValid(problems) and result
    result = self.ValidateToTripIdIsValid(problems) and result
    return result