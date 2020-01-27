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
from . import util


class Transfer(transitfeed.Transfer):
    """
    Extension of transitfeed.Transfer:
    - Adds and validates new fields (see _FIELD_NAMES). See
      https://developers.google.com/transit/gtfs/reference/gtfs-extensions#TripToTripTransfers
    - Adds new ID columns and takes them into account when validating IDs
    """

    _FIELD_NAMES = transitfeed.Transfer._FIELD_NAMES + ['from_route_id', 'to_route_id', 'from_trip_id', 'to_trip_id']
    _ID_COLUMNS = transitfeed.Transfer._ID_COLUMNS + ['from_route_id', 'to_route_id', 'from_trip_id', 'to_trip_id']

    def _ID(self):
        return tuple(self[field] for field in self._ID_COLUMNS)

    def ValidateAfterAdd(self, problems):
        transitfeed.Transfer.ValidateAfterAdd(self, problems)
        self.ValidateFromRouteIdIsValid(problems)
        self.ValidateToRouteIdIsValid(problems)
        self.ValidateFromTripIdIsValid(problems)
        self.ValidateToTripIdIsValid(problems)

    def ValidateFromTripIdIsValid(self, problems):
        if not util.IsEmpty(self.from_trip_id):
            if self.from_trip_id not in self._schedule.trips.keys():
                problems.InvalidValue('from_trip_id', self.from_trip_id)
                return False
            return True

    def ValidateToTripIdIsValid(self, problems):
        if not util.IsEmpty(self.to_trip_id):
            if self.to_trip_id not in self._schedule.trips.keys():
                problems.InvalidValue('to_trip_id', self.to_trip_id)
                return False
            return True

    def ValidateFromRouteIdIsValid(self, problems):
        if not util.IsEmpty(self.from_route_id):
            if self.from_route_id not in self._schedule.routes.keys():
                problems.InvalidValue('from_route_id', self.from_route_id)
                return False
            return True

    def ValidateToRouteIdIsValid(self, problems):
        if not util.IsEmpty(self.to_route_id):
            if self.to_route_id not in self._schedule.routes.keys():
                problems.InvalidValue('to_route_id', self.to_route_id)
                return False
            return True
