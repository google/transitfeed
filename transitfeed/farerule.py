#!/usr/bin/python3

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


from .gtfsobjectbase import GtfsObjectBase


class FareRule(GtfsObjectBase):
    """This class represents a rule that determines which itineraries a
  fare rule applies to."""

    _REQUIRED_FIELD_NAMES = ["fare_id"]
    _FIELD_NAMES = _REQUIRED_FIELD_NAMES + [
        "route_id",
        "origin_id",
        "destination_id",
        "contains_id",
    ]
    _TABLE_NAME = "fare_rules"

    def __init__(
        self,
        fare_id=None,
        route_id=None,
        origin_id=None,
        destination_id=None,
        contains_id=None,
        field_dict=None,
    ):
        self._schedule = None
        (
            self.fare_id,
            self.route_id,
            self.origin_id,
            self.destination_id,
            self.contains_id,
        ) = (fare_id, route_id, origin_id, destination_id, contains_id)
        if field_dict:
            if isinstance(field_dict, self.GetGtfsFactory().FareRule):
                # Special case so that we don't need to re-parse the attributes to
                # native types iteritems returns all attributes that don't start with _
                for k, v in field_dict.items():
                    self.__dict__[k] = v
            else:
                self.__dict__.update(field_dict)

        # canonicalize non-content values as None
        if not self.route_id:
            self.route_id = None
        if not self.origin_id:
            self.origin_id = None
        if not self.destination_id:
            self.destination_id = None
        if not self.contains_id:
            self.contains_id = None

    def GetFieldValuesTuple(self):
        return [getattr(self, fn) for fn in self._FIELD_NAMES]

    def __getitem__(self, name):
        return getattr(self, name)

    def __eq__(self, other):
        if not other:
            return False

        if id(self) == id(other):
            return True

        return self.GetFieldValuesTuple() == other.GetFieldValuesTuple()

    def __ne__(self, other):
        return not self.__eq__(other)

    def AddToSchedule(self, schedule, problems):
        self._schedule = schedule
        schedule.AddFareRuleObject(self, problems)

    def ValidateBeforeAdd(self, problems):
        return True

    def ValidateAfterAdd(self, problems):
        return
