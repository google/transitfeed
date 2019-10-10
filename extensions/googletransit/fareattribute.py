#!/usr/bin/python3

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


class FareAttribute(transitfeed.FareAttribute):
    """Extension of transitfeed.FareAttribute:
  - Adding field 'agency_id' and ValidateAgencyId() function.
  - Overriding ValidateAfterAdd() in order to call ValidateAgencyId().
  - See open proposal "add agency_id column to fare_attributes.txt" at
  http://groups.google.com/group/gtfs-changes/browse_frm/thread/4e74c23bb1f80480
  """

    _FIELD_NAMES = transitfeed.FareAttribute._FIELD_NAMES + ["agency_id"]

    def ValidateAgencyId(self, problems):
        agencies = self._schedule.GetAgencyList()
        for agency in agencies:
            if agency.agency_id == self.agency_id:
                return
        if len(agencies) > 1 or self.agency_id is not None:
            # If there is only one agency and Fare.agencyid is empty or not present
            # then it isn't an error
            problems.InvalidAgencyID(
                "agency_id", self.agency_id, "fare", self.fare_id
            )

    def ValidateAfterAdd(self, problems):
        super(FareAttribute, self).ValidateAfterAdd(problems)
        self.ValidateAgencyId(problems)
