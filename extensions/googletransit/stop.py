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
import transitfeed.problems as problems_module
import route as route_extension
import re

class Stop(transitfeed.Stop):
  """Extension of transitfeed.Stop:
  - Adding and validating new fields (see _FIELD_NAMES). See proposal at
    https://sites.google.com/site/gtfschanges/spec-changes-summary#stops
  - Overriding ValidateAfterAdd() in order to call new validation functions.
  - Overriding ValidateStopLocationType(), adding location_type 2 (entrance).
  """

  _FIELD_NAMES = transitfeed.Stop._FIELD_NAMES + ['stop_timezone',
                                                  'vehicle_type',
                                                  'wheelchair_boarding']

  LOCATION_TYPE_ENTRANCE = 2

  # New validation function for field 'stop_timezone'.
  def ValidateStopTimezone(self, problems):
    util.ValidateTimezone(self.stop_timezone, 'stop_timezone', problems)

  # New validation function for field 'vehicle_type'.
  def ValidateVehicleType(self, problems):
    self.vehicle_type = util.ValidateAndReturnIntValue(
        self.vehicle_type, self._gtfs_factory.Route._ROUTE_TYPE_IDS, None, True,
        'vehicle_type', problems)
    # Entrances must not have a vehicle type, in general google transit does not
    # read vehicle types from stops with a parent station.
    if self.vehicle_type:
      if self.location_type == 2:
        problems.InvalidValue('vehicle_type', self.location_type,
              reason='an entrance must not have a vehicle type')
      elif not util.IsEmpty(self.parent_station):
        problems.InvalidValue('vehicle_type', self.location_type,
              reason='Google Transit does not read vehicle types for stops '
              'having a parent station', type=problems_module.TYPE_WARNING)

  # New validation function for field 'wheelchair_boarding'.
  def ValidateWheelchairBoarding(self, problems):
    self.wheelchair_boarding = util.ValidateAndReturnIntValue(
        self.wheelchair_boarding, [0, 1, 2], 0, True, 'wheelchair_boarding',
        problems)

  # Overriding transitfeed.Stop.ValidateBeforeAdd().
  def ValidateBeforeAdd(self, problems):
    super(Stop, self).ValidateBeforeAdd(problems)
    self.ValidateStopTimezone(problems)
    self.ValidateVehicleType(problems)
    self.ValidateWheelchairBoarding(problems)
    return True # None of these checks are blocking

  # Overriding transitfeed.Stop.ValidateStopLocationType().
  # Adding location_type 2 (entrance).
  def ValidateStopLocationType(self, problems):
    self.location_type = util.ValidateAndReturnIntValue(
        self.location_type, [0, 1, 2], 0, True, 'location_type', problems)
    # Entrances must have a parent_station.
    if self.location_type == 2 and util.IsEmpty(self.parent_station):
      problems.InvalidValue('location_type', self.location_type,
          reason='an entrance must have a parent_station')
    # Entrances or other child stops (having a parent station) must not have a
    # stop_timezone.
    if (not util.IsEmpty(self.parent_station) and
        not util.IsEmpty(self.stop_timezone)):
      problems.InvalidValue('location_type', self.location_type,
          reason='an entrance or a stop having a parent stop must not have a '
          'stop_timezone', type=problems_module.TYPE_WARNING)


  # Overriding _ReportMissingRequiredField() in order to allow empty stop_name
  # if location_type=2 (entrance).
  def _ReportMissingRequiredField(self, problems, required):
    if required == 'stop_name':
      # stops of type 2 (entrance) may have an empty stop_name
      self.ValidateStopLocationType(problems)
      if self.location_type == 2:
        return
    problems.MissingValue(required)
    setattr(self, required, None)
