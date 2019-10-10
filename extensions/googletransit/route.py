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
import transitfeed.util as util


class Route(transitfeed.Route):
    """Extension of transitfeed.Route:
  - Adding field 'co2_per_km' and ValidateCo2PerKm() function. See proposal at
    https://sites.google.com/site/gtfschanges/open-proposals
  - Adding HVT (Hierarchical Vehicle Type) route types, also used in
    extension of transitfeed.Stop for validating the vehicle_type field.
    The HVT values are derived from the European TPEG standard. See discussion
    at http://groups.google.com/group/gtfs-changes/msg/ed917a69cf8c5bef
  """

    _FIELD_NAMES = transitfeed.Route._FIELD_NAMES + ["co2_per_km"]

    _ROUTE_TYPES = dict(
        list(transitfeed.Route._ROUTE_TYPES.items())
        + list(
            {
                8: {"name": "Horse Carriage", "max_speed": 50},
                9: {"name": "Intercity Bus", "max_speed": 120},
                10: {"name": "Commuter Train", "max_speed": 150},
                11: {"name": "Trolleybus", "max_speed": 100},
                12: {"name": "Monorail", "max_speed": 150},
                # adding IDs from hierarchical vehicle types (HVT) list
                100: {"name": "Railway Service", "max_speed": 300},
                101: {"name": "High Speed Rail", "max_speed": 300},
                102: {"name": "Long Distance Trains", "max_speed": 300},
                108: {"name": "Rail Shuttle", "max_speed": 300},
                109: {"name": "Suburban Railway", "max_speed": 300},
                200: {"name": "CoachService", "max_speed": 120},
                201: {"name": "InternationalCoach", "max_speed": 120},
                202: {"name": "NationalCoach", "max_speed": 120},
                204: {"name": "RegionalCoach", "max_speed": 120},
                208: {"name": "CommuterCoach", "max_speed": 120},
                400: {"name": "UrbanRailwayService", "max_speed": 150},
                401: {"name": "Metro", "max_speed": 150},
                402: {"name": "Underground", "max_speed": 150},
                405: {"name": "Monorail", "max_speed": 150},
                700: {"name": "BusService", "max_speed": 100},
                701: {"name": "RegionalBus", "max_speed": 120},
                702: {"name": "ExpressBus", "max_speed": 120},
                704: {"name": "LocalBus", "max_speed": 100},
                800: {"name": "TrolleybusService", "max_speed": 100},
                900: {"name": "TramService", "max_speed": 100},
                1000: {"name": "WaterTransportService", "max_speed": 80},
                1100: {"name": "AirService", "max_speed": 1000},
                1300: {"name": "TelecabinService", "max_speed": 50},
                1400: {"name": "FunicularService", "max_speed": 50},
                1500: {"name": "TaxiService", "max_speed": 100},
                1501: {"name": "CommunalTaxi", "max_speed": 100},
                1700: {"name": "MiscellaneousService", "max_speed": 100},
                1701: {"name": "CableCar", "max_speed": 50},
                1702: {"name": "HorseDrawnCarriage", "max_speed": 50},
            }.items()
        )
    )

    _ROUTE_TYPE_IDS = set(_ROUTE_TYPES.keys())

    # _ROUTE_TYPE_NAMES is not getting updated as we should not continue to allow
    # reverse lookup by name. The new non GTFS route types are only valid as int.

    def ValidateCo2PerKm(self, problems):
        if not util.IsEmpty(self.co2_per_km):
            try:
                self.co2_per_km = float(self.co2_per_km)
            except ValueError:
                problems.InvalidValue("co2_per_km", self.co2_per_km)

    def ValidateBeforeAdd(self, problems):
        self.ValidateCo2PerKm(problems)
        return super(Route, self).ValidateBeforeAdd(problems)
