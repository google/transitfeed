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

from agency import Agency
from fare import Fare
from farerule import FareRule
from headwayperiod import HeadwayPeriod
from route import Route
from serviceperiod import ServicePeriod
from shape import Shape
from stop import Stop
from stoptime import StopTime
from transfer import Transfer
from trip import Trip

class GtfsFactory(object):
  """A factory for the default GTFS objects"""

  # The order in which the files should be loaded
  # Must only include files that can be loaded in a standardized way by Loader
  _loading_order = ['agency.txt',
                    'stops.txt',
                    'routes.txt',
                    'transfers.txt',
                    'trips.txt',
                    'fare_attributes.txt',
                    'fare_rules.txt',
                    'frequencies.txt',
                   ]

  # Mapping from filename to transitfeed class
  # Must include all filenames, even those that cannot be loaded by Loader in
  # a standardized way
  _mapping = { 'agency.txt': Agency,
               'calendar.txt': ServicePeriod,
               'calendar_dates.txt': ServicePeriod,
               'fare_attributes.txt': Fare,
               'fare_rules.txt': FareRule,
               'frequencies.txt': HeadwayPeriod,
               'shapes.txt': Shape,
               'stops.txt': Stop,
               'stop_times.txt': StopTime,
               'routes.txt': Route,
               'transfers.txt': Transfer,
               'trips.txt': Trip,
             }

  # Files that must be included in the feed
  # calendar.txt and calendar_dates.txt are not included as one *or* the other
  # might be specified, and there is no way to express that here.
  # That logic is left to the loader
  _required_filenames = [
    'agency.txt',
    'stops.txt',
    'routes.txt',
    'trips.txt',
    'stop_times.txt',
  ]

  def GetGtfsClass(self, filename):
    """Returns the transitfeed class corresponding to a GTFS file"""
    return self._mapping.get(filename)

  def GetFilenameMapping(self):
    """Returns the mapping from GTFS filenames to transitfeed classes"""
    return self._mapping

  def GetLoadingOrder(self):
    """Returns a list of pairs (order, filename), sorted by loading order.
    Only includes files that Loader's standardized loading knows how to load"""
    return self._loading_order

  def IsFileRequired(self, name):
    """Returns true if a file is required by GTFS, false otherwise.
    Unknown files are, by definition, not required"""
    return name in self._required_filenames

  def GetKnownFilenames(self):
    return self._mapping.keys()