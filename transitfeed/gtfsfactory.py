#!/usr/bin/python3

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


from . import problems
from .agency import Agency
from .fareattribute import FareAttribute
from .farerule import FareRule
from .feedinfo import FeedInfo
from .frequency import Frequency
from .loader import Loader
from .route import Route
from .schedule import Schedule
from .serviceperiod import ServicePeriod
from .shape import Shape
from .shapepoint import ShapePoint
from .stop import Stop
from .stoptime import StopTime
from .transfer import Transfer
from .trip import Trip


class GtfsFactory(object):
    """A factory for the default GTFS objects"""

    _REQUIRED_MAPPING_FIELDS = ["classes", "required", "loading_order"]

    def __init__(self):

        self._class_mapping = {
            "Agency": Agency,
            "ServicePeriod": ServicePeriod,
            "FareAttribute": FareAttribute,
            "FareRule": FareRule,
            "Frequency": Frequency,
            "FeedInfo": FeedInfo,
            "Shape": Shape,
            "ShapePoint": ShapePoint,
            "Stop": Stop,
            "StopTime": StopTime,
            "Route": Route,
            "Transfer": Transfer,
            "Trip": Trip,
            "Schedule": Schedule,
            "Loader": Loader,
        }

        self._file_mapping = {
            "agency.txt": {
                "required": True,
                "loading_order": 0,
                "classes": ["Agency"],
            },
            "calendar.txt": {
                "required": False,
                "loading_order": None,
                "classes": ["ServicePeriod"],
            },
            "calendar_dates.txt": {
                "required": False,
                "loading_order": None,
                "classes": ["ServicePeriod"],
            },
            "fare_attributes.txt": {
                "required": False,
                "loading_order": 50,
                "classes": ["FareAttribute"],
            },
            "fare_rules.txt": {
                "required": False,
                "loading_order": 60,
                "classes": ["FareRule"],
            },
            "feed_info.txt": {
                "required": False,
                "loading_order": 100,
                "classes": ["FeedInfo"],
            },
            "frequencies.txt": {
                "required": False,
                "loading_order": 70,
                "classes": ["Frequency"],
            },
            "shapes.txt": {
                "required": False,
                "loading_order": None,
                "classes": ["Shape", "ShapePoint"],
            },
            "stops.txt": {
                "required": True,
                "loading_order": 10,
                "classes": ["Stop"],
            },
            "stop_times.txt": {
                "required": True,
                "loading_order": None,
                "classes": ["StopTime"],
            },
            "routes.txt": {
                "required": True,
                "loading_order": 20,
                "classes": ["Route"],
            },
            "transfers.txt": {
                "required": False,
                "loading_order": 30,
                "classes": ["Transfer"],
            },
            "trips.txt": {
                "required": True,
                "loading_order": 40,
                "classes": ["Trip"],
            },
        }

    def __getattr__(self, name):
        if name in self._class_mapping:
            return self._class_mapping[name]

        raise AttributeError(name)

    def GetGtfsClassByFileName(self, filename):
        """Returns the transitfeed class corresponding to a GTFS file.

    Args:
      filename: The filename whose class is to be returned

    Raises:
      NonStandardMapping if the specified filename has more than one
          corresponding class
    """
        if filename not in self._file_mapping:
            return None
        mapping = self._file_mapping[filename]
        class_list = mapping["classes"]
        if len(class_list) > 1:
            raise problems.NonStandardMapping(filename)
        else:
            return self._class_mapping[class_list[0]]

    def GetLoadingOrder(self):
        """Returns a list of filenames sorted by loading order.
    Only includes files that Loader's standardized loading knows how to load"""
        result = {}
        for filename, mapping in self._file_mapping.items():
            loading_order = mapping["loading_order"]
            if loading_order is not None:
                result[loading_order] = filename
        return list(result[key] for key in sorted(result))

    def IsFileRequired(self, filename):
        """Returns true if a file is required by GTFS, false otherwise.
    Unknown files are, by definition, not required"""
        if filename not in self._file_mapping:
            return False
        mapping = self._file_mapping[filename]
        return mapping["required"]

    def GetKnownFilenames(self):
        """Returns a list of all known filenames"""
        return list(self._file_mapping.keys())

    def RemoveMapping(self, filename):
        """Removes an entry from the list of known filenames.
       An entry is identified by its filename.

       filename: The filename whose mapping is to be updated.
    """
        if filename in self._file_mapping:
            del self._file_mapping[filename]

    def AddMapping(self, filename, new_mapping):
        """Adds an entry to the list of known filenames.

    Args:
        filename: The filename whose mapping is being added.
        new_mapping: A dictionary with the mapping to add. Must contain all
            fields in _REQUIRED_MAPPING_FIELDS.
    Raises:
        DuplicateMapping if the filename already exists in the mapping
        InvalidMapping if not all required fields are present
    """
        for field in self._REQUIRED_MAPPING_FIELDS:
            if field not in new_mapping:
                raise problems.InvalidMapping(field)
        if filename in self.GetKnownFilenames():
            raise problems.DuplicateMapping(filename)
        self._file_mapping[filename] = new_mapping

    def UpdateMapping(self, filename, mapping_update):
        """Updates an entry in the list of known filenames.
       An entry is identified by its filename.

    Args:
        filename: The filename whose mapping is to be updated
        mapping_update: A dictionary containing the fields to update and their
            new values.
    Raises:
        InexistentMapping if the filename does not exist in the mapping
    """
        if filename not in self._file_mapping:
            raise problems.NonexistentMapping(filename)
        mapping = self._file_mapping[filename]
        mapping.update(mapping_update)

    def AddClass(self, class_name, gtfs_class):
        """Adds an entry to the list of known classes.

    Args:
        class_name: A string with name through which gtfs_class is to be made
                    accessible.
        gtfs_class: The class to be added.
    Raises:
        DuplicateMapping if class_name is already present in the class mapping.
    """
        if class_name in self._class_mapping:
            raise problems.DuplicateMapping(class_name)
        self._class_mapping[class_name] = gtfs_class

    def UpdateClass(self, class_name, gtfs_class):
        """Updates an entry in the list of known classes.

    Args:
        class_name: A string with the class name that is to be updated.
        gtfs_class: The new class
    Raises:
        NonexistentMapping if there is no class with the specified class_name.
    """
        if class_name not in self._class_mapping:
            raise problems.NonexistentMapping(class_name)
        self._class_mapping[class_name] = gtfs_class

    def RemoveClass(self, class_name):
        """Removes an entry from the list of known classes.

    Args:
        class_name: A string with the class name that is to be removed.
    Raises:
        NonexistentMapping if there is no class with the specified class_name.
    """
        if class_name not in self._class_mapping:
            raise problems.NonexistentMapping(class_name)
        del self._class_mapping[class_name]

    def GetProblemReporter(self):
        return problems.ProblemReporter()


def GetGtfsFactory():
    """Called by FeedValidator to retrieve this extension's GtfsFactory.
     Extensions will most likely only need to create an instance of
     transitfeed.GtfsFactory, call {Remove,Add,Update}Mapping as needed, and
     return that instance"""
    return GtfsFactory()
