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


from . import problems as problems_module
from . import util
from .gtfsobjectbase import GtfsObjectBase


class ShapePoint(GtfsObjectBase):
    """This class represents a single shape point.

  Attributes:
    shape_id: represents the shape_id of the point
    shape_pt_lat: represents the latitude of the point
    shape_pt_lon: represents the longitude of the point
    shape_pt_sequence: represents the sequence of the point
    shape_dist_traveled: represents the distance of the point
  """

    _REQUIRED_FIELD_NAMES = [
        "shape_id",
        "shape_pt_lat",
        "shape_pt_lon",
        "shape_pt_sequence",
    ]
    _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ["shape_dist_traveled"]

    def __init__(
        self,
        shape_id=None,
        lat=None,
        lon=None,
        seq=None,
        dist=None,
        field_dict=None,
    ):
        """Initialize a new ShapePoint object.

    Args:
      field_dict: A dictionary mapping attribute name to unicode string
    """
        self._schedule = None
        if field_dict:
            if isinstance(field_dict, self.__class__):
                for k, v in field_dict.items():
                    self.__dict__[k] = v
            else:
                self.__dict__.update(field_dict)
        else:
            self.shape_id = shape_id
            self.shape_pt_lat = lat
            self.shape_pt_lon = lon
            self.shape_pt_sequence = seq
            self.shape_dist_traveled = dist

    def ParseAttributes(self, problems):
        """Parse all attributes, calling problems as needed.

    Return True if all of the values are valid.
    """
        if util.IsEmpty(self.shape_id):
            problems.MissingValue("shape_id")
            return

        try:
            if not isinstance(self.shape_pt_sequence, int):
                self.shape_pt_sequence = util.NonNegIntStringToInt(
                    self.shape_pt_sequence, problems
                )
            elif self.shape_pt_sequence < 0:
                problems.InvalidValue(
                    "shape_pt_sequence",
                    self.shape_pt_sequence,
                    "Value should be a number (0 or higher)",
                )
        except (TypeError, ValueError):
            problems.InvalidValue(
                "shape_pt_sequence",
                self.shape_pt_sequence,
                "Value should be a number (0 or higher)",
            )
            return

        try:
            if not isinstance(self.shape_pt_lat, (int, float)):
                self.shape_pt_lat = util.FloatStringToFloat(
                    self.shape_pt_lat, problems
                )
            if abs(self.shape_pt_lat) > 90.0:
                problems.InvalidValue("shape_pt_lat", self.shape_pt_lat)
                return
        except (TypeError, ValueError):
            problems.InvalidValue("shape_pt_lat", self.shape_pt_lat)
            return

        try:
            if not isinstance(self.shape_pt_lon, (int, float)):
                self.shape_pt_lon = util.FloatStringToFloat(
                    self.shape_pt_lon, problems
                )
            if abs(self.shape_pt_lon) > 180.0:
                problems.InvalidValue("shape_pt_lon", self.shape_pt_lon)
                return
        except (TypeError, ValueError):
            problems.InvalidValue("shape_pt_lon", self.shape_pt_lon)
            return

        if abs(self.shape_pt_lat) < 1.0 and abs(self.shape_pt_lon) < 1.0:
            problems.InvalidValue(
                "shape_pt_lat",
                self.shape_pt_lat,
                "Point location too close to 0, 0, which means "
                "that it's probably an incorrect location.",
                type=problems_module.TYPE_WARNING,
            )
            return

        if self.shape_dist_traveled == "":
            self.shape_dist_traveled = None

        if self.shape_dist_traveled is not None and not isinstance(
            self.shape_dist_traveled, (int, float)
        ):
            try:
                self.shape_dist_traveled = util.FloatStringToFloat(
                    self.shape_dist_traveled, problems
                )
            except (TypeError, ValueError):
                problems.InvalidValue(
                    "shape_dist_traveled",
                    self.shape_dist_traveled,
                    "This value should be a positive number.",
                )
                return

        if (
            self.shape_dist_traveled is not None
            and self.shape_dist_traveled < 0
        ):
            problems.InvalidValue(
                "shape_dist_traveled",
                self.shape_dist_traveled,
                "This value should be a positive number.",
            )
            return

        return True
