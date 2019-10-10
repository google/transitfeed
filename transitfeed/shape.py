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


import bisect

from . import problems as problems_module
from . import util
from .gtfsfactoryuser import GtfsFactoryUser


class Shape(GtfsFactoryUser):
    """This class represents a geographic shape that corresponds to the route
    taken by one or more Trips."""

    _REQUIRED_FIELD_NAMES = [
        "shape_id",
        "shape_pt_lat",
        "shape_pt_lon",
        "shape_pt_sequence",
    ]
    _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ["shape_dist_traveled"]
    _DEPRECATED_FIELD_NAMES = []

    def __init__(self, shape_id):
        # List of shape point tuple (lat, lng, shape_dist_traveled), where lat and
        # lon is the location of the shape point, and shape_dist_traveled is an
        # increasing metric representing the distance traveled along the shape.
        self.points = []
        # An ID that uniquely identifies a shape in the dataset.
        self.shape_id = shape_id
        # The max shape_dist_traveled of shape points in this shape.
        self.max_distance = 0
        # List of shape_dist_traveled of each shape point.
        self.distance = []
        # List of shape_pt_sequence of each shape point.
        self.sequence = []

    def __hash__(self):
        return hash((self.shape_id))

    def AddPoint(
        self,
        lat,
        lon,
        distance=None,
        problems=problems_module.default_problem_reporter,
    ):
        shapepoint_class = self.GetGtfsFactory().ShapePoint
        shapepoint = shapepoint_class(
            self.shape_id, lat, lon, len(self.sequence), distance
        )
        if shapepoint.ParseAttributes(problems):
            self.AddShapePointObjectUnsorted(shapepoint, problems)

    def AddShapePointObjectUnsorted(self, shapepoint, problems):
        """Insert a point into a correct position by sequence. """
        if (
            len(self.sequence) == 0
            or shapepoint.shape_pt_sequence >= self.sequence[-1]
        ):
            index = len(self.sequence)
        elif shapepoint.shape_pt_sequence <= self.sequence[0]:
            index = 0
        else:
            index = bisect.bisect(self.sequence, shapepoint.shape_pt_sequence)

        if shapepoint.shape_pt_sequence in self.sequence:
            problems.InvalidValue(
                "shape_pt_sequence",
                shapepoint.shape_pt_sequence,
                "The sequence number %d occurs more than once in "
                "shape %s." % (shapepoint.shape_pt_sequence, self.shape_id),
            )

        if (
            shapepoint.shape_dist_traveled is not None
            and len(self.sequence) > 0
        ):
            if (
                index != len(self.sequence)
                and shapepoint.shape_dist_traveled > self.distance[index]
            ):
                problems.InvalidValue(
                    "shape_dist_traveled",
                    shapepoint.shape_dist_traveled,
                    "Each subsequent point in a shape should have "
                    "a distance value that shouldn't be larger "
                    "than the next ones. In this case, the next "
                    "distance was %f." % self.distance[index],
                )

            if (
                index > 0
                and shapepoint.shape_dist_traveled < self.distance[index - 1]
            ):
                problems.InvalidValue(
                    "shape_dist_traveled",
                    shapepoint.shape_dist_traveled,
                    "Each subsequent point in a shape should have "
                    "a distance value that's at least as large as "
                    "the previous ones. In this case, the previous "
                    "distance was %f." % self.distance[index - 1],
                )

            if shapepoint.shape_dist_traveled > self.max_distance:
                self.max_distance = shapepoint.shape_dist_traveled

        self.sequence.insert(index, shapepoint.shape_pt_sequence)
        self.distance.insert(index, shapepoint.shape_dist_traveled)
        self.points.insert(
            index,
            (
                shapepoint.shape_pt_lat,
                shapepoint.shape_pt_lon,
                shapepoint.shape_dist_traveled,
            ),
        )

    def ClearPoints(self):
        self.points = []

    def __eq__(self, other):
        if not other:
            return False

        if id(self) == id(other):
            return True

        return self.points == other.points

    def __ne__(self, other):
        return not self.__eq__(other)

    def __repr__(self):
        return "<Shape %s>" % self.__dict__

    def ValidateShapeId(self, problems):
        if util.IsEmpty(self.shape_id):
            problems.MissingValue("shape_id")

    def ValidateShapePoints(self, problems):
        if not self.points:
            problems.OtherProblem(
                'The shape with shape_id "%s" contains no points.'
                % self.shape_id,
                type=problems_module.TYPE_WARNING,
            )

    def Validate(self, problems=problems_module.default_problem_reporter):
        self.ValidateShapeId(problems)
        self.ValidateShapePoints(problems)

    def GetPointWithDistanceTraveled(self, shape_dist_traveled):
        """Returns a point on the shape polyline with the input shape_dist_traveled.

        Args:
          shape_dist_traveled: The input shape_dist_traveled.

        Returns:
          The shape point as a tuple (lat, lng, shape_dist_traveled), where lat and
          lng is the location of the shape point, and shape_dist_traveled is an
          increasing metric representing the distance traveled along the shape.
          Returns None if there is data error in shape.
        """
        if not self.distance:
            return None
        if shape_dist_traveled <= self.distance[0]:
            return self.points[0]
        if shape_dist_traveled >= self.distance[-1]:
            return self.points[-1]

        index = bisect.bisect(self.distance, shape_dist_traveled)
        (lat0, lng0, dist0) = self.points[index - 1]
        (lat1, lng1, dist1) = self.points[index]

        # Interpolate if shape_dist_traveled does not equal to any of the point
        # in shape segment.
        # (lat0, lng0)          (lat, lng)           (lat1, lng1)
        # -----|--------------------|---------------------|------
        #    dist0          shape_dist_traveled         dist1
        #      \------- ca --------/ \-------- bc -------/
        #       \----------------- ba ------------------/
        ca = shape_dist_traveled - dist0
        bc = dist1 - shape_dist_traveled
        ba = bc + ca
        if ba == 0:
            # This only happens when there's data error in shapes and should have been
            # catched before. Check to avoid crash.
            return None
        # This won't work crossing longitude 180 and is only an approximation which
        # works well for short distance.
        lat = (lat1 * ca + lat0 * bc) / ba
        lng = (lng1 * ca + lng0 * bc) / ba
        return (lat, lng, shape_dist_traveled)
