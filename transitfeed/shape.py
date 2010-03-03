#!/usr/bin/python2.5

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

import problems as problems_module
import util

class Shape(object):
  """This class represents a geographic shape that corresponds to the route
  taken by one or more Trips."""
  _REQUIRED_FIELD_NAMES = ['shape_id', 'shape_pt_lat', 'shape_pt_lon',
                           'shape_pt_sequence']
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ['shape_dist_traveled']
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

  def AddPoint(self, lat, lon, distance=None,
               problems=problems_module.default_problem_reporter):

    try:
      lat = float(lat)
      if abs(lat) > 90.0:
        problems.InvalidValue('shape_pt_lat', lat)
        return
    except (TypeError, ValueError):
      problems.InvalidValue('shape_pt_lat', lat)
      return

    try:
      lon = float(lon)
      if abs(lon) > 180.0:
        problems.InvalidValue('shape_pt_lon', lon)
        return
    except (TypeError, ValueError):
      problems.InvalidValue('shape_pt_lon', lon)
      return

    if (abs(lat) < 1.0) and (abs(lon) < 1.0):
      problems.InvalidValue('shape_pt_lat', lat,
                            'Point location too close to 0, 0, which means '
                            'that it\'s probably an incorrect location.',
                            type=problems_module.TYPE_WARNING)
      return

    if distance == '':  # canonicalizing empty string to None for comparison
      distance = None

    if distance != None:
      try:
        distance = float(distance)
        if (distance < self.max_distance and not
            (len(self.points) == 0 and distance == 0)):  # first one can be 0
          problems.InvalidValue('shape_dist_traveled', distance,
                                'Each subsequent point in a shape should '
                                'have a distance value that\'s at least as '
                                'large as the previous ones.  In this case, '
                                'the previous distance was %f.' % 
                                self.max_distance)
          return
        else:
          self.max_distance = distance
          self.distance.append(distance)
      except (TypeError, ValueError):
        problems.InvalidValue('shape_dist_traveled', distance,
                              'This value should be a positive number.')
        return

    self.points.append((lat, lon, distance))

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

  def Validate(self, problems=problems_module.default_problem_reporter):
    if util.IsEmpty(self.shape_id):
      problems.MissingValue('shape_id')

    if not self.points:
      problems.OtherProblem('The shape with shape_id "%s" contains no points.' %
                            self.shape_id, type=problems_module.TYPE_WARNING)

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
