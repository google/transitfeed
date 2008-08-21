#!/usr/bin/python2.4
#
# Copyright 2007 Google Inc. All Rights Reserved.
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

"""A library for manipulating points and polylines.

This is a library for creating and manipulating points on the unit
sphere, as an approximate model of Earth.  The primary use of this
library is to make manipulation and matching of polylines easy in the
transitfeed library.

NOTE: in this library, Earth is modelled as a sphere, whereas
GTFS specifies that latitudes and longitudes are in WGS84.  For the
purpose of comparing and matching latitudes and longitudes that
are relatively close together on the surface of the earth, this
is adequate; for other purposes, this library may not be accurate
enough.
"""

__author__ = 'chris.harrelson.code@gmail.com (Chris Harrelson)'

import copy
import math


class ShapeError(Exception):
  """Thrown whenever there is a shape parsing error."""
  pass


EARTH_RADIUS_METERS = 6371010.0


class Point(object):
  """
  A class representing a point on the unit sphere in three dimensions.
  """
  def __init__(self, x, y, z):
    self.x = x
    self.y = y
    self.z = z

  def __str__(self):
    return "(%.15f, %.15f, %.15f) " % (self.x, self.y, self.z)

  def Norm2(self):
    """
    Returns the L_2 (Euclidean) norm of self.
    """
    sum = self.x * self.x + self.y * self.y + self.z * self.z
    return math.sqrt(sum)

  def IsUnitLength(self):
    return abs(self.Norm2() - 1.0) < 1e-14

  def Plus(self, other):
    """
    Returns a new point which is the pointwise sum of self and other.
    """
    return Point(self.x + other.x,
                 self.y + other.y,
                 self.z + other.z)

  def Minus(self, other):
    """
    Returns a new point which is the pointwise subtraction of other from
    self.
    """
    return Point(self.x - other.x,
                 self.y - other.y,
                 self.z - other.z)

  def DotProd(self, other):
    """
    Returns the (scalar) dot product of self with other.
    """
    return self.x * other.x + self.y * other.y + self.z * other.z

  def Times(self, val):
    """
    Returns a new point which is pointwise multiplied by val.
    """
    return Point(self.x * val, self.y * val, self.z * val)

  def Normalize(self):
    """
    Returns a unit point in the same direction as self.
    """
    return self.Times(1 / self.Norm2())

  def RobustCrossProd(self, other):
    """
    A robust version of cross product.  If self and other
    are not nearly the same point, returns the same value
    as CrossProd() modulo normalization.  Otherwise returns
    an arbitrary unit point orthogonal to self.
    """
    assert(self.IsUnitLength() and other.IsUnitLength())
    x = self.Plus(other).CrossProd(other.Minus(self))
    if abs(x.x) > 1e-15 or abs(x.y) > 1e-15 or abs(x.z) > 1e-15:
      return x.Normalize()
    else:
      return self.Ortho()

  def LargestComponent(self):
    """
    Returns (i, val) where i is the component index (0 - 2)
    which has largest absolute value and val is the value
    of the component.
    """
    if abs(self.x) > abs(self.y):
      if abs(self.x) > abs(self.z):
        return (0, self.x)
      else:
        return (2, self.z)
    else:
      if abs(self.y) > abs(self.z):
        return (1, self.y)
      else:
        return (2, self.z)

  def Ortho(self):
    """Returns a unit-length point orthogonal to this point"""
    (index, val) = self.LargestComponent()
    index = index - 1
    if index < 0:
      index = 2
    temp = Point(0.012, 0.053, 0.00457)
    if index == 0:
      temp.x = 1
    elif index == 1:
      temp.y = 1
    elif index == 2:
      temp.z = 1
    return self.CrossProd(temp).Normalize()

  def CrossProd(self, other):
    """
    Returns the cross product of self and other.
    """
    return Point(
        self.y * other.z - self.z * other.y,
        self.z * other.x - self.x * other.z,
        self.x * other.y - self.y * other.x)

  @staticmethod
  def _approxEq(a, b):
    return abs(a - b) < 1e-11

  def Equals(self, other):
    """
    Returns true of self and other are approximately equal.
    """
    return (self._approxEq(self.x, other.x)
            and self._approxEq(self.y, other.y)
            and self._approxEq(self.z, other.z))

  def Angle(self, other):
    """
    Returns the angle in radians between self and other.
    """
    return math.atan2(self.CrossProd(other).Norm2(),
                      self.DotProd(other))

  def ToLatLng(self):
    """
    Returns that latitude and longitude that this point represents
    under a spherical Earth model.
    """
    rad_lat = math.atan2(self.z, math.sqrt(self.x * self.x + self.y * self.y))
    rad_lng = math.atan2(self.y, self.x)
    return (rad_lat * 180.0 / math.pi, rad_lng * 180.0 / math.pi)

  @staticmethod
  def FromLatLng(lat, lng):
    """
    Returns a new point representing this latitude and longitude under
    a spherical Earth model.
    """
    phi = lat * (math.pi / 180.0)
    theta = lng * (math.pi / 180.0)
    cosphi = math.cos(phi)
    return Point(math.cos(theta) * cosphi,
                 math.sin(theta) * cosphi,
                 math.sin(phi))

  def GetDistanceMeters(self, other):
    assert(self.IsUnitLength() and other.IsUnitLength())
    return self.Angle(other) * EARTH_RADIUS_METERS


def SimpleCCW(a, b, c):
  """
  Returns true if the triangle abc is oriented counterclockwise.
  """
  return c.CrossProd(a).DotProd(b) > 0

def GetClosestPoint(x, a, b):
  """
  Returns the point on the great circle segment ab closest to x.
  """
  assert(x.IsUnitLength())
  assert(a.IsUnitLength())
  assert(b.IsUnitLength())

  a_cross_b = a.RobustCrossProd(b)
  # project to the great circle going through a and b
  p = x.Minus(
      a_cross_b.Times(
      x.DotProd(a_cross_b) / a_cross_b.Norm2()))

  # if p lies between a and b, return it
  if SimpleCCW(a_cross_b, a, p) and SimpleCCW(p, b, a_cross_b):
    return p.Normalize()

  # otherwise return the closer of a or b
  if x.Minus(a).Norm2() <= x.Minus(b).Norm2():
    return a
  else:
    return b


class Poly(object):
  """
  A class representing a polyline.
  """
  def __init__(self, points = [], name=None):
    self._points = list(points)
    self._name = name

  def AddPoint(self, p):
    """
    Adds a new point to the end of the polyline.
    """
    assert(p.IsUnitLength())
    self._points.append(p)

  def GetName(self):
    return self._name

  def GetPoint(self, i):
    return self._points[i]

  def GetPoints(self):
    return self._points

  def GetNumPoints(self):
    return len(self._points)

  def GetClosestPoint(self, p):
    """
    Returns (closest_p, closest_i), where closest_p is the closest point
    to p on the piecewise linear curve represented by the polyline,
    and closest_i is the index of the point on the polyline just before
    the polyline segment that contains closest_p.
    """
    assert(len(self._points) > 0)
    closest_point = self._points[0]
    closest_i = 0

    for i in range(0, len(self._points) - 1):
      (a, b) = (self._points[i], self._points[i+1])
      cur_closest_point = GetClosestPoint(p, a, b)
      if p.Angle(cur_closest_point) < p.Angle(closest_point):
        closest_point = cur_closest_point.Normalize()
        closest_i = i

    return (closest_point, closest_i)

  def CutAtClosestPoint(self, p):
    """
    Let x be the point on the polyline closest to p.  Then
    CutAtClosestPoint returns two new polylines, one representing
    the polyline from the beginning up to x, and one representing
    x onwards to the end of the polyline.  x is the first point
    returned in the second polyline.
    """
    (closest, i) = self.GetClosestPoint(p)

    tmp = [closest]
    tmp.extend(self._points[i+1:])
    return (Poly(self._points[0:i+1]),
            Poly(tmp))

  def GreedyPolyMatchDist(self, shape):
    """
    Tries a greedy matching algorithm to match self to the
    given shape.  Returns the maximum distance in meters of
    any point in self to its matched point in shape under the
    algorithm.

    Args: shape, a Poly object.
    """
    tmp_shape = Poly(shape.GetPoints())
    max_radius = 0
    for (i, point) in enumerate(self._points):
      tmp_shape = tmp_shape.CutAtClosestPoint(point)[1]
      dist = tmp_shape.GetPoint(0).GetDistanceMeters(point)
      max_radius = max(max_radius, dist)
    return max_radius

  def __str__(self):
    out = [self.GetName()]
    if not out:
      out = [': ']
    else:
      out = out.append(': ')
    for point in self._points:
      out = out.append(str(point))
    return ' '.join(out)


class PolyCollection(object):
  """
  A class representing a collection of polylines.
  """
  def __init__(self):
    self._name_to_shape = {}
    pass

  def AddPoly(self, poly, smart_duplicate_handling=True):
    """
    Adds a new polyline to the collection.
    """
    inserted_name = poly.GetName()
    if poly.GetName() in self._name_to_shape:
      if not smart_duplicate_handling:
        raise ShapeError("Duplicate shape found: " + poly.GetName())

      print ("Warning: duplicate shape id being added to collection: " +
             poly.GetName())
      if poly.GreedyPolyMatchDist(self._name_to_shape[poly.GetName()]) < 10:
        print "  (Skipping as it apears to be an exact duplicate)"
      else:
        print "  (Adding new shape variant with uniquified name)"
        inserted_name = "%s-%d" % (inserted_name, len(self._name_to_shape))
    self._name_to_shape[inserted_name] = poly

  def NumPolys(self):
    return len(self._name_to_shape)

  def FindMatchingPolys(self, start_point, end_point, max_radius=150):
    """
    Returns a list of polylines in the collection that have endpoints
    within max_radius of the given start end end points.
    """
    matches = []
    for shape in self._name_to_shape.itervalues():
      if start_point.GetDistanceMeters(shape.GetPoint(0)) < max_radius and \
        end_point.GetDistanceMeters(shape.GetPoint(-1)) < max_radius:
        matches.append(shape)
    return matches
