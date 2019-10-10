#!/usr/bin/python3
#
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

"""Tests for transitfeed.shapelib.py"""

__author__ = "chris.harrelson.code@gmail.com (Chris Harrelson)"

import math
import unittest

from tests import util
from transitfeed import shapelib
from transitfeed.shapelib import Point
from transitfeed.shapelib import Poly
from transitfeed.shapelib import PolyCollection
from transitfeed.shapelib import PolyGraph


def formatPoint(p, precision=12):
    formatString = "(%%.%df, %%.%df, %%.%df)" % (
        precision,
        precision,
        precision,
    )
    return formatString % (p.x, p.y, p.z)


def formatPoints(points):
    return "[%s]" % ", ".join([formatPoint(p, precision=4) for p in points])


class ShapeLibTestBase(util.TestCase):
    def assertApproxEq(self, a, b):
        self.assertAlmostEqual(a, b, 8)

    def assertPointApproxEq(self, a, b):
        try:
            self.assertApproxEq(a.x, b.x)
            self.assertApproxEq(a.y, b.y)
            self.assertApproxEq(a.z, b.z)
        except AssertionError:
            print("ERROR: %s != %s" % (formatPoint(a), formatPoint(b)))
            raise

    def assertPointsApproxEq(self, points1, points2):
        try:
            self.assertEqual(len(points1), len(points2))
        except AssertionError:
            print(
                "ERROR: %s != %s"
                % (formatPoints(points1), formatPoints(points2))
            )
            raise
        for i in range(len(points1)):
            try:
                self.assertPointApproxEq(points1[i], points2[i])
            except AssertionError:
                print(
                    "ERROR: points not equal in position %d\n%s != %s"
                    % (i, formatPoints(points1), formatPoints(points2))
                )
                raise


class TestPoints(ShapeLibTestBase):
    def testPoints(self):
        p = Point(1, 1, 1)

        self.assertApproxEq(p.DotProd(p), 3)

        self.assertApproxEq(p.Norm2(), math.sqrt(3))

        self.assertPointApproxEq(Point(1.5, 1.5, 1.5), p.Times(1.5))

        norm = 1.7320508075688772
        self.assertPointApproxEq(
            p.Normalize(), Point(1 / norm, 1 / norm, 1 / norm)
        )

        p2 = Point(1, 0, 0)
        self.assertPointApproxEq(p2, p2.Normalize())

    def testCrossProd(self):
        p1 = Point(1, 0, 0).Normalize()
        p2 = Point(0, 1, 0).Normalize()
        p1_cross_p2 = p1.CrossProd(p2)
        self.assertApproxEq(p1_cross_p2.x, 0)
        self.assertApproxEq(p1_cross_p2.y, 0)
        self.assertApproxEq(p1_cross_p2.z, 1)

    def testRobustCrossProd(self):
        p1 = Point(1, 0, 0)
        p2 = Point(1, 0, 0)
        self.assertPointApproxEq(Point(0, 0, 0), p1.CrossProd(p2))
        # only needs to be an arbitrary vector perpendicular to (1, 0, 0)
        self.assertPointApproxEq(
            Point(0.000000000000000, -0.998598452020993, 0.052925717957113),
            p1.RobustCrossProd(p2),
        )

    def testS2LatLong(self):
        point = Point.FromLatLng(30, 40)
        self.assertPointApproxEq(
            Point(0.663413948169, 0.556670399226, 0.5), point
        )
        (lat, lng) = point.ToLatLng()
        self.assertApproxEq(30, lat)
        self.assertApproxEq(40, lng)

    def testOrtho(self):
        point = Point(1, 1, 1)
        ortho = point.Ortho()
        self.assertApproxEq(ortho.DotProd(point), 0)

    def testAngle(self):
        point1 = Point(1, 1, 0).Normalize()
        point2 = Point(0, 1, 0)
        self.assertApproxEq(45, point1.Angle(point2) * 360 / (2 * math.pi))
        self.assertApproxEq(point1.Angle(point2), point2.Angle(point1))

    def testGetDistanceMeters(self):
        point1 = Point.FromLatLng(40.536895, -74.203033)
        point2 = Point.FromLatLng(40.575239, -74.112825)
        self.assertApproxEq(
            8732.623770873237, point1.GetDistanceMeters(point2)
        )


class TestClosestPoint(ShapeLibTestBase):
    def testGetClosestPoint(self):
        x = Point(1, 1, 0).Normalize()
        a = Point(1, 0, 0)
        b = Point(0, 1, 0)

        closest = shapelib.GetClosestPoint(x, a, b)
        self.assertApproxEq(0.707106781187, closest.x)
        self.assertApproxEq(0.707106781187, closest.y)
        self.assertApproxEq(0.0, closest.z)


class TestPoly(ShapeLibTestBase):
    def testGetClosestPointShape(self):
        poly = Poly()

        poly.AddPoint(Point(1, 1, 0).Normalize())
        self.assertPointApproxEq(
            Point(0.707106781187, 0.707106781187, 0), poly.GetPoint(0)
        )

        point = Point(0, 1, 1).Normalize()
        self.assertPointApproxEq(
            Point(1, 1, 0).Normalize(), poly.GetClosestPoint(point)[0]
        )

        poly.AddPoint(Point(0, 1, 1).Normalize())

        self.assertPointApproxEq(
            Point(0, 1, 1).Normalize(), poly.GetClosestPoint(point)[0]
        )

    def testCutAtClosestPoint(self):
        poly = Poly()
        poly.AddPoint(Point(0, 1, 0).Normalize())
        poly.AddPoint(Point(0, 0.5, 0.5).Normalize())
        poly.AddPoint(Point(0, 0, 1).Normalize())

        (before, after) = poly.CutAtClosestPoint(
            Point(0, 0.3, 0.7).Normalize()
        )

        self.assertTrue(2 == before.GetNumPoints())
        self.assertTrue(2 == before.GetNumPoints())
        self.assertPointApproxEq(
            Point(0, 0.707106781187, 0.707106781187), before.GetPoint(1)
        )

        self.assertPointApproxEq(
            Point(0, 0.393919298579, 0.919145030018), after.GetPoint(0)
        )

        poly = Poly()
        poly.AddPoint(
            Point.FromLatLng(40.527035999999995, -74.191265999999999)
        )
        poly.AddPoint(
            Point.FromLatLng(40.526859999999999, -74.191140000000004)
        )
        poly.AddPoint(
            Point.FromLatLng(40.524681000000001, -74.189579999999992)
        )
        poly.AddPoint(
            Point.FromLatLng(40.523128999999997, -74.188467000000003)
        )
        poly.AddPoint(
            Point.FromLatLng(40.523054999999999, -74.188676000000001)
        )
        pattern = Poly()
        pattern.AddPoint(Point.FromLatLng(40.52713, -74.191146000000003))
        self.assertApproxEq(14.564268281551, pattern.GreedyPolyMatchDist(poly))

    def testMergePolys(self):
        poly1 = Poly(name="Foo")
        poly1.AddPoint(Point(0, 1, 0).Normalize())
        poly1.AddPoint(Point(0, 0.5, 0.5).Normalize())
        poly1.AddPoint(Point(0, 0, 1).Normalize())
        poly1.AddPoint(Point(1, 1, 1).Normalize())

        poly2 = Poly()
        poly3 = Poly(name="Bar")
        poly3.AddPoint(Point(1, 1, 1).Normalize())
        poly3.AddPoint(Point(2, 0.5, 0.5).Normalize())

        merged1 = Poly.MergePolys([poly1, poly2])
        self.assertPointsApproxEq(poly1.GetPoints(), merged1.GetPoints())
        self.assertEqual("Foo;", merged1.GetName())

        merged2 = Poly.MergePolys([poly2, poly3])
        self.assertPointsApproxEq(poly3.GetPoints(), merged2.GetPoints())
        self.assertEqual(";Bar", merged2.GetName())

        merged3 = Poly.MergePolys(
            [poly1, poly2, poly3], merge_point_threshold=0
        )
        mergedPoints = poly1.GetPoints()[:]
        mergedPoints.append(poly3.GetPoint(-1))
        self.assertPointsApproxEq(mergedPoints, merged3.GetPoints())
        self.assertEqual("Foo;;Bar", merged3.GetName())

        merged4 = Poly.MergePolys([poly2])
        self.assertEqual("", merged4.GetName())
        self.assertEqual(0, merged4.GetNumPoints())

        # test merging two nearby points
        newPoint = poly1.GetPoint(-1).Plus(Point(0.000001, 0, 0)).Normalize()
        poly1.AddPoint(newPoint)
        distance = poly1.GetPoint(-1).GetDistanceMeters(poly3.GetPoint(0))
        self.assertTrue(distance <= 10)
        self.assertTrue(distance > 5)

        merged5 = Poly.MergePolys(
            [poly1, poly2, poly3], merge_point_threshold=10
        )
        mergedPoints = poly1.GetPoints()[:]
        mergedPoints.append(poly3.GetPoint(-1))
        self.assertPointsApproxEq(mergedPoints, merged5.GetPoints())
        self.assertEqual("Foo;;Bar", merged5.GetName())

        merged6 = Poly.MergePolys(
            [poly1, poly2, poly3], merge_point_threshold=5
        )
        mergedPoints = poly1.GetPoints()[:]
        mergedPoints += poly3.GetPoints()
        self.assertPointsApproxEq(mergedPoints, merged6.GetPoints())
        self.assertEqual("Foo;;Bar", merged6.GetName())

    def testReversed(self):
        p1 = Point(1, 0, 0).Normalize()
        p2 = Point(0, 0.5, 0.5).Normalize()
        p3 = Point(0.3, 0.8, 0.5).Normalize()
        poly1 = Poly([p1, p2, p3])
        self.assertPointsApproxEq([p3, p2, p1], poly1.Reversed().GetPoints())

    def testLengthMeters(self):
        p1 = Point(1, 0, 0).Normalize()
        p2 = Point(0, 0.5, 0.5).Normalize()
        p3 = Point(0.3, 0.8, 0.5).Normalize()
        poly0 = Poly([p1])
        poly1 = Poly([p1, p2])
        poly2 = Poly([p1, p2, p3])
        try:
            poly0.LengthMeters()
            self.fail("Should have thrown AssertionError")
        except AssertionError:
            pass

        p1_p2 = p1.GetDistanceMeters(p2)
        p2_p3 = p2.GetDistanceMeters(p3)
        self.assertEqual(p1_p2, poly1.LengthMeters())
        self.assertEqual(p1_p2 + p2_p3, poly2.LengthMeters())
        self.assertEqual(p1_p2 + p2_p3, poly2.Reversed().LengthMeters())


class TestCollection(ShapeLibTestBase):
    def testPolyMatch(self):
        poly = Poly()
        poly.AddPoint(Point(0, 1, 0).Normalize())
        poly.AddPoint(Point(0, 0.5, 0.5).Normalize())
        poly.AddPoint(Point(0, 0, 1).Normalize())

        collection = PolyCollection()
        collection.AddPoly(poly)
        match = collection.FindMatchingPolys(Point(0, 1, 0), Point(0, 0, 1))
        self.assertTrue(len(match) == 1 and match[0] == poly)

        match = collection.FindMatchingPolys(Point(0, 1, 0), Point(0, 1, 0))
        self.assertTrue(len(match) == 0)

        poly = Poly()
        poly.AddPoint(Point.FromLatLng(45.585212, -122.586136))
        poly.AddPoint(Point.FromLatLng(45.586654, -122.587595))
        collection = PolyCollection()
        collection.AddPoly(poly)

        match = collection.FindMatchingPolys(
            Point.FromLatLng(45.585212, -122.586136),
            Point.FromLatLng(45.586654, -122.587595),
        )
        self.assertTrue(len(match) == 1 and match[0] == poly)

        match = collection.FindMatchingPolys(
            Point.FromLatLng(45.585219, -122.586136),
            Point.FromLatLng(45.586654, -122.587595),
        )
        self.assertTrue(len(match) == 1 and match[0] == poly)

        self.assertApproxEq(0.0, poly.GreedyPolyMatchDist(poly))

        match = collection.FindMatchingPolys(
            Point.FromLatLng(45.587212, -122.586136),
            Point.FromLatLng(45.586654, -122.587595),
        )
        self.assertTrue(len(match) == 0)


class TestGraph(ShapeLibTestBase):
    def testReconstructPath(self):
        p1 = Point(1, 0, 0).Normalize()
        p2 = Point(0, 0.5, 0.5).Normalize()
        p3 = Point(0.3, 0.8, 0.5).Normalize()
        poly1 = Poly([p1, p2])
        poly2 = Poly([p3, p2])
        came_from = {p2: (p1, poly1), p3: (p2, poly2)}

        graph = PolyGraph()
        reconstructed1 = graph._ReconstructPath(came_from, p1)
        self.assertEqual(0, reconstructed1.GetNumPoints())

        reconstructed2 = graph._ReconstructPath(came_from, p2)
        self.assertPointsApproxEq([p1, p2], reconstructed2.GetPoints())

        reconstructed3 = graph._ReconstructPath(came_from, p3)
        self.assertPointsApproxEq([p1, p2, p3], reconstructed3.GetPoints())

    def testShortestPath(self):
        p1 = Point(1, 0, 0).Normalize()
        p2 = Point(0, 0.5, 0.5).Normalize()
        p3 = Point(0.3, 0.8, 0.5).Normalize()
        p4 = Point(0.7, 0.7, 0.5).Normalize()
        poly1 = Poly([p1, p2, p3], "poly1")
        poly2 = Poly([p4, p3], "poly2")
        poly3 = Poly([p4, p1], "poly3")
        graph = PolyGraph()
        graph.AddPoly(poly1)
        graph.AddPoly(poly2)
        graph.AddPoly(poly3)
        path = graph.ShortestPath(p1, p4)
        self.assertTrue(path is not None)
        self.assertPointsApproxEq([p1, p4], path.GetPoints())

        path = graph.ShortestPath(p1, p3)
        self.assertTrue(path is not None)
        self.assertPointsApproxEq([p1, p4, p3], path.GetPoints())

        path = graph.ShortestPath(p3, p1)
        self.assertTrue(path is not None)
        self.assertPointsApproxEq([p3, p4, p1], path.GetPoints())

    def testFindShortestMultiPointPath(self):
        p1 = Point(1, 0, 0).Normalize()
        p2 = Point(0.5, 0.5, 0).Normalize()
        p3 = Point(0.5, 0.5, 0.1).Normalize()
        p4 = Point(0, 1, 0).Normalize()
        poly1 = Poly([p1, p2, p3], "poly1")
        poly2 = Poly([p4, p3], "poly2")
        poly3 = Poly([p4, p1], "poly3")
        graph = PolyGraph()
        graph.AddPoly(poly1)
        graph.AddPoly(poly2)
        graph.AddPoly(poly3)
        path = graph.FindShortestMultiPointPath([p1, p3, p4])
        self.assertTrue(path is not None)
        self.assertPointsApproxEq([p1, p2, p3, p4], path.GetPoints())


if __name__ == "__main__":
    unittest.main()
