#!/usr/bin/python3
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

"""A utility program to help add shapes to an existing GTFS feed.

Requires the ogr python package.
"""

__author__ = "chris.harrelson.code@gmail.com (Chris Harrelson)"

import os
import shutil
import sys
import tempfile

import ogr

import transitfeed
from transitfeed import shapelib
from transitfeed import util


class ShapeImporterError(Exception):
    pass


def PrintColumns(shapefile):
    """
    Print the columns of layer 0 of the shapefile to the screen.
    """
    ds = ogr.Open(shapefile)
    layer = ds.GetLayer(0)
    if len(layer) == 0:
        raise ShapeImporterError("Layer 0 has no elements!")

    feature = layer.GetFeature(0)
    print("%d features" % feature.GetFieldCount())
    for j in range(0, feature.GetFieldCount()):
        print(
            "--"
            + feature.GetFieldDefnRef(j).GetName()
            + ": "
            + feature.GetFieldAsString(j)
        )


def AddShapefile(shapefile, graph, key_cols):
    """
    Adds shapes found in the given shape filename to the given polyline
    graph object.
    """
    ds = ogr.Open(shapefile)
    layer = ds.GetLayer(0)

    for i in range(0, len(layer)):
        feature = layer.GetFeature(i)

        geometry = feature.GetGeometryRef()

        if key_cols:
            key_list = []
            for col in key_cols:
                key_list.append(str(feature.GetField(col)))
            shape_id = "-".join(key_list)
        else:
            shape_id = "%s-%d" % (shapefile, i)

        poly = shapelib.Poly(name=shape_id)
        for j in range(0, geometry.GetPointCount()):
            (lat, lng) = (
                round(geometry.GetY(j), 15),
                round(geometry.GetX(j), 15),
            )
            poly.AddPoint(shapelib.Point.FromLatLng(lat, lng))
        graph.AddPoly(poly)

    return graph


def GetMatchingShape(pattern_poly, trip, matches, max_distance, verbosity=0):
    """
    Tries to find a matching shape for the given pattern Poly object,
    trip, and set of possibly matching Polys from which to choose a match.
    """
    if len(matches) == 0:
        print(
            "No matching shape found within max-distance %d for trip %s "
            % (max_distance, trip.trip_id)
        )
        return None

    if verbosity >= 1:
        for match in matches:
            print("match: size %d" % match.GetNumPoints())
    scores = [
        (pattern_poly.GreedyPolyMatchDist(match), match) for match in matches
    ]

    scores.sort()

    if scores[0][0] > max_distance:
        print(
            "No matching shape found within max-distance %d for trip %s "
            "(min score was %f)" % (max_distance, trip.trip_id, scores[0][0])
        )
        return None

    return scores[0][1]


def AddExtraShapes(extra_shapes_txt, graph):
    """
    Add extra shapes into our input set by parsing them out of a GTFS-formatted
    shapes.txt file.  Useful for manually adding lines to a shape file, since it's
    a pain to edit .shp files.
    """

    print("Adding extra shapes from %s" % extra_shapes_txt)
    try:
        tmpdir = tempfile.mkdtemp()
        shutil.copy(extra_shapes_txt, os.path.join(tmpdir, "shapes.txt"))
        loader = transitfeed.ShapeLoader(tmpdir)
        schedule = loader.Load()
        for shape in schedule.GetShapeList():
            print("Adding extra shape: %s" % shape.shape_id)
            graph.AddPoly(ShapeToPoly(shape))
    finally:
        if tmpdir:
            shutil.rmtree(tmpdir)


# Note: this method lives here to avoid cross-dependencies between
# shapelib and transitfeed.
def ShapeToPoly(shape):
    poly = shapelib.Poly(name=shape.shape_id)
    for lat, lng, distance in shape.points:
        point = shapelib.Point.FromLatLng(round(lat, 15), round(lng, 15))
        poly.AddPoint(point)
    return poly


def ValidateArgs(options_parser, options, args):
    if not (args and options.source_gtfs and options.dest_gtfs):
        options_parser.error(
            "You must specify a source and dest GTFS file, "
            "and at least one source shapefile"
        )


def DefineOptions():
    usage = """%prog [options] --source_gtfs=<input GTFS.zip> --dest_gtfs=<output GTFS.zip>\
         <input.shp> [<input.shp>...]
        
        Try to match shapes in one or more SHP files to trips in a GTFS file."""
    options_parser = util.OptionParserLongError(
        usage=usage, version="%prog " + transitfeed.__version__
    )
    options_parser.add_option(
        "--print_columns",
        action="store_true",
        default=False,
        dest="print_columns",
        help="Print column names in shapefile DBF and exit",
    )
    options_parser.add_option(
        "--keycols",
        default="",
        dest="keycols",
        help="Comma-separated list of the column names used"
        "to index shape ids",
    )
    options_parser.add_option(
        "--max_distance",
        type="int",
        default=150,
        dest="max_distance",
        help="Max distance from a shape to which to match",
    )
    options_parser.add_option(
        "--source_gtfs",
        default="",
        dest="source_gtfs",
        metavar="FILE",
        help="Read input GTFS from FILE",
    )
    options_parser.add_option(
        "--dest_gtfs",
        default="",
        dest="dest_gtfs",
        metavar="FILE",
        help="Write output GTFS with shapes to FILE",
    )
    options_parser.add_option(
        "--extra_shapes",
        default="",
        dest="extra_shapes",
        metavar="FILE",
        help="Extra shapes.txt (CSV) formatted file",
    )
    options_parser.add_option(
        "--verbosity",
        type="int",
        default=0,
        dest="verbosity",
        help="Verbosity level. Higher is more verbose",
    )
    return options_parser


def main(key_cols):
    print("Parsing shapefile(s)...")
    graph = shapelib.PolyGraph()
    for arg in args:
        print("  " + arg)
        AddShapefile(arg, graph, key_cols)

    if options.extra_shapes:
        AddExtraShapes(options.extra_shapes, graph)

    print("Loading GTFS from %s..." % options.source_gtfs)
    schedule = transitfeed.Loader(options.source_gtfs).Load()
    shape_count = 0
    pattern_count = 0

    verbosity = options.verbosity

    print("Matching shapes to trips...")
    for route in schedule.GetRouteList():
        print("Processing route", route.route_short_name)
        patterns = route.GetPatternIdTripDict()
        for pattern_id, trips in patterns.items():
            pattern_count += 1
            pattern = trips[0].GetPattern()

            poly_points = [
                shapelib.Point.FromLatLng(p.stop_lat, p.stop_lon)
                for p in pattern
            ]
            if verbosity >= 2:
                print(
                    "\npattern %d, %d points:" % (pattern_id, len(poly_points))
                )
                for i, (stop, point) in enumerate(zip(pattern, poly_points)):
                    print(
                        "Stop %d '%s': %s"
                        % (i + 1, stop.stop_name, point.ToLatLng())
                    )

            # First, try to find polys that run all the way from
            # the start of the trip to the end.
            matches = graph.FindMatchingPolys(
                poly_points[0], poly_points[-1], options.max_distance
            )
            if not matches:
                # Try to find a path through the graph, joining
                # multiple edges to find a path that covers all the
                # points in the trip.  Some shape files are structured
                # this way, with a polyline for each segment between
                # stations instead of a polyline covering an entire line.
                shortest_path = graph.FindShortestMultiPointPath(
                    poly_points, options.max_distance, verbosity=verbosity
                )
                if shortest_path:
                    matches = [shortest_path]
                else:
                    matches = []

            pattern_poly = shapelib.Poly(poly_points)
            shape_match = GetMatchingShape(
                pattern_poly,
                trips[0],
                matches,
                options.max_distance,
                verbosity=verbosity,
            )
            if shape_match:
                shape_count += 1
                # Rename shape for readability.
                shape_match = shapelib.Poly(
                    points=shape_match.GetPoints(),
                    name="shape_%d" % shape_count,
                )
                for trip in trips:
                    try:
                        shape = schedule.GetShape(shape_match.GetName())
                    except KeyError:
                        shape = transitfeed.Shape(shape_match.GetName())
                        for point in shape_match.GetPoints():
                            (lat, lng) = point.ToLatLng()
                            shape.AddPoint(lat, lng)
                        schedule.AddShapeObject(shape)
                    trip.shape_id = shape.shape_id

    print(
        "Matched %d shapes out of %d patterns" % (shape_count, pattern_count)
    )
    schedule.WriteGoogleTransitFeed(options.dest_gtfs)


if __name__ == "__main__":
    # Import psyco if available for better performance.
    try:
        import psyco

        psyco.full()
    except ImportError:
        pass

    options_parser = DefineOptions()
    (options, args) = options_parser.parse_args()

    ValidateArgs(options_parser, options, args)

    if options.print_columns:
        for arg in args:
            PrintColumns(arg)
        sys.exit(0)

    key_cols = options.keycols.split(",")

    main(key_cols)
