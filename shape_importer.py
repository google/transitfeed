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

"""A utility program to help add shapes to an existing GTFS feed.

Requires the ogr python package.
"""

__author__ = 'chris.harrelson.code@gmail.com (Chris Harrelson)'

import csv
import glob
import ogr
from optparse import OptionParser
import sys
import transitfeed
import transitshapelib


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
  print feature.GetFieldCount()
  for j in range(0, feature.GetFieldCount()):
    print '--' + feature.GetFieldDefnRef(j).GetName() + \
          ': ' + feature.GetFieldAsString(j)


def AddShapefile(shapefile, collection, key_cols):
  """
  Adds shapes found in the given shape filename to the given polyline
  collection object.
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
      shape_id = '-'.join(key_list)
    else:
      shape_id = '%s-%d' % (shapefile, i)

    poly = transitshapelib.Poly(name=shape_id)
    for j in range(0, geometry.GetPointCount()):
      (lat, lng) = (geometry.GetY(j), geometry.GetX(j))
      poly.AddPoint(transitshapelib.Point.FromLatLng(lat, lng))
    collection.AddPoly(poly)

  return collection


def GetMatchingShape(pattern_poly, trip, matches, max_distance):
  """
  Tries to find a matching shape for the given pattern Poly object,
  trip, and set of possibly matching Polys from which to choose a match.
  """
  scores = []
  for match in matches:
    score = pattern_poly.GreedyPolyMatchDist(match)

    scores.append((score, match))

  if len(scores) == 0:
    print 'No shapes to which to match trip', trip.trip_id
    return None

  scores.sort()

  if scores[0][0] > max_distance:
    print ('No matching shape found within max-distance %d for trip %s '
           '(min score was %f)'
           % (max_distance, trip.trip_id, scores[0][0]))
    return None

  return scores[0][1]


def ValidateArgs(options_parser, options, args):
  if not (args and options.source_gtfs and options.dest_gtfs):
    options_parser.error("You must specify a source and dest GTFS file, "
                         "and at least one source shapefile")

def DefineOptions():
  options_parser = OptionParser()
  options_parser.add_option("--print_columns",
                            action="store_true",
                            default=False,
                            dest="print_columns",
                            help="Print column names in shapefile DBF and exit")
  options_parser.add_option("--keycols",
                            default="",
                            dest="keycols",
                            help="Comma-separated list of the column names used"
                                 "to index shape ids")
  options_parser.add_option("--max_distance",
                            type="int",
                            default=150,
                            dest="max_distance",
                            help="Max distance from a shape to which to match")
  options_parser.add_option("--source_gtfs",
                            default="",
                            dest="source_gtfs",
                            metavar="FILE",
                            help="Read input GTFS from FILE")
  options_parser.add_option("--dest_gtfs",
                            default="",
                            dest="dest_gtfs",
                            metavar="FILE",
                            help="Write output GTFS with shapes to FILE")
  return options_parser

def main(key_cols):
  print 'Parsing shapefile(s)...'
  collection = transitshapelib.PolyCollection()
  for arg in args:
    print '  ' + arg
    AddShapefile(arg, collection, key_cols)


  print 'Loading GTFS from %s...' % options.source_gtfs
  schedule = transitfeed.Loader(options.source_gtfs).Load()

  print 'Matching shapes to trips...'
  for route in schedule.GetRouteList():
    print 'Processing route', route.route_short_name
    patterns = route.GetPatternIdTripDict()
    for pattern_id in patterns:
      pattern = patterns[pattern_id][0].GetPattern()

      matches = collection.FindMatchingPolys(
          transitshapelib.Point.FromLatLng(pattern[0].stop_lat,
                                           pattern[0].stop_lon),
          transitshapelib.Point.FromLatLng(pattern[-1].stop_lat,
                                           pattern[-1].stop_lon))
      poly_points = []
      for stop in pattern:
        poly_points.append(
            transitshapelib.Point.FromLatLng(stop.stop_lat, stop.stop_lon))

      pattern_poly = transitshapelib.Poly(poly_points)
      shape_match = GetMatchingShape(pattern_poly, patterns[pattern_id][0],
                                     matches, options.max_distance)
      if shape_match:
        for trip in patterns[pattern_id]:
          try:
            shape = schedule.GetShape(shape_match.GetName())
          except KeyError:
            shape = transitfeed.Shape(shape_match.GetName())
            for point in shape_match.GetPoints():
              (lat, lng) = point.ToLatLng()
              shape.AddPoint(lat, lng)
            schedule.AddShapeObject(shape)
          trip.shape_id = shape.shape_id

  schedule.WriteGoogleTransitFeed(options.dest_gtfs)


if __name__ == '__main__':
  options_parser = DefineOptions()
  (options, args) = options_parser.parse_args()

  ValidateArgs(options_parser, options, args)

  if options.print_columns:
    for arg in args:
      PrintColumns(arg)
    sys.exit(0)

  key_cols = options.keycols.split(',')

  main(key_cols)
