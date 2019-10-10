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

"""
This package provides implementation of a converter from a kml
file format into Google transit feed format.

The KmlParser class is the main class implementing the parser.

For point geometries, information about stops is extracted from a kml file.
The extractor expects the stops to be represented as placemarks with
a single point.

For line geometries, information about shapes is extracted from a kml file.

"""

import re
import xml.dom.minidom as minidom

import transitfeed
from transitfeed import util


class Placemark(object):
    def __init__(self):
        self.name = ""
        self.coordinates = []

    def IsPoint(self):
        return len(self.coordinates) == 1

    def IsLine(self):
        return len(self.coordinates) > 1


class KmlParser(object):
    def __init__(self, stopNameRe="(.*)"):
        """
        Args:
          stopNameRe - a regular expression to extract a stop name from a
                       placemaker name
        """
        self.stopNameRe = re.compile(stopNameRe)

    def Parse(self, filename, feed):
        """
        Reads the kml file, parses it and updated the Google transit feed
        object with the extracted information.

        Args:
          filename - kml file name
          feed - an instance of Schedule class to be updated
        """
        dom = minidom.parse(filename)
        self.ParseDom(dom, feed)

    def ParseDom(self, dom, feed):
        """
        Parses the given kml dom tree and updates the Google transit feed object.

        Args:
          dom - kml dom tree
          feed - an instance of Schedule class to be updated
        """
        shape_num = 0
        for node in dom.getElementsByTagName("Placemark"):
            p = self.ParsePlacemark(node)
            if p.IsPoint():
                (lon, lat) = p.coordinates[0]
                m = self.stopNameRe.search(p.name)
                feed.AddStop(lat, lon, m.group(1))
            elif p.IsLine():
                self.ConvertPlacemarkToShape(p, feed)

    def ParsePlacemark(self, node):
        ret = Placemark()
        for child in node.childNodes:
            if child.nodeName == "name":
                ret.name = self.ExtractText(child)
            if child.nodeName == "Point" or child.nodeName == "LineString":
                ret.coordinates = self.ExtractCoordinates(child)
        return ret

    def ExtractText(self, node):
        for child in node.childNodes:
            if child.nodeType == child.TEXT_NODE:
                return child.wholeText  # is a unicode string
        return ""

    def ExtractCoordinates(self, node):
        coordinatesText = ""
        for child in node.childNodes:
            if child.nodeName == "coordinates":
                coordinatesText = self.ExtractText(child)
                break
        ret = []
        for point in coordinatesText.split():
            coords = point.split(",")
            ret.append((float(coords[0]), float(coords[1])))
        return ret

    def ConvertPlacemarkToShape(self, p, feed):
        shape = transitfeed.Shape(p.name)
        for (lon, lat) in p.coordinates:
            shape.AddPoint(lat, lon)

        try:
            existing_shape = feed.GetShape(p.name)

            # If the existing shape has the same points, we don't need to add a new
            # shape.
            if existing_shape == shape:
                return

            # If the shape has different points, we need to modify our shape id so as
            # to avoid duplication.
            shape.shape_id += "_%d" % len(feed.GetShapeList())

        except KeyError:
            # No existing shape with that id, so no worries.
            pass

        feed.AddShapeObject(shape)


def main():
    usage = """%prog <input.kml> <output GTFS.zip>
        
        Reads KML file <input.kml> and creates GTFS file <output GTFS.zip> with
        placemarks in the KML represented as stops.
        """

    parser = util.OptionParserLongError(
        usage=usage, version="%prog " + transitfeed.__version__
    )
    (options, args) = parser.parse_args()
    if len(args) != 2:
        parser.error(
            "You did not provide all required command line arguments."
        )

    if args[0] == "IWantMyCrash":
        raise Exception("For testCrashHandler")

    parser = KmlParser()
    feed = transitfeed.Schedule()
    feed.save_all_stops = True
    parser.Parse(args[0], feed)
    feed.WriteGoogleTransitFeed(args[1])

    print("Done.")


if __name__ == "__main__":
    util.RunWithCrashHandler(main)
