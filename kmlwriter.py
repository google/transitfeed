#!/usr/bin/python3
#
# Copyright 2008 Google Inc. All Rights Reserved.
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

"""A module for writing GTFS feeds out into Google Earth KML format.

For usage information run kmlwriter.py --help

If no output filename is specified, the output file will be given the same
name as the feed file (with ".kml" appended) and will be placed in the same
directory as the input feed.

The resulting KML file has a folder hierarchy which looks like this:

    - Stops
      * stop1
      * stop2
    - Routes
      - route1
        - Shapes
          * shape1
          * shape2
        - Patterns
          - pattern1
          - pattern2
        - Trips
          * trip1
          * trip2
    - Shapes
      * shape1
      - Shape Points
        * shape_point1
        * shape_point2
      * shape2
      - Shape Points
        * shape_point1
        * shape_point2

where the hyphens represent folders and the asteriks represent placemarks.

In a trip, a vehicle visits stops in a certain sequence. Such a sequence of
stops is called a pattern. A pattern is represented by a linestring connecting
the stops. The "Shapes" subfolder of a route folder contains placemarks for
each shape used by a trip in the route. The "Patterns" subfolder contains a
placemark for each unique pattern used by a trip in the route. The "Trips"
subfolder contains a placemark for each trip in the route.

Since there can be many trips and trips for the same route are usually similar,
they are not exported unless the --showtrips option is used. There is also
another option --splitroutes that groups the routes by vehicle type resulting
in a folder hierarchy which looks like this at the top level:

    - Stops
    - Routes - Bus
    - Routes - Tram
    - Routes - Rail
    - Shapes
"""

import os.path
import sys
import xml.etree.ElementTree as ET
from io import IOBase

import extensions.googletransit as googletransit
import transitfeed
from transitfeed import util


class KMLWriter(object):
    """This class knows how to write out a transit feed as KML.

    Sample usage:
      KMLWriter().Write(<transitfeed.Schedule object>, <output filename>)

    Attributes:
      show_trips: True if the individual trips should be included in the routes.
      show_stop_hierarhcy: True if station-stop hierarchy details should be
        included.
      split_routes: True if the routes should be split by type.
      shape_points: True if individual shape points should be plotted.
    """

    def __init__(self):
        """Initialise."""
        self.show_trips = False
        self.show_stop_hierarchy = False
        self.split_routes = False
        self.shape_points = False
        self.altitude_per_sec = 0.0
        self.date_filter = None

    def _SetIndentation(self, elem, level=0):
        """Indented the ElementTree DOM.

        This is the recommended way to cause an ElementTree DOM to be
        prettyprinted on output, as per: http://effbot.org/zone/element-lib.htm

        Run this on the root element before outputting the tree.

        Args:
          elem: The element to start indenting from, usually the document root.
          level: Current indentation level for recursion.
        """
        i = "\n" + level * "  "
        if len(elem):
            if not elem.text or not elem.text.strip():
                elem.text = i + "  "
            for elem in elem:
                self._SetIndentation(elem, level + 1)
            if not elem.tail or not elem.tail.strip():
                elem.tail = i
        else:
            if level and (not elem.tail or not elem.tail.strip()):
                elem.tail = i

    def _CreateFolder(self, parent, name, visible=True, description=None):
        """Create a KML Folder element.

        Args:
          parent: The parent ElementTree.Element instance.
          name: The folder name as a string.
          visible: Whether the folder is initially visible or not.
          description: A description string or None.

        Returns:
          The folder ElementTree.Element instance.
        """
        folder = ET.SubElement(parent, "Folder")
        name_tag = ET.SubElement(folder, "name")
        name_tag.text = name
        if description is not None:
            desc_tag = ET.SubElement(folder, "description")
            desc_tag.text = description
        if not visible:
            visibility = ET.SubElement(folder, "visibility")
            visibility.text = "0"
        return folder

    def _CreateStyleForRoute(self, doc, route):
        """Create a KML Style element for the route.

        The style sets the line colour if the route colour is specified. The
        line thickness is set depending on the vehicle type.

        Args:
          doc: The KML Document ElementTree.Element instance.
          route: The transitfeed.Route to create the style for.

        Returns:
          The id of the style as a string.
        """
        style_id = "route_%s" % route.route_id
        style = ET.SubElement(doc, "Style", {"id": style_id})
        linestyle = ET.SubElement(style, "LineStyle")
        width = ET.SubElement(linestyle, "width")
        type_to_width = {
            0: "3",  # Tram
            1: "3",  # Subway
            2: "5",  # Rail
            3: "1",
        }  # Bus
        width.text = type_to_width.get(route.route_type, "1")
        if route.route_color:
            color = ET.SubElement(linestyle, "color")
            red = route.route_color[0:2].lower()
            green = route.route_color[2:4].lower()
            blue = route.route_color[4:6].lower()
            color.text = "ff%s%s%s" % (blue, green, red)
        return style_id

    def _CreatePlacemark(
        self, parent, name, style_id=None, visible=True, description=None
    ):
        """Create a KML Placemark element.

        Args:
          parent: The parent ElementTree.Element instance.
          name: The placemark name as a string.
          style_id: If not None, the id of a style to use for the placemark.
          visible: Whether the placemark is initially visible or not.
          description: A description string or None.

        Returns:
          The placemark ElementTree.Element instance.
        """
        placemark = ET.SubElement(parent, "Placemark")
        placemark_name = ET.SubElement(placemark, "name")
        placemark_name.text = name
        if description is not None:
            desc_tag = ET.SubElement(placemark, "description")
            desc_tag.text = description
        if style_id is not None:
            styleurl = ET.SubElement(placemark, "styleUrl")
            styleurl.text = "#%s" % style_id
        if not visible:
            visibility = ET.SubElement(placemark, "visibility")
            visibility.text = "0"
        return placemark

    def _CreateLineString(self, parent, coordinate_list):
        """Create a KML LineString element.

        The points of the string are given in coordinate_list. Every element of
        coordinate_list should be one of a tuple (longitude, latitude) or a tuple
        (longitude, latitude, altitude).

        Args:
          parent: The parent ElementTree.Element instance.
          coordinate_list: The list of coordinates.

        Returns:
          The LineString ElementTree.Element instance or None if coordinate_list is
          empty.
        """
        if not coordinate_list:
            return None
        linestring = ET.SubElement(parent, "LineString")
        tessellate = ET.SubElement(linestring, "tessellate")
        tessellate.text = "1"
        if len(coordinate_list[0]) == 3:
            altitude_mode = ET.SubElement(linestring, "altitudeMode")
            altitude_mode.text = "absolute"
        coordinates = ET.SubElement(linestring, "coordinates")
        if len(coordinate_list[0]) == 3:
            coordinate_str_list = ["%f,%f,%f" % t for t in coordinate_list]
        else:
            coordinate_str_list = ["%f,%f" % t for t in coordinate_list]
        coordinates.text = " ".join(coordinate_str_list)
        return linestring

    def _CreateLineStringForShape(self, parent, shape):
        """Create a KML LineString using coordinates from a shape.

        Args:
          parent: The parent ElementTree.Element instance.
          shape: The transitfeed.Shape instance.

        Returns:
          The LineString ElementTree.Element instance or None if coordinate_list is
          empty.
        """
        coordinate_list = [
            (longitude, latitude)
            for (latitude, longitude, distance) in shape.points
        ]
        return self._CreateLineString(parent, coordinate_list)

    def _CreateStopsFolder(self, schedule, doc):
        """Create a KML Folder containing placemarks for each stop in the schedule.

        If there are no stops in the schedule then no folder is created.

        Args:
          schedule: The transitfeed.Schedule instance.
          doc: The KML Document ElementTree.Element instance.

        Returns:
          The Folder ElementTree.Element instance or None if there are no stops.
        """
        if not schedule.GetStopList():
            return None
        stop_folder = self._CreateFolder(doc, "Stops")
        stop_folder_selection = self._StopFolderSelectionMethod(stop_folder)
        stop_style_selection = self._StopStyleSelectionMethod(doc)
        stops = list(schedule.GetStopList())
        stops.sort(key=lambda x: x.stop_name)
        for stop in stops:
            (folder, pathway_folder) = stop_folder_selection(stop)
            (style_id, pathway_style_id) = stop_style_selection(stop)
            self._CreateStopPlacemark(folder, stop, style_id)
            if (
                self.show_stop_hierarchy
                and stop.location_type
                != transitfeed.Stop.LOCATION_TYPE_STATION
                and stop.parent_station
                and stop.parent_station in schedule.stops
            ):
                placemark = self._CreatePlacemark(
                    pathway_folder, stop.stop_name, pathway_style_id
                )
                parent_station = schedule.stops[stop.parent_station]
                coordinates = [
                    (stop.stop_lon, stop.stop_lat),
                    (parent_station.stop_lon, parent_station.stop_lat),
                ]
                self._CreateLineString(placemark, coordinates)
        return stop_folder

    def _StopFolderSelectionMethod(self, stop_folder):
        """Create a method to determine which KML folder a stop should go in.

        Args:
          stop_folder: the parent folder element for all stops.

        Returns:
          A function that should accept a Stop argument and return a tuple of
          (stop KML folder, pathways KML folder).

        Given a Stop, we need to determine which folder the stop should go in.  In
        the most basic case, that's the root Stops folder.  However, if
        show_stop_hierarchy is enabled, we put a stop in a separate sub-folder
        depending on if the stop is a station, a platform, an entrance, or just a
        plain-old stand-alone stop.  This method returns a function that is used
        to pick which folder a stop stop should go in.  It also optionally returns
        a folder where any line-string connections associated with a stop (eg. to
        show the pathway between an entrance and a station) should be added.
        """
        if not self.show_stop_hierarchy:
            return lambda stop: (stop_folder, None)

        # Create the various sub-folders for showing the stop hierarchy
        station_folder = self._CreateFolder(stop_folder, "Stations")
        platform_folder = self._CreateFolder(stop_folder, "Platforms")
        platform_connections = self._CreateFolder(
            platform_folder, "Connections"
        )
        entrance_folder = self._CreateFolder(stop_folder, "Entrances")
        entrance_connections = self._CreateFolder(
            entrance_folder, "Connections"
        )
        standalone_folder = self._CreateFolder(stop_folder, "Stand-Alone")

        def FolderSelectionMethod(stop):
            if stop.location_type == transitfeed.Stop.LOCATION_TYPE_STATION:
                return (station_folder, None)
            elif (
                stop.location_type == googletransit.Stop.LOCATION_TYPE_ENTRANCE
            ):
                return (entrance_folder, entrance_connections)
            elif stop.parent_station:
                return (platform_folder, platform_connections)
            return (standalone_folder, None)

        return FolderSelectionMethod

    def _StopStyleSelectionMethod(self, doc):
        """Create a method to determine which style to apply to a stop placemark.

        Args:
          doc: the KML document.

        Returns:
          A function that should accept a Stop argument and return a tuple of
          (stop placemark style id, pathway placemark style id).  Either style id
          can be None, indicating no style should be set.

        Given a Stop, we need to determine what KML style to apply to the stops'
        placemark.  In the most basic case, no styling is applied.  However, if
        show_stop_hierarchy is enabled, we style each type of stop differently
        depending on if the stop is a station, platform, entrance, etc.  This method
        returns a function that is used to pick which style id should be associated
        with a stop placemark, or None if no style should be applied.  It also
        optionally returns a style id to associate with any line-string connections
        associated with a stop (eg. to show the pathway between an entrance and a
        station).
        """
        if not self.show_stop_hierarchy:
            return lambda stop: (None, None)

        # Create the various styles for showing the stop hierarchy
        self._CreateStyle(
            doc, "stop_entrance", {"IconStyle": {"color": "ff0000ff"}}
        )
        self._CreateStyle(
            doc,
            "entrance_connection",
            {"LineStyle": {"color": "ff0000ff", "width": "2"}},
        )
        self._CreateStyle(
            doc, "stop_platform", {"IconStyle": {"color": "ffff0000"}}
        )
        self._CreateStyle(
            doc,
            "platform_connection",
            {"LineStyle": {"color": "ffff0000", "width": "2"}},
        )
        self._CreateStyle(
            doc, "stop_standalone", {"IconStyle": {"color": "ff00ff00"}}
        )

        def StyleSelectionMethod(stop):
            if stop.location_type == transitfeed.Stop.LOCATION_TYPE_STATION:
                return ("stop_station", None)
            elif (
                stop.location_type == googletransit.Stop.LOCATION_TYPE_ENTRANCE
            ):
                return ("stop_entrance", "entrance_connection")
            elif stop.parent_station:
                return ("stop_platform", "platform_connection")
            return ("stop_standalone", None)

        return StyleSelectionMethod

    def _CreateStyle(self, doc, style_id, style_dict):
        """Helper method to create a <Style/> element in a KML document.

        Args:
          doc: the parent KML document.
          style_id: the style id to include for the <Style/> element.
          style_dict: a dict of sub-elements and values to add to the <Style/>.

        Returns:
          The newly created <Style/> element.

        Each key of the style_dict argument is used to create a sub-element of the
        parent <Style/> element.  If the value associated with that key is a string,
        then it will be used to set the text value of the sub-element.  If the value
        is another dict, it will be used to recursively construct a sub-sub-element
        with the same semantics.
        """

        def CreateElements(current_element, current_dict):
            for (key, value) in current_dict.items():
                element = ET.SubElement(current_element, key)
                if isinstance(value, dict):
                    CreateElements(element, value)
                else:
                    element.text = value

        style = ET.SubElement(doc, "Style", {"id": style_id})
        CreateElements(style, style_dict)
        return style

    def _CreateStopPlacemark(self, stop_folder, stop, style_id):
        """Creates a new stop <Placemark/> element.

        Args:
          stop_folder: the KML folder the placemark will be added to.
          stop: the actual Stop to create a placemark for.
          style_id: optional argument indicating a style id to add to the placemark.
        """
        desc_items = []
        desc_items.append("Stop id: %s" % stop.stop_id)
        if stop.stop_desc:
            desc_items.append(stop.stop_desc)
        if stop.stop_url:
            desc_items.append(
                'Stop info page: <a href="%s">%s</a>'
                % (stop.stop_url, stop.stop_url)
            )
        description = "<br/>".join(desc_items) or None
        placemark = self._CreatePlacemark(
            stop_folder,
            stop.stop_name,
            description=description,
            style_id=style_id,
        )
        point = ET.SubElement(placemark, "Point")
        coordinates = ET.SubElement(point, "coordinates")
        coordinates.text = "%.6f,%.6f" % (stop.stop_lon, stop.stop_lat)

    def _CreateRoutePatternsFolder(
        self, parent, route, style_id=None, visible=True
    ):
        """Create a KML Folder containing placemarks for each pattern in the route.

        A pattern is a sequence of stops used by one of the trips in the route.

        If there are not patterns for the route then no folder is created and None
        is returned.

        Args:
          parent: The parent ElementTree.Element instance.
          route: The transitfeed.Route instance.
          style_id: The id of a style to use if not None.
          visible: Whether the folder is initially visible or not.

        Returns:
          The Folder ElementTree.Element instance or None if there are no patterns.
        """
        pattern_id_to_trips = route.GetPatternIdTripDict()
        if not pattern_id_to_trips:
            return None

        # sort by number of trips using the pattern
        pattern_trips = list(pattern_id_to_trips.values())
        pattern_trips.sort(key=lambda a: len(a))

        folder = self._CreateFolder(parent, "Patterns", visible)
        for n, trips in enumerate(pattern_trips):
            trip_ids = [trip.trip_id for trip in trips]
            name = "Pattern %d (trips: %d)" % (n + 1, len(trips))
            description = "Trips using this pattern (%d in total): %s" % (
                len(trips),
                ", ".join(trip_ids),
            )
            placemark = self._CreatePlacemark(
                folder, name, style_id, visible, description
            )
            coordinates = [
                (stop.stop_lon, stop.stop_lat)
                for stop in trips[0].GetPattern()
            ]
            self._CreateLineString(placemark, coordinates)
        return folder

    def _CreateRouteShapesFolder(
        self, schedule, parent, route, style_id=None, visible=True
    ):
        """Create a KML Folder for the shapes of a route.

        The folder contains a placemark for each shape referenced by a trip in the
        route. If there are no such shapes, no folder is created and None is
        returned.

        Args:
          schedule: The transitfeed.Schedule instance.
          parent: The parent ElementTree.Element instance.
          route: The transitfeed.Route instance.
          style_id: The id of a style to use if not None.
          visible: Whether the placemark is initially visible or not.

        Returns:
          The Folder ElementTree.Element instance or None.
        """
        shape_id_to_trips = {}
        for trip in route.trips:
            if trip.shape_id:
                shape_id_to_trips.setdefault(trip.shape_id, []).append(trip)
        if not shape_id_to_trips:
            return None

        # sort by the number of trips using the shape
        shape_id_to_trips_items = list(shape_id_to_trips.items())
        shape_id_to_trips_items.sort(key=lambda a: len(a))

        folder = self._CreateFolder(parent, "Shapes", visible)
        for shape_id, trips in shape_id_to_trips_items:
            trip_ids = [trip.trip_id for trip in trips]
            name = "%s (trips: %d)" % (shape_id, len(trips))
            description = "Trips using this shape (%d in total): %s" % (
                len(trips),
                ", ".join(trip_ids),
            )
            placemark = self._CreatePlacemark(
                folder, name, style_id, visible, description
            )
            self._CreateLineStringForShape(
                placemark, schedule.GetShape(shape_id)
            )
        return folder

    def _CreateRouteTripsFolder(
        self, parent, route, style_id=None, schedule=None
    ):
        """Create a KML Folder containing all the trips in the route.

        The folder contains a placemark for each of these trips. If there are no
        trips in the route, no folder is created and None is returned.

        Args:
          parent: The parent ElementTree.Element instance.
          route: The transitfeed.Route instance.
          style_id: A style id string for the placemarks or None.

        Returns:
          The Folder ElementTree.Element instance or None.
        """
        if not route.trips:
            return None
        trips = list(route.trips)
        trips.sort(key=lambda x: x.trip_id)
        trips_folder = self._CreateFolder(parent, "Trips", visible=False)
        for trip in trips:
            if self.date_filter and not trip.service_period.IsActiveOn(
                self.date_filter
            ):
                continue

            if trip.trip_headsign:
                description = "Headsign: %s" % trip.trip_headsign
            else:
                description = None

            coordinate_list = []
            for secs, stoptime, tp in trip.GetTimeInterpolatedStops():
                if self.altitude_per_sec > 0:
                    coordinate_list.append(
                        (
                            stoptime.stop.stop_lon,
                            stoptime.stop.stop_lat,
                            (secs - 3600 * 4) * self.altitude_per_sec,
                        )
                    )
                else:
                    coordinate_list.append(
                        (stoptime.stop.stop_lon, stoptime.stop.stop_lat)
                    )
            placemark = self._CreatePlacemark(
                trips_folder,
                trip.trip_id,
                style_id=style_id,
                visible=False,
                description=description,
            )
            self._CreateLineString(placemark, coordinate_list)
        return trips_folder

    def _CreateRoutesFolder(self, schedule, doc, route_type=None):
        """Create a KML Folder containing routes in a schedule.

        The folder contains a subfolder for each route in the schedule of type
        route_type. If route_type is None, then all routes are selected. Each
        subfolder contains a flattened graph placemark, a route shapes placemark
        and, if show_trips is True, a subfolder containing placemarks for each of
        the trips in the route.

        If there are no routes in the schedule then no folder is created and None
        is returned.

        Args:
          schedule: The transitfeed.Schedule instance.
          doc: The KML Document ElementTree.Element instance.
          route_type: The route type integer or None.

        Returns:
          The Folder ElementTree.Element instance or None.
        """

        def GetRouteName(route):
            """Return a placemark name for the route.

            Args:
              route: The transitfeed.Route instance.

            Returns:
              The name as a string.
            """
            name_parts = []
            if route.route_short_name:
                name_parts.append("<b>%s</b>" % route.route_short_name)
            if route.route_long_name:
                name_parts.append(route.route_long_name)
            return " - ".join(name_parts) or route.route_id

        def GetRouteDescription(route):
            """Return a placemark description for the route.

            Args:
              route: The transitfeed.Route instance.

            Returns:
              The description as a string.
            """
            desc_items = []
            if route.route_desc:
                desc_items.append(route.route_desc)
            if route.route_url:
                desc_items.append(
                    'Route info page: <a href="%s">%s</a>'
                    % (route.route_url, route.route_url)
                )
            description = "<br/>".join(desc_items)
            return description or None

        routes = [
            route
            for route in schedule.GetRouteList()
            if route_type is None or route.route_type == route_type
        ]
        if not routes:
            return None
        routes.sort(key=lambda x: GetRouteName(x))

        if route_type is not None:
            route_type_names = {
                0: "Tram, Streetcar or Light rail",
                1: "Subway or Metro",
                2: "Rail",
                3: "Bus",
                4: "Ferry",
                5: "Cable car",
                6: "Gondola or suspended cable car",
                7: "Funicular",
            }
            type_name = route_type_names.get(route_type, str(route_type))
            folder_name = "Routes - %s" % type_name
        else:
            folder_name = "Routes"
        routes_folder = self._CreateFolder(doc, folder_name, visible=False)

        for route in routes:
            style_id = self._CreateStyleForRoute(doc, route)
            route_folder = self._CreateFolder(
                routes_folder,
                GetRouteName(route),
                description=GetRouteDescription(route),
            )
            self._CreateRouteShapesFolder(
                schedule, route_folder, route, style_id, False
            )
            self._CreateRoutePatternsFolder(
                route_folder, route, style_id, False
            )
            if self.show_trips:
                self._CreateRouteTripsFolder(
                    route_folder, route, style_id, schedule
                )
        return routes_folder

    def _CreateShapesFolder(self, schedule, doc):
        """Create a KML Folder containing all the shapes in a schedule.

        The folder contains a placemark for each shape. If there are no shapes in
        the schedule then the folder is not created and None is returned.

        Args:
          schedule: The transitfeed.Schedule instance.
          doc: The KML Document ElementTree.Element instance.

        Returns:
          The Folder ElementTree.Element instance or None.
        """
        if not schedule.GetShapeList():
            return None
        shapes_folder = self._CreateFolder(doc, "Shapes")
        shapes = list(schedule.GetShapeList())
        shapes.sort(key=lambda x: x.shape_id)
        for shape in shapes:
            placemark = self._CreatePlacemark(shapes_folder, shape.shape_id)
            self._CreateLineStringForShape(placemark, shape)
            if self.shape_points:
                self._CreateShapePointFolder(shapes_folder, shape)
        return shapes_folder

    def _CreateShapePointFolder(self, shapes_folder, shape):
        """Create a KML Folder containing all the shape points in a shape.

        The folder contains placemarks for each shapepoint.

        Args:
          shapes_folder: A KML Shape Folder ElementTree.Element instance
          shape: The shape to plot.

        Returns:
          The Folder ElementTree.Element instance or None.
        """

        folder_name = shape.shape_id + " Shape Points"
        folder = self._CreateFolder(shapes_folder, folder_name, visible=False)
        for (index, (lat, lon, dist)) in enumerate(shape.points):
            placemark = self._CreatePlacemark(folder, str(index + 1))
            point = ET.SubElement(placemark, "Point")
            coordinates = ET.SubElement(point, "coordinates")
            coordinates.text = "%.6f,%.6f" % (lon, lat)
        return folder

    def Write(self, schedule, output_file):
        """Writes out a feed as KML.

        Args:
          schedule: A transitfeed.Schedule object containing the feed to write.
          output_file: The name of the output KML file, or file object to use.
        """
        # Generate the DOM to write
        root = ET.Element("kml")
        root.attrib["xmlns"] = "http://earth.google.com/kml/2.1"
        doc = ET.SubElement(root, "Document")
        open_tag = ET.SubElement(doc, "open")
        open_tag.text = "1"
        self._CreateStopsFolder(schedule, doc)
        if self.split_routes:
            route_types = set()
            for route in schedule.GetRouteList():
                route_types.add(route.route_type)
            route_types = list(route_types)
            route_types.sort()
            for route_type in route_types:
                self._CreateRoutesFolder(schedule, doc, route_type)
        else:
            self._CreateRoutesFolder(schedule, doc)
        self._CreateShapesFolder(schedule, doc)

        # Make sure we pretty-print
        self._SetIndentation(root)

        # Now write the output
        if isinstance(output_file, IOBase):
            output = output_file
        else:
            output = open(output_file, "wb")
        output.write(b"""<?xml version="1.0" encoding="UTF-8"?>\n""")
        ET.ElementTree(root).write(output)


def main():
    usage = """%prog [options] <input GTFS.zip> [<output.kml>]
        
        Reads GTFS file or directory <input GTFS.zip> and creates a KML file
        <output.kml> that contains the geographical features of the input. If
        <output.kml> is omitted a default filename is picked based on
        <input GTFS.zip>. By default the KML contains all stops and shapes.
        
        For more information see
        https://github.com/google/transitfeed/wiki/KMLWriter
        """

    parser = util.OptionParserLongError(
        usage=usage, version="%prog " + transitfeed.__version__
    )
    parser.add_option(
        "-t",
        "--showtrips",
        action="store_true",
        dest="show_trips",
        help="include the individual trips for each route",
    )
    parser.add_option(
        "-a",
        "--altitude_per_sec",
        action="store",
        type="float",
        dest="altitude_per_sec",
        help="if greater than 0 trips are drawn with time axis "
        "set to this many meters high for each second of time",
    )
    parser.add_option(
        "-s",
        "--splitroutes",
        action="store_true",
        dest="split_routes",
        help="split the routes by type",
    )
    parser.add_option(
        "-d",
        "--date_filter",
        action="store",
        type="string",
        dest="date_filter",
        help="Restrict to trips active on date YYYYMMDD",
    )
    parser.add_option(
        "-p",
        "--display_shape_points",
        action="store_true",
        dest="shape_points",
        help="shows the actual points along shapes",
    )
    parser.add_option(
        "--show_stop_hierarchy",
        action="store_true",
        dest="show_stop_hierarchy",
        help="include station-stop hierarchy info in output",
    )

    parser.set_defaults(altitude_per_sec=1.0)
    options, args = parser.parse_args()

    if len(args) < 1:
        parser.error("You must provide the path of an input GTFS file.")

    if args[0] == "IWantMyCrash":
        raise Exception("For testCrashHandler")

    input_path = args[0]
    if len(args) >= 2:
        output_path = args[1]
    else:
        path = os.path.normpath(input_path)
        (feed_dir, feed_name) = os.path.split(path)
        if "." in feed_name:
            feed_name = feed_name.rsplit(".", 1)[0]  # strip extension
        output_filename = "%s.kml" % feed_name
        output_path = os.path.join(feed_dir, output_filename)

    feed = None
    try:
        loader = transitfeed.Loader(input_path)
        feed = loader.Load()
    except transitfeed.ExceptionWithContext as e:
        print(
            (
                "\n\nGTFS feed must load without any errors.\n"
                "While loading %s the following error was found:\n%s\n%s\n"
                % (
                    input_path,
                    e.FormatContext(),
                    transitfeed.EncodeUnicode(e.FormatProblem()),
                )
            ),
            file=sys.stderr,
        )
        sys.exit(1)

    print("Writing %s" % output_path)
    writer = KMLWriter()
    writer.show_trips = options.show_trips
    writer.altitude_per_sec = options.altitude_per_sec
    writer.split_routes = options.split_routes
    writer.date_filter = options.date_filter
    writer.shape_points = options.shape_points
    writer.show_stop_hierarchy = options.show_stop_hierarchy
    writer.Write(feed, output_path)


if __name__ == "__main__":
    util.RunWithCrashHandler(main)
