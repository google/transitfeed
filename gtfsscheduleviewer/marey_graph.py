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

"""Output svg/xml data for a marey graph

Marey graphs are a visualization form typically used for timetables. Time
is on the x-axis and position on the y-axis. This module reads data from a
transitfeed.Schedule and creates a marey graph in svg/xml format. The graph
shows the speed between stops for each trip of a route.

TODO: This module was taken from an internal Google tool. It works but is not
well intergrated into transitfeed and schedule_viewer. Also, it has lots of
ugly hacks to compensate set canvas size and so on which could be cleaned up.

For a little more information see (I didn't make this URL ;-)
http://transliteracies.english.ucsb.edu/post/research-project/research-clearinghouse-individual/research-reports/the-indexical-imagination-marey%e2%80%99s-graphic-method-and-the-technological-transformation-of-writing-in-the-nineteenth-century

  MareyGraph: Class, keeps cache of graph data and graph properties
               and draws marey graphs in svg/xml format on request.

"""

import transitfeed


class MareyGraph:
    """Produces and caches marey graph from transit feed data."""

    _MAX_ZOOM = 5.0  # change docstring of ChangeScaleFactor if this changes
    _DUMMY_SEPARATOR = 10  # pixel

    def __init__(self):
        # Timetablerelated state
        self._cache = str()
        self._stoplist = []
        self._tlist = []
        self._stations = []
        self._decorators = []

        # TODO: Initialize default values via constructor parameters
        # or via a class constants

        # Graph properties
        self._tspan = 30  # number of hours to display
        self._offset = 0  # starting hour
        self._hour_grid = 60  # number of pixels for an hour
        self._min_grid = 5  # number of pixels between subhour lines

        # Canvas properties
        self._zoomfactor = 0.9  # svg Scaling factor
        self._xoffset = 0  # move graph horizontally
        self._yoffset = 0  # move graph veritcally
        self._bgcolor = "lightgrey"

        # height/width of graph canvas before transform
        self._gwidth = self._tspan * self._hour_grid

    def Draw(self, stoplist=None, triplist=None, height=520):
        """Main interface for drawing the marey graph.

        If called without arguments, the data generated in the previous call
        will be used. New decorators can be added between calls.

        Args:
          # Class Stop is defined in transitfeed.py
          stoplist: [Stop, Stop, ...]
          # Class Trip is defined in transitfeed.py
          triplist: [Trip, Trip, ...]

        Returns:
          # A string that contain a svg/xml web-page with a marey graph.
          " <svg  width="1440" height="520" version="1.1" ... "
        """
        output = str()
        if not triplist:
            triplist = []
        if not stoplist:
            stoplist = []

        if not self._cache or triplist or stoplist:
            self._gheight = height
            self._tlist = triplist
            self._slist = stoplist
            self._decorators = []
            self._stations = self._BuildStations(stoplist)
            self._cache = "%s %s %s %s" % (
                self._DrawBox(),
                self._DrawHours(),
                self._DrawStations(),
                self._DrawTrips(triplist),
            )

        output = "%s %s %s %s" % (
            self._DrawHeader(),
            self._cache,
            self._DrawDecorators(),
            self._DrawFooter(),
        )
        return output

    def _DrawHeader(self):
        svg_header = """
      <svg  width="%s" height="%s" version="1.1"
      xmlns="http://www.w3.org/2000/svg">
      <script type="text/ecmascript"><![CDATA[
       function init(evt) {
         if ( window.svgDocument == null )
            svgDocument = evt.target.ownerDocument;
       }
      var oldLine = 0;
      var oldStroke = 0;
      var hoffset= %s; // Data from python

      function parseLinePoints(pointnode){
        var wordlist = pointnode.split(" ");
        var xlist = new Array();
        var h;
        var m;
        // TODO: add linebreaks as appropriate
        var xstr = "  Stop Times :";
        for (i=0;i<wordlist.length;i=i+2){
          var coord = wordlist[i].split(",");
          h = Math.floor(parseInt((coord[0])-20)/60);
          m = parseInt((coord[0]-20))%%60;
          xstr = xstr +" "+ (hoffset+h) +":"+m;
        }

        return xstr;
      }

      function LineClick(tripid, x) {
        var line = document.getElementById(tripid);
        if (oldLine)
          oldLine.setAttribute("stroke",oldStroke);
        oldLine = line;
        oldStroke = line.getAttribute("stroke");

        line.setAttribute("stroke","#fff");

        var dynTxt = document.getElementById("dynamicText");
        var tripIdTxt = document.createTextNode(x);
        while (dynTxt.hasChildNodes()){
          dynTxt.removeChild(dynTxt.firstChild);
        }
        dynTxt.appendChild(tripIdTxt);
      }
      ]]> </script>
      <style type="text/css"><![CDATA[
      .T { fill:none; stroke-width:1.5 }
      .TB { fill:none; stroke:#e20; stroke-width:2 }
      .Station { fill:none; stroke-width:1 }
      .Dec { fill:none; stroke-width:1.5 }
      .FullHour { fill:none; stroke:#eee; stroke-width:1 }
      .SubHour { fill:none; stroke:#ddd; stroke-width:1 }
      .Label { fill:#aaa; font-family:Helvetica,Arial,sans;
       text-anchor:middle }
      .Info { fill:#111; font-family:Helvetica,Arial,sans;
      text-anchor:start; }
       ]]></style>
       <text class="Info" id="dynamicText" x="0" y="%d"></text>
       <g id="mcanvas"  transform="translate(%s,%s)">
       <g id="zcanvas" transform="scale(%s)">

       """ % (
            self._gwidth + self._xoffset + 20,
            self._gheight + 15,
            self._offset,
            self._gheight + 10,
            self._xoffset,
            self._yoffset,
            self._zoomfactor,
        )

        return svg_header

    def _DrawFooter(self):
        return "</g></g></svg>"

    def _DrawDecorators(self):
        """Used to draw fancy overlays on trip graphs."""
        return " ".join(self._decorators)

    def _DrawBox(self):
        tmpstr = """<rect x="%s" y="%s" width="%s" height="%s"
                fill="lightgrey" stroke="%s" stroke-width="2" />
             """ % (
            0,
            0,
            self._gwidth + 20,
            self._gheight,
            self._bgcolor,
        )
        return tmpstr

    def _BuildStations(self, stoplist):
        """Dispatches the best algorithm for calculating station line position.

        Args:
          # Class Stop is defined in transitfeed.py
          stoplist: [Stop, Stop, ...]
          # Class Trip is defined in transitfeed.py
          triplist: [Trip, Trip, ...]

        Returns:
          # One integer y-coordinate for each station normalized between
          # 0 and X, where X is the height of the graph in pixels
          [0, 33, 140, ... , X]
        """
        stations = []
        dists = self._EuclidianDistances(stoplist)
        stations = self._CalculateYLines(dists)
        return stations

    def _EuclidianDistances(self, slist):
        """Calculate euclidian distances between stops.

        Uses the stoplists long/lats to approximate distances
        between stations and build a list with y-coordinates for the
        horizontal lines in the graph.

        Args:
          # Class Stop is defined in transitfeed.py
          stoplist: [Stop, Stop, ...]

        Returns:
          # One integer for each pair of stations
          # indicating the approximate distance
          [0,33,140, ... ,X]
        """
        e_dists2 = [
            transitfeed.ApproximateDistanceBetweenStops(stop, tail)
            for (stop, tail) in zip(slist, slist[1:])
        ]

        return e_dists2

    def _CalculateYLines(self, dists):
        """Builds a list with y-coordinates for the horizontal lines in the graph.

        Args:
          # One integer for each pair of stations
          # indicating the approximate distance
          dists: [0,33,140, ... ,X]

        Returns:
          # One integer y-coordinate for each station normalized between
          # 0 and X, where X is the height of the graph in pixels
          [0, 33, 140, ... , X]
        """
        tot_dist = sum(dists)
        if tot_dist > 0:
            pixel_dist = [
                float(d * (self._gheight - 20)) / tot_dist for d in dists
            ]
            pixel_grid = [0] + [
                int(pd + sum(pixel_dist[0:i]))
                for i, pd in enumerate(pixel_dist)
            ]
        else:
            pixel_grid = []

        return pixel_grid

    def _TravelTimes(self, triplist, index=0):
        """ Calculate distances and plot stops.

        Uses a timetable to approximate distances
        between stations

        Args:
        # Class Trip is defined in transitfeed.py
        triplist: [Trip, Trip, ...]
        # (Optional) Index of Triplist prefered for timetable Calculation
        index: 3

        Returns:
        # One integer for each pair of stations
        # indicating the approximate distance
        [0,33,140, ... ,X]
        """

        def DistanceInTravelTime(dep_secs, arr_secs):
            t_dist = arr_secs - dep_secs
            if t_dist < 0:
                t_dist = self._DUMMY_SEPARATOR  # min separation
            return t_dist

        if not triplist:
            return []

        if 0 < index < len(triplist):
            trip = triplist[index]
        else:
            trip = triplist[0]

        t_dists2 = [
            DistanceInTravelTime(stop[3], tail[2])
            for (stop, tail) in zip(
                trip.GetTimeStops(), trip.GetTimeStops()[1:]
            )
        ]
        return t_dists2

    def _AddWarning(self, str):
        print(str)

    def _DrawTrips(self, triplist, colpar=""):
        """Generates svg polylines for each transit trip.

        Args:
          # Class Trip is defined in transitfeed.py
          [Trip, Trip, ...]

        Returns:
          # A string containing a polyline tag for each trip
          ' <polyline class="T" stroke="#336633" points="433,0 ...'
        """

        stations = []
        if not self._stations and triplist:
            self._stations = self._CalculateYLines(self._TravelTimes(triplist))
            if not self._stations:
                self._AddWarning("Failed to use traveltimes for graph")
                self._stations = self._CalculateYLines(self._Uniform(triplist))
                if not self._stations:
                    self._AddWarning("Failed to calculate station distances")
                    return

        stations = self._stations
        tmpstrs = []
        servlist = []
        for t in triplist:
            if not colpar:
                if t.service_id not in servlist:
                    servlist.append(t.service_id)
                shade = int(
                    servlist.index(t.service_id) * (200 / len(servlist)) + 55
                )
                color = "#00%s00" % hex(shade)[2:4]
            else:
                color = colpar

            start_offsets = [0]
            first_stop = t.GetTimeStops()[0]

            for j, freq_offset in enumerate(start_offsets):
                if j > 0 and not colpar:
                    color = "purple"
                scriptcall = (
                    "onmouseover=\"LineClick('%s','Trip %s starting %s and ending %s')\""
                    % (
                        t.trip_id,
                        t.trip_id,
                        transitfeed.FormatSecondsSinceMidnight(
                            t.GetStartTime()
                        ),
                        transitfeed.FormatSecondsSinceMidnight(t.GetEndTime()),
                    )
                )
                tmpstrhead = (
                    '<polyline class="T" id="%s" stroke="%s" %s points="'
                    % (str(t.trip_id), color, scriptcall)
                )
                tmpstrs.append(tmpstrhead)

                for i, s in enumerate(t.GetTimeStops()):
                    arr_t = s[0]
                    dep_t = s[1]
                    if arr_t is None or dep_t is None:
                        continue
                    arr_x = (
                        int(arr_t / 3600.0 * self._hour_grid)
                        - self._hour_grid * self._offset
                    )
                    dep_x = (
                        int(dep_t / 3600.0 * self._hour_grid)
                        - self._hour_grid * self._offset
                    )
                    tmpstrs.append(
                        "%s,%s " % (int(arr_x + 20), int(stations[i] + 20))
                    )
                    tmpstrs.append(
                        "%s,%s " % (int(dep_x + 20), int(stations[i] + 20))
                    )
                tmpstrs.append('" />')
        return "".join(tmpstrs)

    def _Uniform(self, triplist):
        """Fallback to assuming uniform distance between stations"""
        # This should not be neseccary, but we are in fallback mode
        longest = max([len(t.GetTimeStops()) for t in triplist])
        return [100] * longest

    def _DrawStations(self, color="#aaa"):
        """Generates svg with a horizontal line for each station/stop.

        Args:
          # Class Stop is defined in transitfeed.py
          stations: [Stop, Stop, ...]

        Returns:
          # A string containing a polyline tag for each stop
          " <polyline class="Station" stroke="#336633" points="20,0 ..."
        """
        stations = self._stations
        tmpstrs = []
        for y in stations:
            tmpstrs.append(
                '  <polyline class="Station" stroke="%s" \
      points="%s,%s, %s,%s" />'
                % (color, 20, 20 + y + 0.5, self._gwidth + 20, 20 + y + 0.5)
            )
        return "".join(tmpstrs)

    def _DrawHours(self):
        """Generates svg to show a vertical hour and sub-hour grid

        Returns:
          # A string containing a polyline tag for each grid line
          " <polyline class="FullHour" points="20,0 ..."
        """
        tmpstrs = []
        for i in range(0, int(self._gwidth), self._min_grid):
            if i % self._hour_grid == 0:
                tmpstrs.append(
                    '<polyline class="FullHour" points="%d,%d, %d,%d" />'
                    % (i + 0.5 + 20, 20, i + 0.5 + 20, self._gheight)
                )
                tmpstrs.append(
                    '<text class="Label" x="%d" y="%d">%d</text>'
                    % (i + 20, 20, (i / self._hour_grid + self._offset) % 24)
                )
            else:
                tmpstrs.append(
                    '<polyline class="SubHour" points="%d,%d,%d,%d" />'
                    % (i + 0.5 + 20, 20, i + 0.5 + 20, self._gheight)
                )
        return "".join(tmpstrs)

    def AddStationDecoration(self, index, color="#f00"):
        """Flushes existing decorations and highlights the given station-line.

        Args:
          # Integer, index of stop to be highlighted.
          index: 4
          # An optional string with a html color code
          color: "#fff"
        """
        tmpstr = str()
        num_stations = len(self._stations)
        ind = int(index)
        if self._stations:
            if 0 < ind < num_stations:
                y = self._stations[ind]
                tmpstr = (
                    '<polyline class="Dec" stroke="%s" points="%s,%s,%s,%s" />'
                    % (
                        color,
                        20,
                        20 + y + 0.5,
                        self._gwidth + 20,
                        20 + y + 0.5,
                    )
                )
        self._decorators.append(tmpstr)

    def AddTripDecoration(self, triplist, color="#f00"):
        """Flushes existing decorations and highlights the given trips.

        Args:
          # Class Trip is defined in transitfeed.py
          triplist: [Trip, Trip, ...]
          # An optional string with a html color code
          color: "#fff"
        """
        tmpstr = self._DrawTrips(triplist, color)
        self._decorators.append(tmpstr)

    def ChangeScaleFactor(self, newfactor):
        """Changes the zoom of the graph manually.

        1.0 is the original canvas size.

        Args:
          # float value between 0.0 and 5.0
          newfactor: 0.7
        """
        if float(newfactor) > 0 and float(newfactor) < self._MAX_ZOOM:
            self._zoomfactor = newfactor

    def ScaleLarger(self):
        """Increases the zoom of the graph one step (0.1 units)."""
        newfactor = self._zoomfactor + 0.1
        if float(newfactor) > 0 and float(newfactor) < self._MAX_ZOOM:
            self._zoomfactor = newfactor

    def ScaleSmaller(self):
        """Decreases the zoom of the graph one step(0.1 units)."""
        newfactor = self._zoomfactor - 0.1
        if float(newfactor) > 0 and float(newfactor) < self._MAX_ZOOM:
            self._zoomfactor = newfactor

    def ClearDecorators(self):
        """Removes all the current decorators.
        """
        self._decorators = []

    def AddTextStripDecoration(self, txtstr):
        tmpstr = '<text class="Info" x="%d" y="%d">%s</text>' % (
            0,
            20 + self._gheight,
            txtstr,
        )
        self._decorators.append(tmpstr)

    def SetSpan(self, first_arr, last_arr, mint=5, maxt=30):
        s_hour = int(first_arr / 3600) - 1
        e_hour = int(last_arr / 3600) + 1
        self._offset = max(min(s_hour, 23), 0)
        self._tspan = max(min(e_hour - s_hour, maxt), mint)
        self._gwidth = self._tspan * self._hour_grid
