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


"""Output Google Transit URLs for queries near stops.

The output can be used to speed up manual testing. Load the output from this
file and then open many of the links in new tabs. In each result check that the
polyline looks okay (no unnecassary loops, no jumps to a far away location) and
look at the time of each leg. Also check the route names and headsigns are
formatted correctly and not redundant.
"""

import math
import optparse
import os.path
import random
import sys
import urllib.error
import urllib.parse
import urllib.parse
import urllib.request
from datetime import datetime
from datetime import timedelta

import transitfeed


def Distance(lat0, lng0, lat1, lng1):
    """
    Compute the geodesic distance in meters between two points on the
    surface of the Earth.  The latitude and longitude angles are in
    degrees.

    Approximate geodesic distance function (Haversine Formula) assuming
    a perfect sphere of radius 6367 km (see "What are some algorithms
    for calculating the distance between 2 points?" in the GIS Faq at
    http://www.census.gov/geo/www/faq-index.html).  The approximate
    radius is adequate for our needs here, but a more sophisticated
    geodesic function should be used if greater accuracy is required
    (see "When is it NOT okay to assume the Earth is a sphere?" in the
    same faq).
    """
    deg2rad = math.pi / 180.0
    lat0 = lat0 * deg2rad
    lng0 = lng0 * deg2rad
    lat1 = lat1 * deg2rad
    lng1 = lng1 * deg2rad
    dlng = lng1 - lng0
    dlat = lat1 - lat0
    a = math.sin(dlat * 0.5)
    b = math.sin(dlng * 0.5)
    a = a * a + math.cos(lat0) * math.cos(lat1) * b * b
    c = 2.0 * math.atan2(math.sqrt(a), math.sqrt(1.0 - a))
    return 6367000.0 * c


def AddNoiseToLatLng(lat, lng):
    """Add up to 500m of error to each coordinate of lat, lng."""
    m_per_tenth_lat = Distance(lat, lng, lat + 0.1, lng)
    m_per_tenth_lng = Distance(lat, lng, lat, lng + 0.1)
    lat_per_100m = 1 / m_per_tenth_lat * 10
    lng_per_100m = 1 / m_per_tenth_lng * 10
    return (
        lat + (lat_per_100m * 5 * (random.random() * 2 - 1)),
        lng + (lng_per_100m * 5 * (random.random() * 2 - 1)),
    )


def GetRandomLocationsNearStops(schedule):
    """Return a list of (lat, lng) tuples."""
    locations = []
    for s in schedule.GetStopList():
        locations.append(AddNoiseToLatLng(s.stop_lat, s.stop_lon))
    return locations


def GetRandomDatetime():
    """Return a datetime in the next week."""
    seconds_offset = random.randint(0, 60 * 60 * 24 * 7)
    dt = datetime.today() + timedelta(seconds=seconds_offset)
    return dt.replace(second=0, microsecond=0)


def FormatLatLng(lat_lng):
    """Format a (lat, lng) tuple into a string for maps.google.com."""
    return "%0.6f,%0.6f" % lat_lng


def LatLngsToGoogleUrl(source, destination, dt):
    """Return a URL for routing between two (lat, lng) at a datetime."""
    params = {
        "saddr": FormatLatLng(source),
        "daddr": FormatLatLng(destination),
        "time": dt.strftime("%I:%M%p"),
        "date": dt.strftime("%Y-%m-%d"),
        "dirflg": "r",
        "ie": "UTF8",
        "oe": "UTF8",
    }
    url = urllib.parse.urlunsplit(
        (
            "http",
            "maps.google.com",
            "/maps",
            urllib.parse.urlencode(params),
            "",
        )
    )
    return url


def LatLngsToGoogleLink(source, destination):
    """Return a string "<a ..." for a trip at a random time."""
    dt = GetRandomDatetime()
    return "<a href='%s'>from:%s to:%s on %s</a>" % (
        LatLngsToGoogleUrl(source, destination, dt),
        FormatLatLng(source),
        FormatLatLng(destination),
        dt.ctime(),
    )


def WriteOutput(title, locations, limit, f):
    """Write html to f for up to limit trips between locations.

    Args:
      title: String used in html title
      locations: list of (lat, lng) tuples
      limit: maximum number of queries in the html
      f: a file object
    """
    output_prefix = (
        """
        <html>
        <head>
        <meta http-equiv="Content-Type" content="text/html; charset=UTF-8">
        <title>%(title)s</title>
        </head>
        <body>
        Random queries for %(title)s<p>
        This list of random queries should speed up important manual testing. Here are
        some things to check when looking at the results of a query.
        <ul>
          <li> Check the agency attribution under the trip results:
          <ul>
            <li> has correct name and spelling of the agency
            <li> opens a page with general information about the service
          </ul>
          <li> For each alternate trip check that each of these is reasonable:
          <ul>
            <li> the total time of the trip
            <li> the time for each leg. Bad data frequently results in a leg going a
            long way in a few minutes.
            <li> the icons and mode names (Tram, Bus, etc) are correct for each leg
            <li> the route names and headsigns are correctly formatted and not
            redundant.
            For a good example see <a
            href="https://developers.google.com/transit/gtfs/examples/display-to-users">
            the screenshots in the Google Transit Feed Specification</a>.
            <li> the shape line on the map looks correct. Make sure the polyline does
            not zig-zag, loop, skip stops or jump far away unless the trip does the
            same thing.
            <li> the route is active on the day the trip planner returns
          </ul>
        </ul>
        If you find a problem be sure to save the URL. This file is generated randomly.
        <ol>
        """
        % locals()
    )

    output_suffix = (
        """
        </ol>
        </body>
        </html>
        """
        % locals()
    )

    f.write(output_prefix)
    for source, destination in zip(
        locations[0:limit], locations[1 : limit + 1]
    ):
        f.write("<li>%s\n" % LatLngsToGoogleLink(source, destination))
    f.write(output_suffix)


def ParentAndBaseName(path):
    """Given a path return only the parent name and file name as a string."""
    dirname, basename = os.path.split(path)
    dirname = dirname.rstrip(os.path.sep)
    if os.path.altsep:
        dirname = dirname.rstrip(os.path.altsep)
    _, parentname = os.path.split(dirname)
    return os.path.join(parentname, basename)


def main():
    usage = """%prog [options] <input GTFS.zip>
        Create an HTML page of random URLs for the Google Maps transit trip
        planner. The queries go between places near stops listed in a
        <input GTFS.zip>.
        By default 50 random URLs are saved to google_random_queries.html.
        For more information see
        https://github.com/google/transitfeed/wiki/GoogleRandomQueries
        """

    parser = optparse.OptionParser(
        usage=usage, version="%prog " + transitfeed.__version__
    )
    parser.add_option(
        "-l",
        "--limit",
        dest="limit",
        type="int",
        help="Maximum number of URLs to generate",
    )
    parser.add_option(
        "-o",
        "--output",
        dest="output",
        metavar="HTML_OUTPUT_PATH",
        help="write HTML output to HTML_OUTPUT_PATH",
    )
    parser.set_defaults(output="google_random_queries.html", limit=50)
    (options, args) = parser.parse_args()
    if len(args) != 1:
        print(parser.format_help(), file=sys.stderr)
        print(
            "\n\nYou must provide the path of a single feed\n\n",
            file=sys.stderr,
        )
        sys.exit(2)
    feed_path = args[0]

    # ProblemReporter prints problems on console.
    loader = transitfeed.Loader(
        feed_path,
        problems=transitfeed.ProblemReporter(),
        load_stop_times=False,
    )
    schedule = loader.Load()
    locations = GetRandomLocationsNearStops(schedule)
    random.shuffle(locations)
    agencies = ", ".join([a.agency_name for a in schedule.GetAgencyList()])
    title = "%s (%s)" % (agencies, ParentAndBaseName(feed_path))

    WriteOutput(title, locations, options.limit, open(options.output, "w"))
    print(
        "Load %s in your web browser. It contains more instructions."
        % options.output
    )


if __name__ == "__main__":
    main()
