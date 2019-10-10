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
Filters out trips which are not on the defualt routes and
  set their trip_typeattribute accordingly.

For usage information run unusual_trip_filter.py --help
"""

__author__ = "Jiri Semecky <jiri.semecky@gmail.com>"

import transitfeed
from transitfeed import util


class UnusualTripFilter(object):
    """Class filtering trips going on unusual paths.

    Those are usually trips going to/from depot or changing to another route
    in the middle. Sets the 'trip_type' attribute of the trips.txt dataset
    so that non-standard trips are marked as special (value 1)
    instead of regular (default value 0).
    """

    def __init__(
        self, threshold=0.1, force=False, quiet=False, route_type=None
    ):
        self._threshold = threshold
        self._quiet = quiet
        self._force = force
        if route_type in transitfeed.Route._ROUTE_TYPE_NAMES:
            self._route_type = transitfeed.Route._ROUTE_TYPE_NAMES[route_type]
        elif route_type is None:
            self._route_type = None
        else:
            self._route_type = int(route_type)

    def filter_line(self, route):
        """Mark unusual trips for the given route."""
        if (
            self._route_type is not None
            and self._route_type != route.route_type
        ):
            self.info(
                "Skipping route %s due to different route_type value (%s)"
                % (route["route_id"], route["route_type"])
            )
            return
        self.info("Filtering infrequent trips for route %s." % route.route_id)
        trip_count = len(route.trips)
        for pattern_id, pattern in list(route.GetPatternIdTripDict().items()):
            ratio = float(1.0 * len(pattern) / trip_count)
            if not self._force:
                if ratio < self._threshold:
                    self.info(
                        "\t%d trips on route %s with headsign '%s' recognized "
                        "as unusual (ratio %f)"
                        % (
                            len(pattern),
                            route["route_short_name"],
                            pattern[0]["trip_headsign"],
                            ratio,
                        )
                    )
                    for trip in pattern:
                        trip.trip_type = 1  # special
                        self.info(
                            "\t\tsetting trip_type of trip %s as special"
                            % trip.trip_id
                        )
            else:
                self.info(
                    "\t%d trips on route %s with headsign '%s' recognized "
                    "as %s (ratio %f)"
                    % (
                        len(pattern),
                        route["route_short_name"],
                        pattern[0]["trip_headsign"],
                        ("regular", "unusual")[ratio < self._threshold],
                        ratio,
                    )
                )
                for trip in pattern:
                    trip.trip_type = ("0", "1")[ratio < self._threshold]
                    self.info(
                        "\t\tsetting trip_type of trip %s as %s"
                        % (
                            trip.trip_id,
                            ("regular", "unusual")[ratio < self._threshold],
                        )
                    )

    def filter(self, dataset):
        """Mark unusual trips for all the routes in the dataset."""
        self.info("Going to filter infrequent routes in the dataset")
        for route in list(dataset.routes.values()):
            self.filter_line(route)

    def info(self, text):
        if not self._quiet:
            print(text.encode("utf-8"))


def main():
    usage = """%prog [options] <GTFS.zip>
        
        Sets the trip_type for trips that have an unusual pattern for a route.
        <GTFS.zip> is overwritten with the modifed GTFS file unless the --output
        option is used.
        
        For more information see
        https://github.com/google/transitfeed/wiki/UnusualTripFilter
        """
    parser = util.OptionParserLongError(
        usage=usage, version="%prog " + transitfeed.__version__
    )
    parser.add_option(
        "-o",
        "--output",
        dest="output",
        metavar="FILE",
        help="Name of the output GTFS file (writing to input feed if omitted).",
    )
    parser.add_option(
        "-m",
        "--memory_db",
        dest="memory_db",
        action="store_true",
        help="Force use of in-memory sqlite db.",
    )
    parser.add_option(
        "-t",
        "--threshold",
        default=0.1,
        dest="threshold",
        type="float",
        help="Frequency threshold for considering pattern as non-regular.",
    )
    parser.add_option(
        "-r",
        "--route_type",
        default=None,
        dest="route_type",
        type="string",
        help="Filter only selected route type (specified by number"
        "or one of the following names: "
        + ", ".join(transitfeed.Route._ROUTE_TYPE_NAMES)
        + ").",
    )
    parser.add_option(
        "-f",
        "--override_trip_type",
        default=False,
        dest="override_trip_type",
        action="store_true",
        help="Forces overwrite of current trip_type values.",
    )
    parser.add_option(
        "-q",
        "--quiet",
        dest="quiet",
        default=False,
        action="store_true",
        help="Suppress information output.",
    )

    (options, args) = parser.parse_args()
    if len(args) != 1:
        parser.error("You must provide the path of a single feed.")

    filter = UnusualTripFilter(
        float(options.threshold),
        force=options.override_trip_type,
        quiet=options.quiet,
        route_type=options.route_type,
    )
    feed_name = args[0]
    feed_name = feed_name.strip()
    filter.info("Loading %s" % feed_name)
    loader = transitfeed.Loader(
        feed_name, extra_validation=True, memory_db=options.memory_db
    )
    data = loader.Load()
    filter.filter(data)
    print("Saving data")

    # Write the result
    if options.output is None:
        data.WriteGoogleTransitFeed(feed_name)
    else:
        data.WriteGoogleTransitFeed(options.output)


if __name__ == "__main__":
    util.RunWithCrashHandler(main)
