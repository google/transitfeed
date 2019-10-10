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

"""Google has a homegrown database for managing the company shuttle. The
database dumps its contents in XML. This scripts converts the proprietary XML
format into a Google Transit Feed Specification file.
"""

import datetime
import os.path
import re
import urllib.error
import urllib.parse
import urllib.request
import xml.etree.ElementTree as ET
from optparse import OptionParser

import transitfeed


class NoUnusedStopExceptionProblemReporter(transitfeed.ProblemReporter):
    """The company shuttle database has a few unused stops for reasons unrelated
    to this script. Ignore them.
    """

    def __init__(self):
        accumulator = transitfeed.ExceptionProblemAccumulator()
        transitfeed.ProblemReporter.__init__(self, accumulator)

    def UnusedStop(self, stop_id, stop_name):
        pass


def SaveFeed(input, output):
    tree = ET.parse(urllib.request.urlopen(input))

    schedule = transitfeed.Schedule()
    service_period = schedule.GetDefaultServicePeriod()
    service_period.SetWeekdayService()
    service_period.SetStartDate("20070314")
    service_period.SetEndDate("20071231")
    # Holidays for 2007
    service_period.SetDateHasService("20070528", has_service=False)
    service_period.SetDateHasService("20070704", has_service=False)
    service_period.SetDateHasService("20070903", has_service=False)
    service_period.SetDateHasService("20071122", has_service=False)
    service_period.SetDateHasService("20071123", has_service=False)
    service_period.SetDateHasService("20071224", has_service=False)
    service_period.SetDateHasService("20071225", has_service=False)
    service_period.SetDateHasService("20071226", has_service=False)
    service_period.SetDateHasService("20071231", has_service=False)

    stops = {}  # Map from xml stop id to python Stop object
    schedule.NewDefaultAgency(
        name="GBus", url="http://shuttle/", timezone="America/Los_Angeles"
    )

    for xml_stop in tree.getiterator("stop"):
        stop = schedule.AddStop(
            lat=float(xml_stop.attrib["lat"]),
            lng=float(xml_stop.attrib["lng"]),
            name=xml_stop.attrib["name"],
        )
        stops[xml_stop.attrib["id"]] = stop

    for xml_shuttleGroup in tree.getiterator("shuttleGroup"):
        if xml_shuttleGroup.attrib["name"] == "Test":
            continue
        r = schedule.AddRoute(
            short_name="",
            long_name=xml_shuttleGroup.attrib["name"],
            route_type="Bus",
        )
        for xml_route in xml_shuttleGroup.getiterator("route"):
            t = r.AddTrip(
                schedule=schedule,
                headsign=xml_route.attrib["name"],
                trip_id=xml_route.attrib["id"],
            )
            trip_stops = []  # Build a list of (time, Stop) tuples
            for xml_schedule in xml_route.getiterator("schedule"):
                trip_stops.append(
                    (
                        int(xml_schedule.attrib["time"]) / 1000,
                        stops[xml_schedule.attrib["stopId"]],
                    )
                )
            trip_stops.sort()  # Sort by time
            for (time, stop) in trip_stops:
                t.AddStopTime(
                    stop=stop, arrival_secs=time, departure_secs=time
                )

    schedule.Validate(problems=NoUnusedStopExceptionProblemReporter())
    schedule.WriteGoogleTransitFeed(output)


def main():
    parser = OptionParser()
    parser.add_option("--input", dest="input", help="Path or URL of input")
    parser.add_option(
        "--output",
        dest="output",
        help="Path of output file. Should end in .zip and if it "
        "contains the substring YYYYMMDD it will be replaced with "
        "today's date. It is impossible to include the literal "
        "string YYYYYMMDD in the path of the output file.",
    )
    parser.add_option(
        "--execute",
        dest="execute",
        help="Commands to run to copy the output. %(path)s is "
        "replaced with full path of the output and %(name)s is "
        "replaced with name part of the path. Try "
        "scp %(path)s myhost:www/%(name)s",
        action="append",
    )
    parser.set_defaults(input=None, output=None, execute=[])
    (options, args) = parser.parse_args()

    today = datetime.date.today().strftime("%Y%m%d")
    options.output = re.sub(r"YYYYMMDD", today, options.output)
    (_, name) = os.path.split(options.output)
    path = options.output

    SaveFeed(options.input, options.output)

    for command in options.execute:
        import subprocess

        def check_call(cmd):
            """Convenience function that is in the docs for subprocess but not
            installed on my system."""
            retcode = subprocess.call(cmd, shell=True)
            if retcode < 0:
                raise Exception(
                    "Child '%s' was terminated by signal %d" % (cmd, -retcode)
                )
            elif retcode != 0:
                raise Exception("Child '%s' returned %d" % (cmd, retcode))

        # path_output and filename_current can be used to run arbitrary commands
        check_call(command % locals())


if __name__ == "__main__":
    main()
