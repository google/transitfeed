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


"""Filter the unused stops out of a transit feed file."""

import optparse
import sys

import transitfeed


def main():
    parser = optparse.OptionParser(
        usage="usage: %prog [options] input_feed output_feed",
        version="%prog " + transitfeed.__version__,
    )
    parser.add_option(
        "-l",
        "--list_removed",
        dest="list_removed",
        default=False,
        action="store_true",
        help="Print removed stops to stdout",
    )
    (options, args) = parser.parse_args()
    if len(args) != 2:
        print(parser.format_help(), file=sys.stderr)
        print(
            "\n\nYou must provide input_feed and output_feed\n\n",
            file=sys.stderr,
        )
        sys.exit(2)
    input_path = args[0]
    output_path = args[1]

    loader = transitfeed.Loader(input_path)
    schedule = loader.Load()

    print("Removing unused stops...")
    removed = 0
    for stop_id, stop in list(schedule.stops.items()):
        if not stop.GetTrips(schedule):
            removed += 1
            del schedule.stops[stop_id]
            if options.list_removed:
                print("Removing %s (%s)" % (stop_id, stop.stop_name))
    if removed == 0:
        print("No unused stops.")
    elif removed == 1:
        print("Removed 1 stop")
    else:
        print("Removed %d stops" % removed)

    schedule.WriteGoogleTransitFeed(output_path)


if __name__ == "__main__":
    main()
