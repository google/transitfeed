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

# Editor of stop locations in GTFS feed.
#
# This is an extension of schedule_viewer.py, which allows editing
# of stop location usig drag'n'drop.
# You must provide a Google Maps API key.
#
# Usage:
# location_editor.py --key `cat key` --port 8765 --feed_filename feed.zip


import schedule_viewer


class LocationEditorRequestHandler(schedule_viewer.ScheduleRequestHandler):
    def handle_json_GET_setstoplocation(self, params):
        schedule = self.server.schedule
        stop_id = params.get("id", None)
        lat = params.get("lat", -1)
        lon = params.get("lng", -1)
        stop = schedule.GetStop(stop_id)
        if stop is None:
            msg = "Stop with id=" + stop_id + "not found."
        else:
            stop.stop_lat = float(lat)
            stop.stop_lon = float(lon)
            msg = (
                "Location of "
                + stop["stop_name"]
                + "("
                + stop_id
                + ") set to "
                + lat
                + "x"
                + lon
            )
        print(msg)
        return msg

    def handle_json_GET_savedata(self, params):
        schedule = self.server.schedule
        if not self.server.feed_path:
            msg = "Feed path not defined"
        else:
            schedule.WriteGoogleTransitFeed(self.server.feed_path)
            msg = "Data saved to " + self.server.feed_path
        print(msg)
        return msg

    def AllowEditMode(self):
        return True


if __name__ == "__main__":
    schedule_viewer.main(LocationEditorRequestHandler)
