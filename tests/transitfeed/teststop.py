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

# Unit tests for the stop module.


import transitfeed
from tests import util


class StopHierarchyTestCase(util.MemoryZipTestCase):
    def testParentAtSameLatLon(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            "BEATTY_AIRPORT,Airport,36.868446,-116.784582,,STATION\n"
            "STATION,Airport,36.868446,-116.784582,1,\n"
            "BULLFROG,Bullfrog,36.88108,-116.81797,,\n"
            "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        self.assertEqual(1, schedule.stops["STATION"].location_type)
        self.assertEqual(0, schedule.stops["BEATTY_AIRPORT"].location_type)
        self.accumulator.AssertNoMoreExceptions()

    def testBadLocationType(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type\n"
            "BEATTY_AIRPORT,Airport,36.868446,-116.784582,7\n"
            "BULLFROG,Bullfrog,36.88108,-116.81797,notvalid\n"
            "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("location_type", e.column_name)
        self.assertEqual(3, e.row_num)
        self.assertEqual(0, e.type)
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("location_type", e.column_name)
        self.assertEqual(2, e.row_num)
        self.assertEqual(1, e.type)
        self.accumulator.AssertNoMoreExceptions()

    def testBadLocationTypeAtSameLatLon(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            "BEATTY_AIRPORT,Airport,36.868446,-116.784582,,STATION\n"
            "STATION,Airport,36.868446,-116.784582,7,\n"
            "BULLFROG,Bullfrog,36.88108,-116.81797,,\n"
            "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("parent_station", e.column_name)
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("location_type", e.column_name)
        self.assertEqual(3, e.row_num)
        self.accumulator.AssertNoMoreExceptions()

    def testStationUsed(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type\n"
            "BEATTY_AIRPORT,Airport,36.868446,-116.784582,1\n"
            "BULLFROG,Bullfrog,36.88108,-116.81797,\n"
            "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        self.accumulator.PopException("UsedStation")
        self.accumulator.AssertNoMoreExceptions()

    def testParentNotFound(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            "BEATTY_AIRPORT,Airport,36.868446,-116.784582,,STATION\n"
            "BULLFROG,Bullfrog,36.88108,-116.81797,,\n"
            "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("parent_station", e.column_name)
        self.accumulator.AssertNoMoreExceptions()

    def testParentIsStop(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            "BEATTY_AIRPORT,Airport,36.868446,-116.784582,,BULLFROG\n"
            "BULLFROG,Bullfrog,36.88108,-116.81797,,\n"
            "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("parent_station", e.column_name)
        self.accumulator.AssertNoMoreExceptions()

    def testParentOfEntranceIsStop(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            "BEATTY_AIRPORT,Airport,36.868446,-116.784582,7,BULLFROG\n"
            "BULLFROG,Bullfrog,36.88108,-116.81797,,\n"
            "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("parent_station", e.column_name)
        self.assertTrue(e.FormatProblem().find("location_type=1") != -1)
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("location_type", e.column_name)
        self.accumulator.AssertNoMoreExceptions()

    def testStationWithParent(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            "BEATTY_AIRPORT,Airport,36.868446,-116.784582,,STATION\n"
            "STATION,Airport,36.868446,-116.784582,1,STATION2\n"
            "STATION2,Airport 2,36.868000,-116.784000,1,\n"
            "BULLFROG,Bullfrog,36.868088,-116.784797,,STATION2\n"
            "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("parent_station", e.column_name)
        self.assertEqual(3, e.row_num)
        self.accumulator.AssertNoMoreExceptions()

    def testStationWithSelfParent(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            "BEATTY_AIRPORT,Airport,36.868446,-116.784582,,STATION\n"
            "STATION,Airport,36.868446,-116.784582,1,STATION\n"
            "BULLFROG,Bullfrog,36.88108,-116.81797,,\n"
            "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("parent_station", e.column_name)
        self.assertEqual(3, e.row_num)
        self.accumulator.AssertNoMoreExceptions()

    def testStopNearToNonParentStation(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            "BEATTY_AIRPORT,Airport,36.868446,-116.784582,,\n"
            "BULLFROG,Bullfrog,36.868446,-116.784582,,\n"
            "BULLFROG_ST,Bullfrog,36.868446,-116.784582,1,\n"
            "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("StopsTooClose")
        self.assertMatchesRegex("BEATTY_AIRPORT", e.FormatProblem())
        self.assertMatchesRegex("BULLFROG", e.FormatProblem())
        self.assertMatchesRegex("are 0.00m apart", e.FormatProblem())
        e = self.accumulator.PopException("DifferentStationTooClose")
        self.assertMatchesRegex(
            'The parent_station of stop "Airport"', e.FormatProblem()
        )
        e = self.accumulator.PopException("DifferentStationTooClose")
        self.assertMatchesRegex(
            'The parent_station of stop "Bullfrog"', e.FormatProblem()
        )
        self.accumulator.AssertNoMoreExceptions()

    def testStopTooFarFromParentStation(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            "BULLFROG_ST,Bullfrog,36.880,-116.817,1,\n"  # Parent station of all.
            "BEATTY_AIRPORT,Airport,36.880,-116.816,,BULLFROG_ST\n"  # ~ 90m far
            "BULLFROG,Bullfrog,36.881,-116.818,,BULLFROG_ST\n"  # ~ 150m far
            "STAGECOACH,Stagecoach,36.915,-116.751,,BULLFROG_ST\n",
        )  # > 3km far
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("StopTooFarFromParentStation")
        self.assertEqual(0, e.type)  # Error
        self.assertTrue(
            e.FormatProblem().find(
                "Stagecoach (ID STAGECOACH) is too far from its parent"
                " station Bullfrog (ID BULLFROG_ST)"
            )
            != -1
        )
        e = self.accumulator.PopException("StopTooFarFromParentStation")
        self.assertEqual(1, e.type)  # Warning
        self.assertTrue(
            e.FormatProblem().find(
                "Bullfrog (ID BULLFROG) is too far from its parent"
                " station Bullfrog (ID BULLFROG_ST)"
            )
            != -1
        )
        self.accumulator.AssertNoMoreExceptions()

    def testStopTimeZone(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station,"
            "stop_timezone\n"
            "BEATTY_AIRPORT,Airport,36.868446,-116.784582,,STATION,"
            "America/New_York\n"
            "STATION,Airport,36.868446,-116.784582,1,,\n"
            "BULLFROG,Bullfrog,36.88108,-116.81797,,,\n"
            "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,,,\n",
        )
        self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual(1, e.type)  # Warning
        self.assertEqual(2, e.row_num)
        self.assertEqual("stop_timezone", e.column_name)
        self.accumulator.AssertNoMoreExceptions()

    # Uncomment once validation is implemented
    # def testStationWithoutReference(self):
    #  self.SetArchiveContents(
    #      "stops.txt",
    #      "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
    #      "BEATTY_AIRPORT,Airport,36.868446,-116.784582,,\n"
    #      "STATION,Airport,36.868446,-116.784582,1,\n"
    #      "BULLFROG,Bullfrog,36.88108,-116.81797,,\n"
    #      "STAGECOACH,Stagecoach Hotel,36.915682,-116.751677,,\n")
    #  schedule = self.MakeLoaderAndLoad()
    #  e = self.accumulator.PopException("OtherProblem")
    #  self.assertEquals("parent_station", e.column_name)
    #  self.assertEquals(2, e.row_num)
    #  self.accumulator.AssertNoMoreExceptions()


class StopSpacesTestCase(util.MemoryZipTestCase):
    def testFieldsWithSpace(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_code,stop_name,stop_lat,stop_lon,stop_url,location_type,"
            "parent_station\n"
            "BEATTY_AIRPORT, ,Airport,36.868446,-116.784582, , ,\n"
            "BULLFROG,,Bullfrog,36.88108,-116.81797,,,\n"
            "STAGECOACH,,Stagecoach Hotel,36.915682,-116.751677,,,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        self.accumulator.AssertNoMoreExceptions()

    def testFieldsWithEmptyString(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
            'BEATTY_AIRPORT,Airport,"",-116.784582,,\n'
            "BULLFROG,Bullfrog,36.88108,-116.81797,,\n"
            'STAGECOACH,Stagecoach Hotel,36.915682,"",,STAGECOACH-STA\n'
            "STAGECOACH-STA,Stagecoach Hotel Station,36.915682,-116.751677,1,\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("MissingValue")
        self.assertEqual("stop_lat", e.column_name)
        self.assertEqual(2, e.row_num)
        e = self.accumulator.PopException("MissingValue")
        self.assertEqual("stop_lon", e.column_name)
        self.assertEqual(4, e.row_num)
        self.accumulator.AssertNoMoreExceptions()


class StopBlankHeaders(util.MemoryZipTestCase):
    def testBlankHeaderValueAtEnd(self):
        # Modify the stops.txt added by MemoryZipTestCase.setUp. This allows the
        # original stops.txt to be changed without modifying anything in this test.
        # Add a column to the end of every row, leaving the header name blank.
        new = []
        for i, row in enumerate(
            self.GetArchiveContents("stops.txt").split("\n")
        ):
            if i == 0:
                new.append(row + ",")
            elif row:
                new.append(row + "," + str(i))  # Put a junk value in data rows
        self.SetArchiveContents("stops.txt", "\n".join(new))
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("CsvSyntax")
        self.assertTrue(
            e.FormatProblem().find("header row should not contain any blank")
            != -1
        )
        self.accumulator.AssertNoMoreExceptions()

    def testBlankHeaderValueAtStart(self):
        # Modify the stops.txt added by MemoryZipTestCase.setUp. This allows the
        # original stops.txt to be changed without modifying anything in this test.
        # Add a column to the start of every row, leaving the header name blank.
        new = []
        for i, row in enumerate(
            self.GetArchiveContents("stops.txt").split("\n")
        ):
            if i == 0:
                new.append("," + row)
            elif row:
                new.append(str(i) + "," + row)  # Put a junk value in data rows
        self.SetArchiveContents("stops.txt", "\n".join(new))
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("CsvSyntax")
        self.assertTrue(
            e.FormatProblem().find("header row should not contain any blank")
            != -1
        )
        self.accumulator.AssertNoMoreExceptions()

    def testBlankHeaderValueInMiddle(self):
        # Modify the stops.txt added by MemoryZipTestCase.setUp. This allows the
        # original stops.txt to be changed without modifying anything in this test.
        # Add two columns to the start of every row, leaving the second header name
        # blank.
        new = []
        for i, row in enumerate(
            self.GetArchiveContents("stops.txt").split("\n")
        ):
            if i == 0:
                new.append("test_name,," + row)
            elif row:
                # Put a junk value in data rows
                new.append(str(i) + "," + str(i) + "," + row)
        self.SetArchiveContents("stops.txt", "\n".join(new))
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("CsvSyntax")
        self.assertTrue(
            e.FormatProblem().find("header row should not contain any blank")
            != -1
        )
        e = self.accumulator.PopException("UnrecognizedColumn")
        self.assertEqual("test_name", e.column_name)
        self.accumulator.AssertNoMoreExceptions()


class BadLatLonInStopUnitTest(util.ValidationTestCase):
    def runTest(self):
        stop = transitfeed.Stop(
            field_dict={
                "stop_id": "STOP1",
                "stop_name": "Stop one",
                "stop_lat": "0x20",
                "stop_lon": "140.01",
            }
        )
        self.ValidateAndExpectInvalidValue(stop, "stop_lat")

        stop = transitfeed.Stop(
            field_dict={
                "stop_id": "STOP1",
                "stop_name": "Stop one",
                "stop_lat": "13.0",
                "stop_lon": "1e2",
            }
        )
        self.ValidateAndExpectInvalidFloatValue(stop, "1e2")


class BadLatLonInFileUnitTest(util.MemoryZipTestCase):
    def runTest(self):
        self.SetArchiveContents(
            "stops.txt",
            "stop_id,stop_name,stop_lat,stop_lon\n"
            "BEATTY_AIRPORT,Airport,0x20,140.00\n"
            "BULLFROG,Bullfrog,48.20001,140.0123\n"
            "STAGECOACH,Stagecoach Hotel,48.002,bogus\n",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual(2, e.row_num)
        self.assertEqual("stop_lat", e.column_name)
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual(4, e.row_num)
        self.assertEqual("stop_lon", e.column_name)
        self.accumulator.AssertNoMoreExceptions()


class StopValidationTestCase(util.ValidationTestCase):
    def runTest(self):
        # success case
        stop = transitfeed.Stop()
        stop.stop_id = "45"
        stop.stop_name = "Couch AT End Table"
        stop.stop_lat = 50.0
        stop.stop_lon = 50.0
        stop.stop_desc = "Edge of the Couch"
        stop.zone_id = "A"
        stop.stop_url = "http://example.com"
        stop.wheelchair_boarding = "2"
        stop.Validate(self.problems)

        # latitude too large
        stop.stop_lat = 100.0
        self.ValidateAndExpectInvalidValue(stop, "stop_lat")
        stop.stop_lat = 50.0

        # latitude as a string works when it is valid
        # empty strings or whitespaces should get reported as MissingValue
        stop.stop_lat = "50.0"
        stop.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()
        stop.stop_lat = "10f"
        self.ValidateAndExpectInvalidValue(stop, "stop_lat")
        stop.stop_lat = "None"
        self.ValidateAndExpectInvalidValue(stop, "stop_lat")
        stop.stop_lat = ""
        self.ValidateAndExpectMissingValue(stop, "stop_lat")
        stop.stop_lat = " "
        self.ValidateAndExpectMissingValue(stop, "stop_lat")
        stop.stop_lat = 50.0

        # longitude too large
        stop.stop_lon = 200.0
        self.ValidateAndExpectInvalidValue(stop, "stop_lon")
        stop.stop_lon = 50.0

        # longitude as a string works when it is valid
        # empty strings or whitespaces should get reported as MissingValue
        stop.stop_lon = "50.0"
        stop.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()
        stop.stop_lon = "10f"
        self.ValidateAndExpectInvalidValue(stop, "stop_lon")
        stop.stop_lon = "None"
        self.ValidateAndExpectInvalidValue(stop, "stop_lon")
        stop.stop_lon = ""
        self.ValidateAndExpectMissingValue(stop, "stop_lon")
        stop.stop_lon = " "
        self.ValidateAndExpectMissingValue(stop, "stop_lon")
        stop.stop_lon = 50.0

        # lat, lon too close to 0, 0
        stop.stop_lat = 0.0
        stop.stop_lon = 0.0
        self.ValidateAndExpectInvalidValue(stop, "stop_lat")
        stop.stop_lat = 50.0
        stop.stop_lon = 50.0

        # invalid stop_url
        stop.stop_url = "www.example.com"
        self.ValidateAndExpectInvalidValue(stop, "stop_url")
        stop.stop_url = "http://example.com"

        stop.stop_id = "   "
        self.ValidateAndExpectMissingValue(stop, "stop_id")
        stop.stop_id = "45"

        stop.stop_name = ""
        self.ValidateAndExpectMissingValue(stop, "stop_name")
        stop.stop_name = " "
        self.ValidateAndExpectMissingValue(stop, "stop_name")
        stop.stop_name = "Couch AT End Table"

        # description same as name
        stop.stop_desc = "Couch AT End Table"
        self.ValidateAndExpectInvalidValue(stop, "stop_desc")
        stop.stop_desc = "Edge of the Couch"
        self.accumulator.AssertNoMoreExceptions()

        stop.stop_timezone = "This_Timezone/Does_Not_Exist"
        self.ValidateAndExpectInvalidValue(stop, "stop_timezone")
        stop.stop_timezone = "America/Los_Angeles"
        stop.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()

        # invalid wheelchair_boarding
        stop.wheelchair_boarding = "3"
        self.ValidateAndExpectInvalidValue(stop, "wheelchair_boarding")
        stop.wheelchair_boarding = None


class StopAttributes(util.ValidationTestCase):
    def testWithoutSchedule(self):
        stop = transitfeed.Stop()
        stop.Validate(self.problems)
        for name in "stop_id stop_name stop_lat stop_lon".split():
            e = self.accumulator.PopException("MissingValue")
            self.assertEqual(name, e.column_name)
        self.accumulator.AssertNoMoreExceptions()

        stop = transitfeed.Stop()
        # Test behaviour for unset and unknown attribute
        self.assertEqual(stop["new_column"], "")
        try:
            t = stop.new_column
            self.fail("Expecting AttributeError")
        except AttributeError as e:
            pass  # Expected
        stop.stop_id = "a"
        stop.stop_name = "my stop"
        stop.new_column = "val"
        stop.stop_lat = 5.909
        stop.stop_lon = "40.02"
        self.assertEqual(stop.new_column, "val")
        self.assertEqual(stop["new_column"], "val")
        self.assertTrue(isinstance(stop["stop_lat"], str))
        self.assertAlmostEqual(float(stop["stop_lat"]), 5.909)
        self.assertTrue(isinstance(stop["stop_lon"], str))
        self.assertAlmostEqual(float(stop["stop_lon"]), 40.02)
        stop.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()
        # After validation stop.stop_lon has been converted to a float
        self.assertAlmostEqual(stop.stop_lat, 5.909)
        self.assertAlmostEqual(stop.stop_lon, 40.02)
        self.assertEqual(stop.new_column, "val")
        self.assertEqual(stop["new_column"], "val")

    def testBlankAttributeName(self):
        stop1 = transitfeed.Stop(field_dict={"": "a"})
        stop2 = transitfeed.Stop(field_dict=stop1)
        self.assertEqual("a", getattr(stop1, ""))
        # The attribute "" is treated as private and not copied
        self.assertRaises(AttributeError, getattr, stop2, "")
        self.assertEqual(set(), set(stop1.keys()))
        self.assertEqual(set(), set(stop2.keys()))

    def testWithSchedule(self):
        schedule = transitfeed.Schedule(problem_reporter=self.problems)

        stop = transitfeed.Stop(field_dict={})
        # AddStopObject silently fails for Stop objects without stop_id
        schedule.AddStopObject(stop)
        self.assertFalse(schedule.GetStopList())
        self.assertFalse(stop._schedule)

        # Okay to add a stop with only stop_id
        stop = transitfeed.Stop(field_dict={"stop_id": "b"})
        schedule.AddStopObject(stop)
        stop.Validate(self.problems)
        for name in "stop_name stop_lat stop_lon".split():
            e = self.accumulator.PopException("MissingValue")
            self.assertEqual(name, e.column_name)
        self.accumulator.AssertNoMoreExceptions()

        stop.new_column = "val"
        self.assertTrue("new_column" in schedule.GetTableColumns("stops"))

        # Adding a duplicate stop_id fails
        schedule.AddStopObject(transitfeed.Stop(field_dict={"stop_id": "b"}))
        self.accumulator.PopException("DuplicateID")
        self.accumulator.AssertNoMoreExceptions()
