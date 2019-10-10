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

# Unit tests for the transfer module.


from io import BytesIO

import transitfeed
from tests import util


class TransferObjectTestCase(util.ValidationTestCase):
    def testValidation(self):
        # Totally bogus data shouldn't cause a crash
        transfer = transitfeed.Transfer(field_dict={"ignored": "foo"})
        self.assertEqual(0, transfer.transfer_type)

        transfer = transitfeed.Transfer(
            from_stop_id="S1", to_stop_id="S2", transfer_type="1"
        )
        self.assertEqual("S1", transfer.from_stop_id)
        self.assertEqual("S2", transfer.to_stop_id)
        self.assertEqual(1, transfer.transfer_type)
        self.assertEqual(None, transfer.min_transfer_time)
        # references to other tables aren't checked without schedule so this
        # validates even though from_stop_id and to_stop_id are invalid.
        transfer.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()
        self.assertEqual("S1", transfer.from_stop_id)
        self.assertEqual("S2", transfer.to_stop_id)
        self.assertEqual(1, transfer.transfer_type)
        self.assertEqual(None, transfer.min_transfer_time)
        self.accumulator.AssertNoMoreExceptions()

        transfer = transitfeed.Transfer(
            field_dict={
                "from_stop_id": "S1",
                "to_stop_id": "S2",
                "transfer_type": "2",
                "min_transfer_time": "2",
            }
        )
        self.assertEqual("S1", transfer.from_stop_id)
        self.assertEqual("S2", transfer.to_stop_id)
        self.assertEqual(2, transfer.transfer_type)
        self.assertEqual(2, transfer.min_transfer_time)
        transfer.Validate(self.problems)
        self.assertEqual("S1", transfer.from_stop_id)
        self.assertEqual("S2", transfer.to_stop_id)
        self.assertEqual(2, transfer.transfer_type)
        self.assertEqual(2, transfer.min_transfer_time)
        self.accumulator.AssertNoMoreExceptions()

        transfer = transitfeed.Transfer(
            field_dict={
                "from_stop_id": "S1",
                "to_stop_id": "S2",
                "transfer_type": "-4",
                "min_transfer_time": "2",
            }
        )
        self.assertEqual("S1", transfer.from_stop_id)
        self.assertEqual("S2", transfer.to_stop_id)
        self.assertEqual("-4", transfer.transfer_type)
        self.assertEqual(2, transfer.min_transfer_time)
        transfer.Validate(self.problems)
        e = self.accumulator.PopInvalidValue("transfer_type")
        e = self.accumulator.PopException(
            "MinimumTransferTimeSetWithInvalidTransferType"
        )
        self.assertEqual("S1", transfer.from_stop_id)
        self.assertEqual("S2", transfer.to_stop_id)
        self.assertEqual("-4", transfer.transfer_type)
        self.assertEqual(2, transfer.min_transfer_time)

        transfer = transitfeed.Transfer(
            field_dict={
                "from_stop_id": "S1",
                "to_stop_id": "S2",
                "transfer_type": "",
                "min_transfer_time": "-1",
            }
        )
        self.assertEqual(0, transfer.transfer_type)
        transfer.Validate(self.problems)
        # It's negative *and* transfer_type is not 2
        e = self.accumulator.PopException(
            "MinimumTransferTimeSetWithInvalidTransferType"
        )
        e = self.accumulator.PopInvalidValue("min_transfer_time")

        # Non-integer min_transfer_time with transfer_type == 2
        transfer = transitfeed.Transfer(
            field_dict={
                "from_stop_id": "S1",
                "to_stop_id": "S2",
                "transfer_type": "2",
                "min_transfer_time": "foo",
            }
        )
        self.assertEqual("foo", transfer.min_transfer_time)
        transfer.Validate(self.problems)
        e = self.accumulator.PopInvalidValue("min_transfer_time")

        # Non-integer min_transfer_time with transfer_type != 2
        transfer = transitfeed.Transfer(
            field_dict={
                "from_stop_id": "S1",
                "to_stop_id": "S2",
                "transfer_type": "1",
                "min_transfer_time": "foo",
            }
        )
        self.assertEqual("foo", transfer.min_transfer_time)
        transfer.Validate(self.problems)
        # It's not an integer *and* transfer_type is not 2
        e = self.accumulator.PopException(
            "MinimumTransferTimeSetWithInvalidTransferType"
        )
        e = self.accumulator.PopInvalidValue("min_transfer_time")

        # Fractional min_transfer_time with transfer_type == 2
        transfer = transitfeed.Transfer(
            field_dict={
                "from_stop_id": "S1",
                "to_stop_id": "S2",
                "transfer_type": "2",
                "min_transfer_time": "2.5",
            }
        )
        self.assertEqual("2.5", transfer.min_transfer_time)
        transfer.Validate(self.problems)
        e = self.accumulator.PopInvalidValue("min_transfer_time")

        # Fractional min_transfer_time with transfer_type != 2
        transfer = transitfeed.Transfer(
            field_dict={
                "from_stop_id": "S1",
                "to_stop_id": "S2",
                "transfer_type": "1",
                "min_transfer_time": "2.5",
            }
        )
        self.assertEqual("2.5", transfer.min_transfer_time)
        transfer.Validate(self.problems)
        # It's not an integer *and* transfer_type is not 2
        e = self.accumulator.PopException(
            "MinimumTransferTimeSetWithInvalidTransferType"
        )
        e = self.accumulator.PopInvalidValue("min_transfer_time")

        # simple successes
        transfer = transitfeed.Transfer()
        transfer.from_stop_id = "S1"
        transfer.to_stop_id = "S2"
        transfer.transfer_type = 0
        repr(transfer)  # shouldn't crash
        transfer.Validate(self.problems)
        transfer.transfer_type = 3
        transfer.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()

        # transfer_type is out of range
        transfer.transfer_type = 4
        self.ValidateAndExpectInvalidValue(transfer, "transfer_type")
        transfer.transfer_type = -1
        self.ValidateAndExpectInvalidValue(transfer, "transfer_type")
        transfer.transfer_type = "text"
        self.ValidateAndExpectInvalidValue(transfer, "transfer_type")
        transfer.transfer_type = 2

        # invalid min_transfer_time
        transfer.min_transfer_time = -1
        self.ValidateAndExpectInvalidValue(transfer, "min_transfer_time")
        transfer.min_transfer_time = "text"
        self.ValidateAndExpectInvalidValue(transfer, "min_transfer_time")
        transfer.min_transfer_time = 4 * 3600
        transfer.Validate(self.problems)
        e = self.accumulator.PopInvalidValue("min_transfer_time")
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        transfer.min_transfer_time = 25 * 3600
        transfer.Validate(self.problems)
        e = self.accumulator.PopInvalidValue("min_transfer_time")
        self.assertEqual(e.type, transitfeed.TYPE_ERROR)
        transfer.min_transfer_time = 250
        transfer.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()

        # missing stop ids
        transfer.from_stop_id = ""
        self.ValidateAndExpectMissingValue(transfer, "from_stop_id")
        transfer.from_stop_id = "S1"
        transfer.to_stop_id = None
        self.ValidateAndExpectMissingValue(transfer, "to_stop_id")
        transfer.to_stop_id = "S2"

        # from_stop_id and to_stop_id are present in schedule
        schedule = transitfeed.Schedule()
        # 597m appart
        stop1 = schedule.AddStop(57.5, 30.2, "stop 1")
        stop2 = schedule.AddStop(57.5, 30.21, "stop 2")
        transfer = transitfeed.Transfer(schedule=schedule)
        transfer.from_stop_id = stop1.stop_id
        transfer.to_stop_id = stop2.stop_id
        transfer.transfer_type = 2
        transfer.min_transfer_time = 600
        repr(transfer)  # shouldn't crash
        transfer.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()

        # only from_stop_id is present in schedule
        schedule = transitfeed.Schedule()
        stop1 = schedule.AddStop(57.5, 30.2, "stop 1")
        transfer = transitfeed.Transfer(schedule=schedule)
        transfer.from_stop_id = stop1.stop_id
        transfer.to_stop_id = "unexist"
        transfer.transfer_type = 2
        transfer.min_transfer_time = 250
        self.ValidateAndExpectInvalidValue(transfer, "to_stop_id")
        transfer.from_stop_id = "unexist"
        transfer.to_stop_id = stop1.stop_id
        self.ValidateAndExpectInvalidValue(transfer, "from_stop_id")
        self.accumulator.AssertNoMoreExceptions()

        # Transfer can only be added to a schedule once because _schedule is set
        transfer = transitfeed.Transfer()
        transfer.from_stop_id = stop1.stop_id
        transfer.to_stop_id = stop1.stop_id
        schedule.AddTransferObject(transfer)
        self.assertRaises(AssertionError, schedule.AddTransferObject, transfer)

    def testValidationSpeedDistanceAllTransferTypes(self):
        schedule = transitfeed.Schedule()
        transfer = transitfeed.Transfer(schedule=schedule)
        stop1 = schedule.AddStop(1, 0, "stop 1")
        stop2 = schedule.AddStop(0, 1, "stop 2")
        transfer = transitfeed.Transfer(schedule=schedule)
        transfer.from_stop_id = stop1.stop_id
        transfer.to_stop_id = stop2.stop_id
        for transfer_type in [0, 1, 2, 3]:
            transfer.transfer_type = transfer_type

            # from_stop_id and to_stop_id are present in schedule
            # and a bit far away (should be warning)
            # 2303m appart
            stop1.stop_lat = 57.5
            stop1.stop_lon = 30.32
            stop2.stop_lat = 57.52
            stop2.stop_lon = 30.33
            transfer.min_transfer_time = 2500
            repr(transfer)  # shouldn't crash
            transfer.Validate(self.problems)
            if transfer_type != 2:
                e = self.accumulator.PopException(
                    "MinimumTransferTimeSetWithInvalidTransferType"
                )
                self.assertEqual(e.transfer_type, transfer.transfer_type)
            e = self.accumulator.PopException("TransferDistanceTooBig")
            self.assertEqual(e.type, transitfeed.TYPE_WARNING)
            self.assertEqual(e.from_stop_id, stop1.stop_id)
            self.assertEqual(e.to_stop_id, stop2.stop_id)
            self.accumulator.AssertNoMoreExceptions()

            # from_stop_id and to_stop_id are present in schedule
            # and too far away (should be error)
            # 11140m appart
            stop1.stop_lat = 57.5
            stop1.stop_lon = 30.32
            stop2.stop_lat = 57.4
            stop2.stop_lon = 30.33
            transfer.min_transfer_time = 3600
            repr(transfer)  # shouldn't crash
            transfer.Validate(self.problems)
            if transfer_type != 2:
                e = self.accumulator.PopException(
                    "MinimumTransferTimeSetWithInvalidTransferType"
                )
                self.assertEqual(e.transfer_type, transfer.transfer_type)
            e = self.accumulator.PopException("TransferDistanceTooBig")
            self.assertEqual(e.type, transitfeed.TYPE_ERROR)
            self.assertEqual(e.from_stop_id, stop1.stop_id)
            self.assertEqual(e.to_stop_id, stop2.stop_id)
            e = self.accumulator.PopException("TransferWalkingSpeedTooFast")
            self.assertEqual(e.type, transitfeed.TYPE_WARNING)
            self.assertEqual(e.from_stop_id, stop1.stop_id)
            self.assertEqual(e.to_stop_id, stop2.stop_id)
            self.accumulator.AssertNoMoreExceptions()

    def testSmallTransferTimeTriggersWarning(self):
        # from_stop_id and to_stop_id are present in schedule
        # and transfer time is too small
        schedule = transitfeed.Schedule()
        # 298m appart
        stop1 = schedule.AddStop(57.5, 30.2, "stop 1")
        stop2 = schedule.AddStop(57.5, 30.205, "stop 2")
        transfer = transitfeed.Transfer(schedule=schedule)
        transfer.from_stop_id = stop1.stop_id
        transfer.to_stop_id = stop2.stop_id
        transfer.transfer_type = 2
        transfer.min_transfer_time = 1
        repr(transfer)  # shouldn't crash
        transfer.Validate(self.problems)
        e = self.accumulator.PopException("TransferWalkingSpeedTooFast")
        self.assertEqual(e.type, transitfeed.TYPE_WARNING)
        self.assertEqual(e.from_stop_id, stop1.stop_id)
        self.assertEqual(e.to_stop_id, stop2.stop_id)
        self.accumulator.AssertNoMoreExceptions()

    def testVeryCloseStationsDoNotTriggerWarning(self):
        # from_stop_id and to_stop_id are present in schedule
        # and transfer time is too small, but stations
        # are very close together.
        schedule = transitfeed.Schedule()
        # 239m appart
        stop1 = schedule.AddStop(57.5, 30.2, "stop 1")
        stop2 = schedule.AddStop(57.5, 30.204, "stop 2")
        transfer = transitfeed.Transfer(schedule=schedule)
        transfer.from_stop_id = stop1.stop_id
        transfer.to_stop_id = stop2.stop_id
        transfer.transfer_type = 2
        transfer.min_transfer_time = 1
        repr(transfer)  # shouldn't crash
        transfer.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()

    def testCustomAttribute(self):
        """Add unknown attributes to a Transfer and make sure they are saved."""
        transfer = transitfeed.Transfer()
        transfer.attr1 = "foo1"
        schedule = self.SimpleSchedule()
        transfer.to_stop_id = "stop1"
        transfer.from_stop_id = "stop1"
        schedule.AddTransferObject(transfer)
        transfer.attr2 = "foo2"

        saved_schedule_file = BytesIO()
        schedule.WriteGoogleTransitFeed(saved_schedule_file)
        self.accumulator.AssertNoMoreExceptions()

        # Ignore NoServiceExceptions error to keep the test simple
        load_problems = util.GetTestFailureProblemReporter(
            self,
            ("ExpirationDate", "UnrecognizedColumn", "NoServiceExceptions"),
        )
        loaded_schedule = transitfeed.Loader(
            saved_schedule_file, problems=load_problems, extra_validation=True
        ).Load()
        transfers = loaded_schedule.GetTransferList()
        self.assertEqual(1, len(transfers))
        self.assertEqual("foo1", transfers[0].attr1)
        self.assertEqual("foo1", transfers[0]["attr1"])
        self.assertEqual("foo2", transfers[0].attr2)
        self.assertEqual("foo2", transfers[0]["attr2"])

    def testDuplicateId(self):
        schedule = self.SimpleSchedule()
        transfer1 = transitfeed.Transfer(
            from_stop_id="stop1", to_stop_id="stop2"
        )
        schedule.AddTransferObject(transfer1)
        transfer2 = transitfeed.Transfer(field_dict=transfer1)
        transfer2.transfer_type = 3
        schedule.AddTransferObject(transfer2)
        transfer2.Validate()
        e = self.accumulator.PopException("DuplicateID")
        self.assertEqual("(from_stop_id, to_stop_id)", e.column_name)
        self.assertEqual("(stop1, stop2)", e.value)
        self.assertTrue(e.IsWarning())
        self.accumulator.AssertNoMoreExceptions()
        # Check that both transfers were kept
        self.assertEqual(transfer1, schedule.GetTransferList()[0])
        self.assertEqual(transfer2, schedule.GetTransferList()[1])

        # Adding a transfer with a different ID shouldn't cause a problem report.
        transfer3 = transitfeed.Transfer(
            from_stop_id="stop1", to_stop_id="stop3"
        )
        schedule.AddTransferObject(transfer3)
        self.assertEqual(3, len(schedule.GetTransferList()))
        self.accumulator.AssertNoMoreExceptions()

        # GetTransferIter should return all Transfers
        transfer4 = transitfeed.Transfer(from_stop_id="stop1")
        schedule.AddTransferObject(transfer4)
        self.assertEqual(
            ",stop2,stop2,stop3",
            ",".join(
                sorted(t["to_stop_id"] for t in schedule.GetTransferIter())
            ),
        )
        self.accumulator.AssertNoMoreExceptions()


class TransferValidationTestCase(util.MemoryZipTestCase):
    """Integration test for transfers."""

    def testInvalidStopIds(self):
        self.SetArchiveContents(
            "transfers.txt",
            "from_stop_id,to_stop_id,transfer_type\n"
            "DOESNOTEXIST,BULLFROG,2\n"
            ",BULLFROG,2\n"
            "BULLFROG,,2\n"
            "BULLFROG,DOESNOTEXISTEITHER,2\n"
            "DOESNOTEXIT,DOESNOTEXISTEITHER,2\n"
            ",,2\n",
        )
        schedule = self.MakeLoaderAndLoad()
        # First row
        e = self.accumulator.PopInvalidValue("from_stop_id")
        # Second row
        e = self.accumulator.PopMissingValue("from_stop_id")
        # Third row
        e = self.accumulator.PopMissingValue("to_stop_id")
        # Fourth row
        e = self.accumulator.PopInvalidValue("to_stop_id")
        # Fifth row
        e = self.accumulator.PopInvalidValue("from_stop_id")
        e = self.accumulator.PopInvalidValue("to_stop_id")
        # Sixth row
        e = self.accumulator.PopMissingValue("from_stop_id")
        e = self.accumulator.PopMissingValue("to_stop_id")
        self.accumulator.AssertNoMoreExceptions()

    def testDuplicateTransfer(self):
        self.AppendToArchiveContents(
            "stops.txt",
            "BEATTY_AIRPORT_HANGER,Airport Hanger,36.868178,-116.784915\n"
            "BEATTY_AIRPORT_34,Runway 34,36.85352,-116.786316\n",
        )
        self.AppendToArchiveContents("trips.txt", "AB,FULLW,AIR1\n")
        self.AppendToArchiveContents(
            "stop_times.txt",
            "AIR1,7:00:00,7:00:00,BEATTY_AIRPORT_HANGER,1\n"
            "AIR1,7:05:00,7:05:00,BEATTY_AIRPORT_34,2\n"
            "AIR1,7:10:00,7:10:00,BEATTY_AIRPORT_HANGER,3\n",
        )
        self.SetArchiveContents(
            "transfers.txt",
            "from_stop_id,to_stop_id,transfer_type\n"
            "BEATTY_AIRPORT,BEATTY_AIRPORT_HANGER,0\n"
            "BEATTY_AIRPORT,BEATTY_AIRPORT_HANGER,3",
        )
        schedule = self.MakeLoaderAndLoad()
        e = self.accumulator.PopException("DuplicateID")
        self.assertEqual("(from_stop_id, to_stop_id)", e.column_name)
        self.assertEqual("(BEATTY_AIRPORT, BEATTY_AIRPORT_HANGER)", e.value)
        self.assertTrue(e.IsWarning())
        self.assertEqual("transfers.txt", e.file_name)
        self.assertEqual(3, e.row_num)
        self.accumulator.AssertNoMoreExceptions()

        saved_schedule_file = BytesIO()
        schedule.WriteGoogleTransitFeed(saved_schedule_file)
        self.accumulator.AssertNoMoreExceptions()
        load_problems = util.GetTestFailureProblemReporter(
            self, ("ExpirationDate", "DuplicateID")
        )
        loaded_schedule = transitfeed.Loader(
            saved_schedule_file, problems=load_problems, extra_validation=True
        ).Load()
        self.assertEqual(
            [0, 3],
            [int(t.transfer_type) for t in loaded_schedule.GetTransferIter()],
        )
