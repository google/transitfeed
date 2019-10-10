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

# Unit tests for the route module.


from io import BytesIO

import transitfeed
from tests import util


class RepeatedRouteNameTestCase(util.LoadTestCase):
    def runTest(self):
        self.ExpectInvalidValue("repeated_route_name", "route_long_name")


class SameShortLongNameTestCase(util.LoadTestCase):
    def runTest(self):
        self.ExpectInvalidValue("same_short_long_name", "route_long_name")


class RouteMemoryZipTestCase(util.MemoryZipTestCase):
    def assertLoadAndCheckExtraValues(self, schedule_file):
        """Load file-like schedule_file and check for extra route columns."""
        load_problems = util.GetTestFailureProblemReporter(
            self, ("ExpirationDate", "UnrecognizedColumn")
        )
        loaded_schedule = transitfeed.Loader(
            schedule_file, problems=load_problems, extra_validation=True
        ).Load()
        self.assertEqual("foo", loaded_schedule.GetRoute("t")["t_foo"])
        self.assertEqual("", loaded_schedule.GetRoute("AB")["t_foo"])
        self.assertEqual("bar", loaded_schedule.GetRoute("n")["n_foo"])
        self.assertEqual("", loaded_schedule.GetRoute("AB")["n_foo"])
        # Uncomment the following lines to print the string in testExtraFileColumn
        # print(repr(zipfile.ZipFile(schedule_file).read("routes.txt")))
        # self.fail()

    def testExtraObjectAttribute(self):
        """Extra columns added to an object are preserved when writing."""
        schedule = self.MakeLoaderAndLoad()
        # Add an attribute after AddRouteObject
        route_t = transitfeed.Route(
            short_name="T", route_type="Bus", route_id="t"
        )
        schedule.AddRouteObject(route_t)
        route_t.t_foo = "foo"
        # Add an attribute before AddRouteObject
        route_n = transitfeed.Route(
            short_name="N", route_type="Bus", route_id="n"
        )
        route_n.n_foo = "bar"
        schedule.AddRouteObject(route_n)
        saved_schedule_file = BytesIO()
        schedule.WriteGoogleTransitFeed(saved_schedule_file)
        self.accumulator.AssertNoMoreExceptions()

        self.assertLoadAndCheckExtraValues(saved_schedule_file)

    def testExtraFileColumn(self):
        """Extra columns loaded from a file are preserved when writing."""
        # Uncomment the code in assertLoadAndCheckExtraValues to generate this
        # string.
        self.SetArchiveContents(
            "routes.txt",
            "route_id,agency_id,route_short_name,route_long_name,route_type,"
            "t_foo,n_foo\n"
            "AB,DTA,,Airport Bullfrog,3,,\n"
            "t,DTA,T,,3,foo,\n"
            "n,DTA,N,,3,,bar\n",
        )
        load1_problems = util.GetTestFailureProblemReporter(
            self, ("ExpirationDate", "UnrecognizedColumn")
        )
        schedule = self.MakeLoaderAndLoad(problems=load1_problems)
        saved_schedule_file = BytesIO()
        schedule.WriteGoogleTransitFeed(saved_schedule_file)

        self.assertLoadAndCheckExtraValues(saved_schedule_file)


class RouteConstructorTestCase(util.TestCase):
    def setUp(self):
        self.accumulator = util.RecordingProblemAccumulator(self)
        self.problems = transitfeed.ProblemReporter(self.accumulator)

    def tearDown(self):
        self.accumulator.TearDownAssertNoMoreExceptions()

    def testDefault(self):
        route = transitfeed.Route()
        repr(route)
        self.assertEqual({}, dict(route))
        route.Validate(self.problems)
        repr(route)
        self.assertEqual({}, dict(route))

        e = self.accumulator.PopException("MissingValue")
        self.assertEqual("route_id", e.column_name)
        e = self.accumulator.PopException("MissingValue")
        self.assertEqual("route_type", e.column_name)
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("route_short_name", e.column_name)
        self.accumulator.AssertNoMoreExceptions()

    def testInitArgs(self):
        # route_type name
        route = transitfeed.Route(
            route_id="id1", short_name="22", route_type="Bus"
        )
        repr(route)
        route.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()
        self.assertEqual(3, route.route_type)  # converted to an int
        self.assertEqual(
            {"route_id": "id1", "route_short_name": "22", "route_type": "3"},
            dict(route),
        )

        # route_type as an int
        route = transitfeed.Route(
            route_id="i1", long_name="Twenty 2", route_type=1
        )
        repr(route)
        route.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()
        self.assertEqual(1, route.route_type)  # kept as an int
        self.assertEqual(
            {
                "route_id": "i1",
                "route_long_name": "Twenty 2",
                "route_type": "1",
            },
            dict(route),
        )

        # route_type as a string
        route = transitfeed.Route(
            route_id="id1", short_name="22", route_type="1"
        )
        repr(route)
        route.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()
        self.assertEqual(1, route.route_type)  # converted to an int
        self.assertEqual(
            {"route_id": "id1", "route_short_name": "22", "route_type": "1"},
            dict(route),
        )

        # route_type has undefined int value
        route = transitfeed.Route(
            route_id="id1", short_name="22", route_type="8"
        )
        repr(route)
        route.Validate(self.problems)
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("route_type", e.column_name)
        self.assertEqual(1, e.type)
        self.accumulator.AssertNoMoreExceptions()
        self.assertEqual(
            {"route_id": "id1", "route_short_name": "22", "route_type": "8"},
            dict(route),
        )

        # route_type that doesn't parse
        route = transitfeed.Route(
            route_id="id1", short_name="22", route_type="1foo"
        )
        repr(route)
        route.Validate(self.problems)
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual("route_type", e.column_name)
        self.accumulator.AssertNoMoreExceptions()
        self.assertEqual(
            {
                "route_id": "id1",
                "route_short_name": "22",
                "route_type": "1foo",
            },
            dict(route),
        )

        # agency_id
        route = transitfeed.Route(
            route_id="id1", short_name="22", route_type=1, agency_id="myage"
        )
        repr(route)
        route.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()
        self.assertEqual(
            {
                "route_id": "id1",
                "route_short_name": "22",
                "route_type": "1",
                "agency_id": "myage",
            },
            dict(route),
        )

    def testInitArgOrder(self):
        """Call Route.__init__ without any names so a change in order is noticed."""
        route = transitfeed.Route("short", "long name", "Bus", "r1", "a1")
        self.assertEqual(
            {
                "route_id": "r1",
                "route_short_name": "short",
                "route_long_name": "long name",
                "route_type": "3",
                "agency_id": "a1",
            },
            dict(route),
        )

    def testFieldDict(self):
        route = transitfeed.Route(field_dict={})
        self.assertEqual({}, dict(route))

        route = transitfeed.Route(
            field_dict={
                "route_id": "id1",
                "route_short_name": "22",
                "agency_id": "myage",
                "route_type": "1",
                "bikes_allowed": "1",
            }
        )
        route.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()
        self.assertEqual(
            {
                "route_id": "id1",
                "route_short_name": "22",
                "agency_id": "myage",
                "route_type": "1",
                "bikes_allowed": "1",
            },
            dict(route),
        )

        route = transitfeed.Route(
            field_dict={
                "route_id": "id1",
                "route_short_name": "22",
                "agency_id": "myage",
                "route_type": "1",
                "my_column": "v",
            }
        )
        route.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()
        self.assertEqual(
            {
                "route_id": "id1",
                "route_short_name": "22",
                "agency_id": "myage",
                "route_type": "1",
                "my_column": "v",
            },
            dict(route),
        )
        route._private = 0.3  # Isn't copied
        route_copy = transitfeed.Route(field_dict=route)
        self.assertEqual(
            {
                "route_id": "id1",
                "route_short_name": "22",
                "agency_id": "myage",
                "route_type": "1",
                "my_column": "v",
            },
            dict(route_copy),
        )


class RouteValidationTestCase(util.ValidationTestCase):
    def runTest(self):
        # success case
        route = transitfeed.Route()
        route.route_id = "054C"
        route.route_short_name = "54C"
        route.route_long_name = "South Side - North Side"
        route.route_type = 7
        route.Validate(self.problems)

        # blank short & long names
        route.route_short_name = ""
        route.route_long_name = "    "
        self.ValidateAndExpectInvalidValue(route, "route_short_name")

        # short name too long
        route.route_short_name = "South Side"
        route.route_long_name = ""
        self.ValidateAndExpectInvalidValue(route, "route_short_name")
        route.route_short_name = "M7bis"  # 5 is OK
        route.Validate(self.problems)

        # long name contains short name
        route.route_short_name = "54C"
        route.route_long_name = "54C South Side - North Side"
        self.ValidateAndExpectInvalidValue(route, "route_long_name")
        route.route_long_name = "54C(South Side - North Side)"
        self.ValidateAndExpectInvalidValue(route, "route_long_name")
        route.route_long_name = "54C-South Side - North Side"
        self.ValidateAndExpectInvalidValue(route, "route_long_name")

        # long name is same as short name
        route.route_short_name = "54C"
        route.route_long_name = "54C"
        self.ValidateAndExpectInvalidValue(route, "route_long_name")

        # route description is same as short name
        route.route_desc = "54C"
        route.route_short_name = "54C"
        route.route_long_name = ""
        self.ValidateAndExpectInvalidValue(route, "route_desc")
        route.route_desc = None

        # route description is same as long name
        route.route_desc = "South Side - North Side"
        route.route_long_name = "South Side - North Side"
        self.ValidateAndExpectInvalidValue(route, "route_desc")
        route.route_desc = None

        # invalid route types
        route.route_type = 8
        self.ValidateAndExpectInvalidValue(route, "route_type")
        route.route_type = -1
        self.ValidateAndExpectInvalidValue(route, "route_type")
        route.route_type = 7

        # invalid route URL
        route.route_url = "www.example.com"
        self.ValidateAndExpectInvalidValue(route, "route_url")
        route.route_url = None

        # invalid route color
        route.route_color = "orange"
        self.ValidateAndExpectInvalidValue(route, "route_color")
        route.route_color = None

        # invalid route text color
        route.route_text_color = "orange"
        self.ValidateAndExpectInvalidValue(route, "route_text_color")
        route.route_text_color = None

        # missing route ID
        route.route_id = None
        self.ValidateAndExpectMissingValue(route, "route_id")
        route.route_id = "054C"

        # bad color contrast
        route.route_text_color = None  # black
        route.route_color = "0000FF"  # Bad
        self.ValidateAndExpectInvalidValue(route, "route_color")
        route.route_color = "00BF00"  # OK
        route.Validate(self.problems)
        route.route_color = "005F00"  # Bad
        self.ValidateAndExpectInvalidValue(route, "route_color")
        route.route_color = "FF00FF"  # OK
        route.Validate(self.problems)
        route.route_text_color = "FFFFFF"  # OK too
        route.Validate(self.problems)
        route.route_text_color = "00FF00"  # think of color-blind people!
        self.ValidateAndExpectInvalidValue(route, "route_color")
        route.route_text_color = "007F00"
        route.route_color = "FF0000"
        self.ValidateAndExpectInvalidValue(route, "route_color")
        route.route_color = "00FFFF"  # OK
        route.Validate(self.problems)
        route.route_text_color = None  # black
        route.route_color = None  # white
        route.Validate(self.problems)
        self.accumulator.AssertNoMoreExceptions()

        # bad bikes_allowed
        route.bikes_allowed = "3"
        self.ValidateAndExpectInvalidValue(route, "bikes_allowed")
