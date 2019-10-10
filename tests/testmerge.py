#!/usr/bin/python3
#
# Copyright 2007 Google Inc. All Rights Reserved.
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

"""Unit tests for the merge module."""

__author__ = "timothy.stranex@gmail.com (Timothy Stranex)"

import os.path
import re
import unittest
import zipfile
from io import StringIO, BytesIO

import merge
import transitfeed
from tests import util


def CheckAttribs(a, b, attrs, assertEquals):
    """Checks that the objects a and b have the same values for the attributes
    given in attrs. These checks are done using the given assert function.

    Args:
      a: The first object.
      b: The second object.
      attrs: The list of attribute names (strings).
      assertEquals: The assertEquals method from unittest.TestCase.
    """
    # For Stop objects (and maybe others in the future) Validate converts some
    # attributes from string to native type
    a.Validate()
    b.Validate()
    for k in attrs:
        assertEquals(getattr(a, k), getattr(b, k))


def CreateAgency():
    """Create an transitfeed.Agency object for testing.

    Returns:
      The agency object.
    """
    return transitfeed.Agency(
        name="agency",
        url="http://agency",
        timezone="Africa/Johannesburg",
        id="agency",
    )


class TestingProblemReporter(merge.MergeProblemReporter):
    def __init__(self, accumulator):
        merge.MergeProblemReporter.__init__(self, accumulator)


class TestingProblemAccumulator(transitfeed.ProblemAccumulatorInterface):
    """This problem reporter keeps track of all problems.

    Attributes:
      problems: The list of problems reported.
    """

    def __init__(self):
        self.problems = []
        self._expect_classes = []

    def _Report(self, problem):
        problem.FormatProblem()  # Shouldn't crash
        self.problems.append(problem)
        for problem_class in self._expect_classes:
            if isinstance(problem, problem_class):
                return
        raise problem

    def CheckReported(self, problem_class):
        """Checks if a problem of the given class was reported.

        Args:
          problem_class: The problem class, a class inheriting from
                         MergeProblemWithContext.

        Returns:
          True if a matching problem was reported.
        """
        for problem in self.problems:
            if isinstance(problem, problem_class):
                return True
        return False

    def ExpectProblemClass(self, problem_class):
        """Supresses exception raising for problems inheriting from this class.

        Args:
          problem_class: The problem class, a class inheriting from
                         MergeProblemWithContext.
        """
        self._expect_classes.append(problem_class)

    def assertExpectedProblemsReported(self, testcase):
        """Asserts that every expected problem class has been reported.

        The assertions are done using the assert_ method of the testcase.

        Args:
          testcase: The unittest.TestCase instance.
        """
        for problem_class in self._expect_classes:
            testcase.assertTrue(self.CheckReported(problem_class))


class TestApproximateDistanceBetweenPoints(util.TestCase):
    def _assertWithinEpsilon(self, a, b, epsilon=1.0):
        """Asserts that a and b are equal to within an epsilon.

        Args:
          a: The first value (float).
          b: The second value (float).
          epsilon: The epsilon value (float).
        """
        self.assertTrue(abs(a - b) < epsilon)

    def testDegenerate(self):
        p = (30.0, 30.0)
        self._assertWithinEpsilon(
            merge.ApproximateDistanceBetweenPoints(p, p), 0.0
        )

    def testFar(self):
        p1 = (30.0, 30.0)
        p2 = (40.0, 40.0)
        self.assertTrue(merge.ApproximateDistanceBetweenPoints(p1, p2) > 1e4)


class TestSchemedMerge(util.TestCase):
    class TestEntity:
        """A mock entity (like Route or Stop) for testing."""

        def __init__(self, x, y, z):
            self.x = x
            self.y = y
            self.z = z

    def setUp(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        accumulator = TestingProblemAccumulator()
        self.fm = merge.FeedMerger(
            a_schedule,
            b_schedule,
            merged_schedule,
            TestingProblemReporter(accumulator),
        )
        self.ds = merge.DataSetMerger(self.fm)

        def Migrate(ent, sched, newid):
            """A migration function for the mock entity."""
            return self.TestEntity(ent.x, ent.y, ent.z)

        self.ds._Migrate = Migrate

    def testMergeIdentical(self):
        class TestAttrib:
            """An object that is equal to everything."""

            def __eq__(self, b):
                return True

        x = 99
        a = TestAttrib()
        b = TestAttrib()

        self.assertTrue(self.ds._MergeIdentical(x, x) == x)
        self.assertTrue(self.ds._MergeIdentical(a, b) is b)
        self.assertRaises(merge.MergeError, self.ds._MergeIdentical, 1, 2)

    def testMergeIdenticalCaseInsensitive(self):
        self.assertTrue(
            self.ds._MergeIdenticalCaseInsensitive("abc", "ABC") == "ABC"
        )
        self.assertTrue(
            self.ds._MergeIdenticalCaseInsensitive("abc", "AbC") == "AbC"
        )
        self.assertRaises(
            merge.MergeError,
            self.ds._MergeIdenticalCaseInsensitive,
            "abc",
            "bcd",
        )
        self.assertRaises(
            merge.MergeError,
            self.ds._MergeIdenticalCaseInsensitive,
            "abc",
            "ABCD",
        )

    def testMergeOptional(self):
        x = 99
        y = 100

        self.assertEqual(self.ds._MergeOptional(None, None), None)
        self.assertEqual(self.ds._MergeOptional(None, x), x)
        self.assertEqual(self.ds._MergeOptional(x, None), x)
        self.assertEqual(self.ds._MergeOptional(x, x), x)
        self.assertRaises(merge.MergeError, self.ds._MergeOptional, x, y)

    def testMergeSameAgency(self):
        kwargs = {
            "name": "xxx",
            "agency_url": "http://www.example.com",
            "agency_timezone": "Europe/Zurich",
        }
        id1 = "agency1"
        id2 = "agency2"
        id3 = "agency3"
        id4 = "agency4"
        id5 = "agency5"

        a = self.fm.a_schedule.NewDefaultAgency(id=id1, **kwargs)
        b = self.fm.b_schedule.NewDefaultAgency(id=id2, **kwargs)
        c = transitfeed.Agency(id=id3, **kwargs)
        self.fm.merged_schedule.AddAgencyObject(c)
        self.fm.Register(a, b, c)

        d = transitfeed.Agency(id=id4, **kwargs)
        e = transitfeed.Agency(id=id5, **kwargs)
        self.fm.a_schedule.AddAgencyObject(d)
        self.fm.merged_schedule.AddAgencyObject(e)
        self.fm.Register(d, None, e)

        self.assertEqual(self.ds._MergeSameAgency(id1, id2), id3)
        self.assertEqual(self.ds._MergeSameAgency(None, None), id3)
        self.assertEqual(self.ds._MergeSameAgency(id1, None), id3)
        self.assertEqual(self.ds._MergeSameAgency(None, id2), id3)

        # id1 is not a valid agency_id in the new schedule so it cannot be merged
        self.assertRaises(KeyError, self.ds._MergeSameAgency, id1, id1)

        # this fails because d (id4) and b (id2) don't map to the same agency
        # in the merged schedule
        self.assertRaises(merge.MergeError, self.ds._MergeSameAgency, id4, id2)

    def testSchemedMerge_Success(self):
        def Merger(a, b):
            return a + b

        scheme = {"x": Merger, "y": Merger, "z": Merger}
        a = self.TestEntity(1, 2, 3)
        b = self.TestEntity(4, 5, 6)
        c = self.ds._SchemedMerge(scheme, a, b)

        self.assertEqual(c.x, 5)
        self.assertEqual(c.y, 7)
        self.assertEqual(c.z, 9)

    def testSchemedMerge_Failure(self):
        def Merger(a, b):
            raise merge.MergeError()

        scheme = {"x": Merger, "y": Merger, "z": Merger}
        a = self.TestEntity(1, 2, 3)
        b = self.TestEntity(4, 5, 6)

        self.assertRaises(
            merge.MergeError, self.ds._SchemedMerge, scheme, a, b
        )

    def testSchemedMerge_NoNewId(self):
        class TestDataSetMerger(merge.DataSetMerger):
            def _Migrate(self, entity, schedule, newid):
                self.newid = newid
                return entity

        dataset_merger = TestDataSetMerger(self.fm)
        a = self.TestEntity(1, 2, 3)
        b = self.TestEntity(4, 5, 6)
        dataset_merger._SchemedMerge({}, a, b)
        self.assertEqual(dataset_merger.newid, False)

    def testSchemedMerge_ErrorTextContainsAttributeNameAndReason(self):
        reason = "my reason"
        attribute_name = "long_attribute_name"

        def GoodMerger(a, b):
            return a + b

        def BadMerger(a, b):
            raise merge.MergeError(reason)

        a = self.TestEntity(1, 2, 3)
        setattr(a, attribute_name, 1)
        b = self.TestEntity(4, 5, 6)
        setattr(b, attribute_name, 2)
        scheme = {
            "x": GoodMerger,
            "y": GoodMerger,
            "z": GoodMerger,
            attribute_name: BadMerger,
        }

        try:
            self.ds._SchemedMerge(scheme, a, b)
        except merge.MergeError as merge_error:
            error_text = str(merge_error)
            self.assertTrue(reason in error_text)
            self.assertTrue(attribute_name in error_text)


class TestFeedMerger(util.TestCase):
    class Merger:
        def __init__(self, test, n, should_fail=False):
            self.test = test
            self.n = n
            self.should_fail = should_fail

        def MergeDataSets(self):
            self.test.called.append(self.n)
            return not self.should_fail

    def setUp(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        accumulator = TestingProblemAccumulator()
        self.fm = merge.FeedMerger(
            a_schedule,
            b_schedule,
            merged_schedule,
            TestingProblemReporter(accumulator),
        )
        self.called = []

    def testSequence(self):
        for i in range(10):
            self.fm.AddMerger(TestFeedMerger.Merger(self, i))
        self.assertTrue(self.fm.MergeSchedules())
        self.assertEqual(self.called, list(range(10)))

    def testStopsAfterError(self):
        for i in range(10):
            self.fm.AddMerger(TestFeedMerger.Merger(self, i, i == 5))
        self.assertTrue(not self.fm.MergeSchedules())
        self.assertEqual(self.called, list(range(6)))

    def testRegister(self):
        s1 = transitfeed.Stop(stop_id="1")
        s2 = transitfeed.Stop(stop_id="2")
        s3 = transitfeed.Stop(stop_id="3")
        self.fm.Register(s1, s2, s3)
        self.assertEqual(self.fm.a_merge_map, {s1: s3})
        self.assertEqual("3", s1._migrated_entity.stop_id)
        self.assertEqual(self.fm.b_merge_map, {s2: s3})
        self.assertEqual("3", s2._migrated_entity.stop_id)

    def testRegisterNone(self):
        s2 = transitfeed.Stop(stop_id="2")
        s3 = transitfeed.Stop(stop_id="3")
        self.fm.Register(None, s2, s3)
        self.assertEqual(self.fm.a_merge_map, {})
        self.assertEqual(self.fm.b_merge_map, {s2: s3})
        self.assertEqual("3", s2._migrated_entity.stop_id)

    def testGenerateId_Prefix(self):
        x = "test"
        a = self.fm.GenerateId(x)
        b = self.fm.GenerateId(x)
        self.assertNotEqual(a, b)
        self.assertTrue(a.startswith(x))
        self.assertTrue(b.startswith(x))

    def testGenerateId_None(self):
        a = self.fm.GenerateId(None)
        b = self.fm.GenerateId(None)
        self.assertNotEqual(a, b)

    def testGenerateId_InitialCounter(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()

        for i in range(10):
            agency = transitfeed.Agency(
                name="agency",
                url="http://agency",
                timezone="Africa/Johannesburg",
                id="agency_%d" % i,
            )
            if i % 2:
                b_schedule.AddAgencyObject(agency)
            else:
                a_schedule.AddAgencyObject(agency)
        accumulator = TestingProblemAccumulator()
        feed_merger = merge.FeedMerger(
            a_schedule,
            b_schedule,
            merged_schedule,
            TestingProblemReporter(accumulator),
        )

        # check that the postfix number of any generated ids are greater than
        # the postfix numbers of any ids in the old and new schedules
        gen_id = feed_merger.GenerateId(None)
        postfix_num = int(gen_id[gen_id.rfind("_") + 1 :])
        self.assertTrue(postfix_num >= 10)

    def testGetMerger(self):
        class MergerA(merge.DataSetMerger):
            pass

        class MergerB(merge.DataSetMerger):
            pass

        a = MergerA(self.fm)
        b = MergerB(self.fm)

        self.fm.AddMerger(a)
        self.fm.AddMerger(b)

        self.assertEqual(self.fm.GetMerger(MergerA), a)
        self.assertEqual(self.fm.GetMerger(MergerB), b)

    def testGetMerger_Error(self):
        self.assertRaises(
            LookupError, self.fm.GetMerger, TestFeedMerger.Merger
        )


class TestServicePeriodMerger(util.TestCase):
    def setUp(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        self.accumulator = TestingProblemAccumulator()
        self.problem_reporter = TestingProblemReporter(self.accumulator)
        self.fm = merge.FeedMerger(
            a_schedule, b_schedule, merged_schedule, self.problem_reporter
        )
        self.spm = merge.ServicePeriodMerger(self.fm)
        self.fm.AddMerger(self.spm)

    def _AddTwoPeriods(self, start1, end1, start2, end2):
        sp1fields = ["test1", start1, end1] + ["1"] * 7
        self.sp1 = transitfeed.ServicePeriod(field_list=sp1fields)
        sp2fields = ["test2", start2, end2] + ["1"] * 7
        self.sp2 = transitfeed.ServicePeriod(field_list=sp2fields)

        self.fm.a_schedule.AddServicePeriodObject(self.sp1)
        self.fm.b_schedule.AddServicePeriodObject(self.sp2)

    def testCheckDisjoint_True(self):
        self._AddTwoPeriods("20071213", "20071231", "20080101", "20080201")
        self.assertTrue(self.spm.CheckDisjointCalendars())

    def testCheckDisjoint_False1(self):
        self._AddTwoPeriods("20071213", "20080201", "20080101", "20080301")
        self.assertTrue(not self.spm.CheckDisjointCalendars())

    def testCheckDisjoint_False2(self):
        self._AddTwoPeriods("20080101", "20090101", "20070101", "20080601")
        self.assertTrue(not self.spm.CheckDisjointCalendars())

    def testCheckDisjoint_False3(self):
        self._AddTwoPeriods("20080301", "20080901", "20080101", "20090101")
        self.assertTrue(not self.spm.CheckDisjointCalendars())

    def testDisjoinCalendars(self):
        self._AddTwoPeriods("20071213", "20080201", "20080101", "20080301")
        self.spm.DisjoinCalendars("20080101")
        self.assertEqual(self.sp1.start_date, "20071213")
        self.assertEqual(self.sp1.end_date, "20071231")
        self.assertEqual(self.sp2.start_date, "20080101")
        self.assertEqual(self.sp2.end_date, "20080301")

    def testDisjoinCalendars_Dates(self):
        self._AddTwoPeriods("20071213", "20080201", "20080101", "20080301")
        self.sp1.SetDateHasService("20071201")
        self.sp1.SetDateHasService("20081231")
        self.sp2.SetDateHasService("20071201")
        self.sp2.SetDateHasService("20081231")

        self.spm.DisjoinCalendars("20080101")

        self.assertTrue("20071201" in list(self.sp1.date_exceptions.keys()))
        self.assertTrue(
            "20081231" not in list(self.sp1.date_exceptions.keys())
        )
        self.assertTrue(
            "20071201" not in list(self.sp2.date_exceptions.keys())
        )
        self.assertTrue("20081231" in list(self.sp2.date_exceptions.keys()))

    def testUnion(self):
        self._AddTwoPeriods("20071213", "20071231", "20080101", "20080201")
        self.accumulator.ExpectProblemClass(merge.MergeNotImplemented)
        self.fm.MergeSchedules()
        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetServicePeriodList()), 2)

        # make fields a copy of the service period attributes except service_id
        fields = list(transitfeed.ServicePeriod._DAYS_OF_WEEK)
        fields += ["start_date", "end_date"]

        # now check that these attributes are preserved in the merge
        CheckAttribs(
            self.sp1, self.fm.a_merge_map[self.sp1], fields, self.assertEqual
        )
        CheckAttribs(
            self.sp2, self.fm.b_merge_map[self.sp2], fields, self.assertEqual
        )

        self.accumulator.assertExpectedProblemsReported(self)

    def testMerge_RequiredButNotDisjoint(self):
        self._AddTwoPeriods("20070101", "20090101", "20080101", "20100101")
        self.accumulator.ExpectProblemClass(merge.CalendarsNotDisjoint)
        self.assertEqual(self.spm.MergeDataSets(), False)
        self.accumulator.assertExpectedProblemsReported(self)

    def testMerge_NotRequiredAndNotDisjoint(self):
        self._AddTwoPeriods("20070101", "20090101", "20080101", "20100101")
        self.spm.require_disjoint_calendars = False
        self.accumulator.ExpectProblemClass(merge.MergeNotImplemented)
        self.fm.MergeSchedules()
        self.accumulator.assertExpectedProblemsReported(self)


class TestAgencyMerger(util.TestCase):
    def setUp(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        self.accumulator = TestingProblemAccumulator()
        self.problem_reporter = TestingProblemReporter(self.accumulator)
        self.fm = merge.FeedMerger(
            a_schedule, b_schedule, merged_schedule, self.problem_reporter
        )
        self.am = merge.AgencyMerger(self.fm)
        self.fm.AddMerger(self.am)

        self.a1 = transitfeed.Agency(
            id="a1",
            agency_name="a1",
            agency_url="http://www.a1.com",
            agency_timezone="Africa/Johannesburg",
            agency_phone="123 456 78 90",
        )
        self.a2 = transitfeed.Agency(
            id="a2",
            agency_name="a1",
            agency_url="http://www.a1.com",
            agency_timezone="Africa/Johannesburg",
            agency_phone="789 65 43 21",
        )

    def testMerge(self):
        self.a2.agency_id = self.a1.agency_id
        self.fm.a_schedule.AddAgencyObject(self.a1)
        self.fm.b_schedule.AddAgencyObject(self.a2)
        self.fm.MergeSchedules()

        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetAgencyList()), 1)
        self.assertEqual(
            merged_schedule.GetAgencyList()[0], self.fm.a_merge_map[self.a1]
        )
        self.assertEqual(
            self.fm.a_merge_map[self.a1], self.fm.b_merge_map[self.a2]
        )
        # differing values such as agency_phone should be taken from self.a2
        self.assertEqual(merged_schedule.GetAgencyList()[0], self.a2)
        self.assertEqual(self.am.GetMergeStats(), (1, 0, 0))

        # check that id is preserved
        self.assertEqual(
            self.fm.a_merge_map[self.a1].agency_id, self.a1.agency_id
        )

    def testNoMerge_DifferentId(self):
        self.fm.a_schedule.AddAgencyObject(self.a1)
        self.fm.b_schedule.AddAgencyObject(self.a2)
        self.fm.MergeSchedules()

        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetAgencyList()), 2)

        self.assertTrue(
            self.fm.a_merge_map[self.a1] in merged_schedule.GetAgencyList()
        )
        self.assertTrue(
            self.fm.b_merge_map[self.a2] in merged_schedule.GetAgencyList()
        )
        self.assertEqual(self.a1, self.fm.a_merge_map[self.a1])
        self.assertEqual(self.a2, self.fm.b_merge_map[self.a2])
        self.assertEqual(self.am.GetMergeStats(), (0, 1, 1))

        # check that the ids are preserved
        self.assertEqual(
            self.fm.a_merge_map[self.a1].agency_id, self.a1.agency_id
        )
        self.assertEqual(
            self.fm.b_merge_map[self.a2].agency_id, self.a2.agency_id
        )

    def testNoMerge_SameId(self):
        # Force a1.agency_id to be unicode to make sure it is correctly encoded
        # to utf-8 before concatinating to the agency_name containing non-ascii
        # characters.
        self.a1.agency_id = str(self.a1.agency_id)
        self.a2.agency_id = str(self.a1.agency_id)
        self.a2.agency_name = "different \xc3\xa9"
        self.fm.a_schedule.AddAgencyObject(self.a1)
        self.fm.b_schedule.AddAgencyObject(self.a2)

        self.accumulator.ExpectProblemClass(merge.SameIdButNotMerged)
        self.fm.MergeSchedules()

        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetAgencyList()), 2)
        self.assertEqual(self.am.GetMergeStats(), (0, 1, 1))

        # check that the merged entities have different ids
        self.assertNotEqual(
            self.fm.a_merge_map[self.a1].agency_id,
            self.fm.b_merge_map[self.a2].agency_id,
        )

        self.accumulator.assertExpectedProblemsReported(self)


class TestStopMerger(util.TestCase):
    def setUp(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        self.accumulator = TestingProblemAccumulator()
        self.problem_reporter = TestingProblemReporter(self.accumulator)
        self.fm = merge.FeedMerger(
            a_schedule, b_schedule, merged_schedule, self.problem_reporter
        )
        self.sm = merge.StopMerger(self.fm)
        self.fm.AddMerger(self.sm)

        self.s1 = transitfeed.Stop(30.0, 30.0, "Andr\202", "s1")
        self.s1.stop_desc = "stop 1"
        self.s1.stop_url = "http://stop/1"
        self.s1.zone_id = "zone1"
        self.s2 = transitfeed.Stop(30.0, 30.0, "s2", "s2")
        self.s2.stop_desc = "stop 2"
        self.s2.stop_url = "http://stop/2"
        self.s2.zone_id = "zone1"

    def testMerge(self):
        self.s2.stop_id = self.s1.stop_id
        self.s2.stop_name = self.s1.stop_name
        self.s1.location_type = 1
        self.s2.location_type = 1

        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        self.fm.MergeSchedules()

        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetStopList()), 1)
        self.assertEqual(
            merged_schedule.GetStopList()[0], self.fm.a_merge_map[self.s1]
        )
        self.assertEqual(
            self.fm.a_merge_map[self.s1], self.fm.b_merge_map[self.s2]
        )
        self.assertEqual(self.sm.GetMergeStats(), (1, 0, 0))

        # check that the remaining attributes are taken from the new stop
        fields = [
            "stop_name",
            "stop_lat",
            "stop_lon",
            "stop_desc",
            "stop_url",
            "location_type",
        ]
        CheckAttribs(
            self.fm.a_merge_map[self.s1], self.s2, fields, self.assertEqual
        )

        # check that the id is preserved
        self.assertEqual(self.fm.a_merge_map[self.s1].stop_id, self.s1.stop_id)

        # check that the zone_id is preserved
        self.assertEqual(self.fm.a_merge_map[self.s1].zone_id, self.s1.zone_id)

    def testNoMerge_DifferentId(self):
        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        self.fm.MergeSchedules()

        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetStopList()), 2)
        self.assertTrue(
            self.fm.a_merge_map[self.s1] in merged_schedule.GetStopList()
        )
        self.assertTrue(
            self.fm.b_merge_map[self.s2] in merged_schedule.GetStopList()
        )
        self.assertEqual(self.sm.GetMergeStats(), (0, 1, 1))

    def testNoMerge_DifferentName(self):
        self.s2.stop_id = self.s1.stop_id
        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        self.accumulator.ExpectProblemClass(merge.SameIdButNotMerged)
        self.fm.MergeSchedules()

        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetStopList()), 2)
        self.assertTrue(
            self.fm.a_merge_map[self.s1] in merged_schedule.GetStopList()
        )
        self.assertTrue(
            self.fm.b_merge_map[self.s2] in merged_schedule.GetStopList()
        )
        self.assertEqual(self.sm.GetMergeStats(), (0, 1, 1))

    def testNoMerge_FarApart(self):
        self.s2.stop_id = self.s1.stop_id
        self.s2.stop_name = self.s1.stop_name
        self.s2.stop_lat = 40.0
        self.s2.stop_lon = 40.0

        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        self.accumulator.ExpectProblemClass(merge.SameIdButNotMerged)
        self.fm.MergeSchedules()

        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetStopList()), 2)
        self.assertTrue(
            self.fm.a_merge_map[self.s1] in merged_schedule.GetStopList()
        )
        self.assertTrue(
            self.fm.b_merge_map[self.s2] in merged_schedule.GetStopList()
        )
        self.assertEqual(self.sm.GetMergeStats(), (0, 1, 1))

        # check that the merged ids are different
        self.assertNotEqual(
            self.fm.a_merge_map[self.s1].stop_id,
            self.fm.b_merge_map[self.s2].stop_id,
        )

        self.accumulator.assertExpectedProblemsReported(self)

    def testMerge_CaseInsensitive(self):
        self.s2.stop_id = self.s1.stop_id
        self.s2.stop_name = self.s1.stop_name.upper()
        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        self.fm.MergeSchedules()
        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetStopList()), 1)
        self.assertEqual(self.sm.GetMergeStats(), (1, 0, 0))

    def testNoMerge_ZoneId(self):
        self.s2.zone_id = "zone2"
        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        self.fm.MergeSchedules()

        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetStopList()), 2)

        self.assertTrue(self.s1.zone_id in self.fm.a_zone_map)
        self.assertTrue(self.s2.zone_id in self.fm.b_zone_map)
        self.assertEqual(self.sm.GetMergeStats(), (0, 1, 1))

        # check that the zones are still different
        self.assertNotEqual(
            self.fm.a_merge_map[self.s1].zone_id,
            self.fm.b_merge_map[self.s2].zone_id,
        )

    def testZoneId_SamePreservation(self):
        # checks that if the zone_ids of some stops are the same before the
        # merge, they are still the same after.
        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.a_schedule.AddStopObject(self.s2)
        self.fm.MergeSchedules()
        self.assertEqual(
            self.fm.a_merge_map[self.s1].zone_id,
            self.fm.a_merge_map[self.s2].zone_id,
        )

    def testZoneId_DifferentSchedules(self):
        # zone_ids may be the same in different schedules but unless the stops
        # are merged, they should map to different zone_ids
        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        self.fm.MergeSchedules()
        self.assertNotEqual(
            self.fm.a_merge_map[self.s1].zone_id,
            self.fm.b_merge_map[self.s2].zone_id,
        )

    def testZoneId_MergePreservation(self):
        # check that if two stops are merged, the zone mapping is used for all
        # other stops too
        self.s2.stop_id = self.s1.stop_id
        self.s2.stop_name = self.s1.stop_name
        s3 = transitfeed.Stop(field_dict=self.s1)
        s3.stop_id = "different"

        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.a_schedule.AddStopObject(s3)
        self.fm.b_schedule.AddStopObject(self.s2)
        self.fm.MergeSchedules()

        self.assertEqual(
            self.fm.a_merge_map[self.s1].zone_id,
            self.fm.a_merge_map[s3].zone_id,
        )
        self.assertEqual(
            self.fm.a_merge_map[s3].zone_id,
            self.fm.b_merge_map[self.s2].zone_id,
        )

    def testMergeStationType(self):
        self.s2.stop_id = self.s1.stop_id
        self.s2.stop_name = self.s1.stop_name
        self.s1.location_type = 1
        self.s2.location_type = 1
        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        self.fm.MergeSchedules()
        merged_stops = self.fm.GetMergedSchedule().GetStopList()
        self.assertEqual(len(merged_stops), 1)
        self.assertEqual(merged_stops[0].location_type, 1)

    def testMergeDifferentTypes(self):
        self.s2.stop_id = self.s1.stop_id
        self.s2.stop_name = self.s1.stop_name
        self.s2.location_type = 1
        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        try:
            self.fm.MergeSchedules()
            self.fail("Expecting MergeError")
        except merge.SameIdButNotMerged as merge_error:
            self.assertTrue(("%s" % merge_error).find("location_type") != -1)

    def AssertS1ParentIsS2(self):
        """Assert that the merged s1 has parent s2."""
        new_s1 = self.s1._migrated_entity
        new_s2 = self.s2._migrated_entity
        self.assertEqual(new_s1.parent_station, new_s2.stop_id)
        self.assertEqual(new_s2.parent_station, None)
        self.assertEqual(new_s1.location_type, 0)
        self.assertEqual(new_s2.location_type, 1)

    def testMergeMaintainParentRelationship(self):
        self.s2.location_type = 1
        self.s1.parent_station = self.s2.stop_id
        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.a_schedule.AddStopObject(self.s2)
        self.fm.MergeSchedules()
        self.AssertS1ParentIsS2()

    def testParentRelationshipAfterMerge(self):
        s3 = transitfeed.Stop(field_dict=self.s1)
        s3.parent_station = self.s2.stop_id
        self.s2.location_type = 1
        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        self.fm.b_schedule.AddStopObject(s3)
        self.fm.MergeSchedules()
        self.AssertS1ParentIsS2()

    def testParentRelationshipWithNewParentid(self):
        self.s2.location_type = 1
        self.s1.parent_station = self.s2.stop_id
        # s3 will have a stop_id conflict with self.s2 so parent_id of the
        # migrated self.s1 will need to be updated
        s3 = transitfeed.Stop(field_dict=self.s2)
        s3.stop_lat = 45
        self.fm.a_schedule.AddStopObject(s3)
        self.fm.b_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        self.accumulator.ExpectProblemClass(merge.SameIdButNotMerged)
        self.fm.MergeSchedules()
        self.assertNotEqual(
            s3._migrated_entity.stop_id, self.s2._migrated_entity.stop_id
        )
        # Check that s2 got a new id
        self.assertNotEqual(self.s2.stop_id, self.s2._migrated_entity.stop_id)
        self.AssertS1ParentIsS2()

    def _AddStopsApart(self):
        """Adds two stops to the schedules and returns the distance between them.

        Returns:
          The distance between the stops in metres, a value greater than zero.
        """
        self.s2.stop_id = self.s1.stop_id
        self.s2.stop_name = self.s1.stop_name
        self.s2.stop_lat += 1.0e-3
        self.fm.a_schedule.AddStopObject(self.s1)
        self.fm.b_schedule.AddStopObject(self.s2)
        return transitfeed.ApproximateDistanceBetweenStops(self.s1, self.s2)

    def testSetLargestStopDistanceSmall(self):
        largest_stop_distance = self._AddStopsApart() * 0.5
        self.sm.SetLargestStopDistance(largest_stop_distance)
        self.assertEqual(self.sm.largest_stop_distance, largest_stop_distance)
        self.accumulator.ExpectProblemClass(merge.SameIdButNotMerged)
        self.fm.MergeSchedules()
        self.assertEqual(len(self.fm.GetMergedSchedule().GetStopList()), 2)
        self.accumulator.assertExpectedProblemsReported(self)

    def testSetLargestStopDistanceLarge(self):
        largest_stop_distance = self._AddStopsApart() * 2.0
        self.sm.SetLargestStopDistance(largest_stop_distance)
        self.assertEqual(self.sm.largest_stop_distance, largest_stop_distance)
        self.fm.MergeSchedules()
        self.assertEqual(len(self.fm.GetMergedSchedule().GetStopList()), 1)


class TestRouteMerger(util.TestCase):
    fields = ["route_short_name", "route_long_name", "route_type", "route_url"]

    def setUp(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        self.accumulator = TestingProblemAccumulator()
        self.problem_reporter = TestingProblemReporter(self.accumulator)
        self.fm = merge.FeedMerger(
            a_schedule, b_schedule, merged_schedule, self.problem_reporter
        )
        self.fm.AddMerger(merge.AgencyMerger(self.fm))
        self.rm = merge.RouteMerger(self.fm)
        self.fm.AddMerger(self.rm)

        akwargs = {
            "id": "a1",
            "agency_name": "a1",
            "agency_url": "http://www.a1.com",
            "agency_timezone": "Europe/Zurich",
        }
        self.a1 = transitfeed.Agency(**akwargs)
        self.a2 = transitfeed.Agency(**akwargs)
        a_schedule.AddAgencyObject(self.a1)
        b_schedule.AddAgencyObject(self.a2)

        rkwargs = {
            "route_id": "r1",
            "agency_id": "a1",
            "short_name": "r1",
            "long_name": "r1r1",
            "route_type": "0",
        }
        self.r1 = transitfeed.Route(**rkwargs)
        self.r2 = transitfeed.Route(**rkwargs)
        self.r2.route_url = "http://route/2"

    def testMerge(self):
        self.fm.a_schedule.AddRouteObject(self.r1)
        self.fm.b_schedule.AddRouteObject(self.r2)
        self.fm.MergeSchedules()

        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetRouteList()), 1)
        r = merged_schedule.GetRouteList()[0]
        self.assertTrue(self.fm.a_merge_map[self.r1] is r)
        self.assertTrue(self.fm.b_merge_map[self.r2] is r)
        CheckAttribs(self.r2, r, self.fields, self.assertEqual)
        self.assertEqual(r.agency_id, self.fm.a_merge_map[self.a1].agency_id)
        self.assertEqual(self.rm.GetMergeStats(), (1, 0, 0))

        # check that the id is preserved
        self.assertEqual(
            self.fm.a_merge_map[self.r1].route_id, self.r1.route_id
        )

    def testMergeNoAgency(self):
        self.r1.agency_id = None
        self.r2.agency_id = None
        self.fm.a_schedule.AddRouteObject(self.r1)
        self.fm.b_schedule.AddRouteObject(self.r2)
        self.fm.MergeSchedules()

        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetRouteList()), 1)
        r = merged_schedule.GetRouteList()[0]
        CheckAttribs(self.r2, r, self.fields, self.assertEqual)
        # Merged route has copy of default agency_id
        self.assertEqual(r.agency_id, self.a1.agency_id)
        self.assertEqual(self.rm.GetMergeStats(), (1, 0, 0))

        # check that the id is preserved
        self.assertEqual(
            self.fm.a_merge_map[self.r1].route_id, self.r1.route_id
        )

    def testMigrateNoAgency(self):
        self.r1.agency_id = None
        self.fm.a_schedule.AddRouteObject(self.r1)
        self.fm.MergeSchedules()
        merged_schedule = self.fm.GetMergedSchedule()
        self.assertEqual(len(merged_schedule.GetRouteList()), 1)
        r = merged_schedule.GetRouteList()[0]
        CheckAttribs(self.r1, r, self.fields, self.assertEqual)
        # Migrated route has copy of default agency_id
        self.assertEqual(r.agency_id, self.a1.agency_id)

    def testNoMerge_DifferentId(self):
        self.r2.route_id = "r2"
        self.fm.a_schedule.AddRouteObject(self.r1)
        self.fm.b_schedule.AddRouteObject(self.r2)
        self.fm.MergeSchedules()
        self.assertEqual(len(self.fm.GetMergedSchedule().GetRouteList()), 2)
        self.assertEqual(self.rm.GetMergeStats(), (0, 1, 1))

    def testNoMerge_SameId(self):
        self.r2.route_short_name = "different"
        self.fm.a_schedule.AddRouteObject(self.r1)
        self.fm.b_schedule.AddRouteObject(self.r2)
        self.accumulator.ExpectProblemClass(merge.SameIdButNotMerged)
        self.fm.MergeSchedules()
        self.assertEqual(len(self.fm.GetMergedSchedule().GetRouteList()), 2)
        self.assertEqual(self.rm.GetMergeStats(), (0, 1, 1))

        # check that the merged ids are different
        self.assertNotEqual(
            self.fm.a_merge_map[self.r1].route_id,
            self.fm.b_merge_map[self.r2].route_id,
        )

        self.accumulator.assertExpectedProblemsReported(self)


class TestTripMerger(util.TestCase):
    def setUp(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        self.accumulator = TestingProblemAccumulator()
        self.problem_reporter = TestingProblemReporter(self.accumulator)
        self.fm = merge.FeedMerger(
            a_schedule, b_schedule, merged_schedule, self.problem_reporter
        )
        self.fm.AddDefaultMergers()
        self.tm = self.fm.GetMerger(merge.TripMerger)

        akwargs = {
            "id": "a1",
            "agency_name": "a1",
            "agency_url": "http://www.a1.com",
            "agency_timezone": "Europe/Zurich",
        }
        self.a1 = transitfeed.Agency(**akwargs)

        rkwargs = {
            "route_id": "r1",
            "agency_id": "a1",
            "short_name": "r1",
            "long_name": "r1r1",
            "route_type": "0",
        }
        self.r1 = transitfeed.Route(**rkwargs)

        self.s1 = transitfeed.ServicePeriod("s1")
        self.s1.start_date = "20071201"
        self.s1.end_date = "20071231"
        self.s1.SetWeekdayService()

        self.shape = transitfeed.Shape("shape1")
        self.shape.AddPoint(30.0, 30.0)

        self.t1 = transitfeed.Trip(
            service_period=self.s1, route=self.r1, trip_id="t1"
        )
        self.t2 = transitfeed.Trip(
            service_period=self.s1, route=self.r1, trip_id="t2"
        )
        # Must add self.t1 to a schedule before calling self.t1.AddStopTime
        a_schedule.AddTripObject(self.t1, validate=False)
        a_schedule.AddTripObject(self.t2, validate=False)
        self.t1.block_id = "b1"
        self.t2.block_id = "b1"
        self.t1.shape_id = "shape1"

        self.stop = transitfeed.Stop(30.0, 30.0, stop_id="stop1")
        self.t1.AddStopTime(self.stop, arrival_secs=0, departure_secs=0)

        a_schedule.AddAgencyObject(self.a1)
        a_schedule.AddStopObject(self.stop)
        a_schedule.AddRouteObject(self.r1)
        a_schedule.AddServicePeriodObject(self.s1)
        a_schedule.AddShapeObject(self.shape)

    def testMigrate(self):
        self.accumulator.ExpectProblemClass(merge.MergeNotImplemented)
        self.fm.MergeSchedules()
        self.accumulator.assertExpectedProblemsReported(self)

        r = self.fm.a_merge_map[self.r1]
        s = self.fm.a_merge_map[self.s1]
        shape = self.fm.a_merge_map[self.shape]
        t1 = self.fm.a_merge_map[self.t1]
        t2 = self.fm.a_merge_map[self.t2]

        self.assertEqual(t1.route_id, r.route_id)
        self.assertEqual(t1.service_id, s.service_id)
        self.assertEqual(t1.shape_id, shape.shape_id)
        self.assertEqual(t1.block_id, t2.block_id)

        self.assertEqual(len(t1.GetStopTimes()), 1)
        st = t1.GetStopTimes()[0]
        self.assertEqual(st.stop, self.fm.a_merge_map[self.stop])

    def testReportsNotImplementedProblem(self):
        self.accumulator.ExpectProblemClass(merge.MergeNotImplemented)
        self.fm.MergeSchedules()
        self.accumulator.assertExpectedProblemsReported(self)

    def testMergeStats(self):
        self.assertTrue(self.tm.GetMergeStats() is None)

    def testConflictingTripid(self):
        a1_in_b = transitfeed.Agency(field_dict=self.a1)
        r1_in_b = transitfeed.Route(field_dict=self.r1)
        t1_in_b = transitfeed.Trip(field_dict=self.t1)
        t1_in_b.trip_short_name = "t1-b"
        shape_in_b = transitfeed.Shape("shape1")
        shape_in_b.AddPoint(30.0, 30.0)
        s_in_b = transitfeed.ServicePeriod("s1")
        s_in_b.start_date = "20080101"
        s_in_b.end_date = "20080131"
        s_in_b.SetWeekdayService()

        self.fm.b_schedule.AddAgencyObject(a1_in_b)
        self.fm.b_schedule.AddRouteObject(r1_in_b)
        self.fm.b_schedule.AddShapeObject(shape_in_b)
        self.fm.b_schedule.AddTripObject(t1_in_b, validate=False)
        self.fm.b_schedule.AddServicePeriodObject(s_in_b, validate=False)
        self.accumulator.ExpectProblemClass(merge.MergeNotImplemented)
        self.fm.MergeSchedules()
        # 3 trips moved to merged_schedule: from a_schedule t1, t2 and from
        # b_schedule t1
        trips = self.fm.merged_schedule.GetTripList()
        self.assertEqual(len(trips), 3)
        t1_in_b_merged = [
            trip for trip in trips if trip.trip_short_name == "t1-b"
        ]
        self.assertEqual(len(t1_in_b_merged), 1)
        self.assertEqual(t1_in_b_merged[0].original_trip_id, "t1")


class TestFareMerger(util.TestCase):
    def setUp(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        self.accumulator = TestingProblemAccumulator()
        self.problem_reporter = TestingProblemReporter(self.accumulator)
        self.fm = merge.FeedMerger(
            a_schedule, b_schedule, merged_schedule, self.problem_reporter
        )
        self.faremerger = merge.FareMerger(self.fm)
        self.fm.AddMerger(self.faremerger)

        self.f1 = transitfeed.FareAttribute("f1", "10", "ZAR", "1", "0")
        self.f2 = transitfeed.FareAttribute("f2", "10", "ZAR", "1", "0")

    def testMerge(self):
        self.f2.fare_id = self.f1.fare_id
        self.fm.a_schedule.AddFareAttributeObject(self.f1)
        self.fm.b_schedule.AddFareAttributeObject(self.f2)
        self.fm.MergeSchedules()
        self.assertEqual(
            len(self.fm.merged_schedule.GetFareAttributeList()), 1
        )
        self.assertEqual(self.faremerger.GetMergeStats(), (1, 0, 0))

        # check that the id is preserved
        self.assertEqual(self.fm.a_merge_map[self.f1].fare_id, self.f1.fare_id)

    def testNoMerge_DifferentPrice(self):
        self.f2.fare_id = self.f1.fare_id
        self.f2.price = 11.0
        self.fm.a_schedule.AddFareAttributeObject(self.f1)
        self.fm.b_schedule.AddFareAttributeObject(self.f2)
        self.accumulator.ExpectProblemClass(merge.SameIdButNotMerged)
        self.fm.MergeSchedules()
        self.assertEqual(
            len(self.fm.merged_schedule.GetFareAttributeList()), 2
        )
        self.assertEqual(self.faremerger.GetMergeStats(), (0, 1, 1))

        # check that the merged ids are different
        self.assertNotEqual(
            self.fm.a_merge_map[self.f1].fare_id,
            self.fm.b_merge_map[self.f2].fare_id,
        )

        self.accumulator.assertExpectedProblemsReported(self)

    def testNoMerge_DifferentId(self):
        self.fm.a_schedule.AddFareAttributeObject(self.f1)
        self.fm.b_schedule.AddFareAttributeObject(self.f2)
        self.fm.MergeSchedules()
        self.assertEqual(
            len(self.fm.merged_schedule.GetFareAttributeList()), 2
        )
        self.assertEqual(self.faremerger.GetMergeStats(), (0, 1, 1))

        # check that the ids are preserved
        self.assertEqual(self.fm.a_merge_map[self.f1].fare_id, self.f1.fare_id)
        self.assertEqual(self.fm.b_merge_map[self.f2].fare_id, self.f2.fare_id)


class TestShapeMerger(util.TestCase):
    def setUp(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        self.accumulator = TestingProblemAccumulator()
        self.problem_reporter = TestingProblemReporter(self.accumulator)
        self.fm = merge.FeedMerger(
            a_schedule, b_schedule, merged_schedule, self.problem_reporter
        )
        self.sm = merge.ShapeMerger(self.fm)
        self.fm.AddMerger(self.sm)

        # setup some shapes
        # s1 and s2 have the same endpoints but take different paths
        # s3 has different endpoints to s1 and s2

        self.s1 = transitfeed.Shape("s1")
        self.s1.AddPoint(30.0, 30.0)
        self.s1.AddPoint(40.0, 30.0)
        self.s1.AddPoint(50.0, 50.0)

        self.s2 = transitfeed.Shape("s2")
        self.s2.AddPoint(30.0, 30.0)
        self.s2.AddPoint(40.0, 35.0)
        self.s2.AddPoint(50.0, 50.0)

        self.s3 = transitfeed.Shape("s3")
        self.s3.AddPoint(31.0, 31.0)
        self.s3.AddPoint(45.0, 35.0)
        self.s3.AddPoint(51.0, 51.0)

    def testMerge(self):
        self.s2.shape_id = self.s1.shape_id
        self.fm.a_schedule.AddShapeObject(self.s1)
        self.fm.b_schedule.AddShapeObject(self.s2)
        self.fm.MergeSchedules()
        self.assertEqual(len(self.fm.merged_schedule.GetShapeList()), 1)
        self.assertEqual(self.fm.merged_schedule.GetShapeList()[0], self.s2)
        self.assertEqual(self.sm.GetMergeStats(), (1, 0, 0))

        # check that the id is preserved
        self.assertEqual(
            self.fm.a_merge_map[self.s1].shape_id, self.s1.shape_id
        )

    def testNoMerge_DifferentId(self):
        self.fm.a_schedule.AddShapeObject(self.s1)
        self.fm.b_schedule.AddShapeObject(self.s2)
        self.fm.MergeSchedules()
        self.assertEqual(len(self.fm.merged_schedule.GetShapeList()), 2)
        self.assertEqual(self.s1, self.fm.a_merge_map[self.s1])
        self.assertEqual(self.s2, self.fm.b_merge_map[self.s2])
        self.assertEqual(self.sm.GetMergeStats(), (0, 1, 1))

        # check that the ids are preserved
        self.assertEqual(
            self.fm.a_merge_map[self.s1].shape_id, self.s1.shape_id
        )
        self.assertEqual(
            self.fm.b_merge_map[self.s2].shape_id, self.s2.shape_id
        )

    def testNoMerge_FarEndpoints(self):
        self.s3.shape_id = self.s1.shape_id
        self.fm.a_schedule.AddShapeObject(self.s1)
        self.fm.b_schedule.AddShapeObject(self.s3)
        self.accumulator.ExpectProblemClass(merge.SameIdButNotMerged)
        self.fm.MergeSchedules()
        self.assertEqual(len(self.fm.merged_schedule.GetShapeList()), 2)
        self.assertEqual(self.s1, self.fm.a_merge_map[self.s1])
        self.assertEqual(self.s3, self.fm.b_merge_map[self.s3])
        self.assertEqual(self.sm.GetMergeStats(), (0, 1, 1))

        # check that the ids are different
        self.assertNotEqual(
            self.fm.a_merge_map[self.s1].shape_id,
            self.fm.b_merge_map[self.s3].shape_id,
        )

        self.accumulator.assertExpectedProblemsReported(self)

    def _AddShapesApart(self):
        """Adds two shapes to the schedules.

        The maximum of the distances between the endpoints is returned.

        Returns:
          The distance in metres, a value greater than zero.
        """
        self.s3.shape_id = self.s1.shape_id
        self.fm.a_schedule.AddShapeObject(self.s1)
        self.fm.b_schedule.AddShapeObject(self.s3)
        distance1 = merge.ApproximateDistanceBetweenPoints(
            self.s1.points[0][:2], self.s3.points[0][:2]
        )
        distance2 = merge.ApproximateDistanceBetweenPoints(
            self.s1.points[-1][:2], self.s3.points[-1][:2]
        )
        return max(distance1, distance2)

    def testSetLargestShapeDistanceSmall(self):
        largest_shape_distance = self._AddShapesApart() * 0.5
        self.sm.SetLargestShapeDistance(largest_shape_distance)
        self.assertEqual(
            self.sm.largest_shape_distance, largest_shape_distance
        )
        self.accumulator.ExpectProblemClass(merge.SameIdButNotMerged)
        self.fm.MergeSchedules()
        self.assertEqual(len(self.fm.GetMergedSchedule().GetShapeList()), 2)
        self.accumulator.assertExpectedProblemsReported(self)

    def testSetLargestShapeDistanceLarge(self):
        largest_shape_distance = self._AddShapesApart() * 2.0
        self.sm.SetLargestShapeDistance(largest_shape_distance)
        self.assertEqual(
            self.sm.largest_shape_distance, largest_shape_distance
        )
        self.fm.MergeSchedules()
        self.assertEqual(len(self.fm.GetMergedSchedule().GetShapeList()), 1)


class TestFareRuleMerger(util.TestCase):
    def setUp(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        self.accumulator = TestingProblemAccumulator()
        self.problem_reporter = TestingProblemReporter(self.accumulator)
        self.fm = merge.FeedMerger(
            a_schedule, b_schedule, merged_schedule, self.problem_reporter
        )
        self.fm.AddDefaultMergers()
        self.fare_rule_merger = self.fm.GetMerger(merge.FareRuleMerger)

        akwargs = {
            "id": "a1",
            "agency_name": "a1",
            "agency_url": "http://www.a1.com",
            "agency_timezone": "Europe/Zurich",
        }
        self.a1 = transitfeed.Agency(**akwargs)
        self.a2 = transitfeed.Agency(**akwargs)

        rkwargs = {
            "route_id": "r1",
            "agency_id": "a1",
            "short_name": "r1",
            "long_name": "r1r1",
            "route_type": "0",
        }
        self.r1 = transitfeed.Route(**rkwargs)
        self.r2 = transitfeed.Route(**rkwargs)

        self.f1 = transitfeed.FareAttribute("f1", "10", "ZAR", "1", "0")
        self.f2 = transitfeed.FareAttribute("f1", "10", "ZAR", "1", "0")
        self.f3 = transitfeed.FareAttribute("f3", "11", "USD", "1", "0")

        self.fr1 = transitfeed.FareRule("f1", "r1")
        self.fr2 = transitfeed.FareRule("f1", "r1")
        self.fr3 = transitfeed.FareRule("f3", "r1")

        self.fm.a_schedule.AddAgencyObject(self.a1)
        self.fm.a_schedule.AddRouteObject(self.r1)
        self.fm.a_schedule.AddFareAttributeObject(self.f1)
        self.fm.a_schedule.AddFareAttributeObject(self.f3)
        self.fm.a_schedule.AddFareRuleObject(self.fr1)
        self.fm.a_schedule.AddFareRuleObject(self.fr3)

        self.fm.b_schedule.AddAgencyObject(self.a2)
        self.fm.b_schedule.AddRouteObject(self.r2)
        self.fm.b_schedule.AddFareAttributeObject(self.f2)
        self.fm.b_schedule.AddFareRuleObject(self.fr2)

    def testMerge(self):
        self.accumulator.ExpectProblemClass(merge.FareRulesBroken)
        self.accumulator.ExpectProblemClass(merge.MergeNotImplemented)
        self.fm.MergeSchedules()

        self.assertEqual(
            len(self.fm.merged_schedule.GetFareAttributeList()), 2
        )

        fare_1 = self.fm.a_merge_map[self.f1]
        fare_2 = self.fm.a_merge_map[self.f3]

        self.assertEqual(len(fare_1.GetFareRuleList()), 1)
        fare_rule_1 = fare_1.GetFareRuleList()[0]
        self.assertEqual(len(fare_2.GetFareRuleList()), 1)
        fare_rule_2 = fare_2.GetFareRuleList()[0]

        self.assertEqual(
            fare_rule_1.fare_id, self.fm.a_merge_map[self.f1].fare_id
        )
        self.assertEqual(
            fare_rule_1.route_id, self.fm.a_merge_map[self.r1].route_id
        )
        self.assertEqual(
            fare_rule_2.fare_id, self.fm.a_merge_map[self.f3].fare_id
        )
        self.assertEqual(
            fare_rule_2.route_id, self.fm.a_merge_map[self.r1].route_id
        )

        self.accumulator.assertExpectedProblemsReported(self)

    def testMergeStats(self):
        self.assertTrue(self.fare_rule_merger.GetMergeStats() is None)


class TestTransferMerger(util.TestCase):
    def setUp(self):
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        self.accumulator = TestingProblemAccumulator()
        self.problem_reporter = TestingProblemReporter(self.accumulator)
        self.fm = merge.FeedMerger(
            a_schedule, b_schedule, merged_schedule, self.problem_reporter
        )

    def testStopsMerged(self):
        stop0 = transitfeed.Stop(lat=30.0, lng=30.0, name="0", stop_id="0")
        stop1 = transitfeed.Stop(lat=30.1, lng=30.1, name="1", stop_id="1")
        self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))
        self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))

        self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1))
        self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1))
        self.fm.a_schedule.AddTransferObject(
            transitfeed.Transfer(from_stop_id="0", to_stop_id="1")
        )
        self.fm.b_schedule.AddTransferObject(
            transitfeed.Transfer(from_stop_id="0", to_stop_id="1")
        )
        self.fm.AddMerger(merge.StopMerger(self.fm))
        self.fm.AddMerger(merge.TransferMerger(self.fm))
        self.fm.MergeSchedules()
        transfers = self.fm.merged_schedule.GetTransferList()
        self.assertEqual(1, len(transfers))
        self.assertEqual("0", transfers[0].from_stop_id)
        self.assertEqual("1", transfers[0].to_stop_id)

    def testToStopNotMerged(self):
        """When stops aren't merged transfer is duplicated."""
        self.accumulator.ExpectProblemClass(merge.SameIdButNotMerged)
        stop0 = transitfeed.Stop(lat=30.0, lng=30.0, name="0", stop_id="0")
        stop1a = transitfeed.Stop(lat=30.1, lng=30.1, name="1a", stop_id="1")
        stop1b = transitfeed.Stop(lat=30.1, lng=30.1, name="1b", stop_id="1")

        # a_schedule and b_schedule both have a transfer with to_stop_id=1 but the
        # stops are not merged so the transfer must be duplicated. Create a copy
        # of the Stop objects to add to the schedules.
        self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))
        self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1a))
        self.fm.a_schedule.AddTransferObject(
            transitfeed.Transfer(from_stop_id="0", to_stop_id="1")
        )
        self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))
        self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1b))
        self.fm.b_schedule.AddTransferObject(
            transitfeed.Transfer(from_stop_id="0", to_stop_id="1")
        )
        self.fm.AddMerger(merge.StopMerger(self.fm))
        self.fm.AddMerger(merge.TransferMerger(self.fm))
        self.fm.MergeSchedules()

        transfers = self.fm.merged_schedule.GetTransferList()
        self.assertEqual(2, len(transfers))
        self.assertEqual("0", transfers[0].from_stop_id)
        self.assertEqual("0", transfers[1].from_stop_id)

        # transfers are not ordered so allow the migrated to_stop_id values to
        # appear in either order.
        def MergedScheduleStopName(stop_id):
            return self.fm.merged_schedule.GetStop(stop_id).stop_name

        if MergedScheduleStopName(transfers[0].to_stop_id) == "1a":
            self.assertEqual(
                "1b", MergedScheduleStopName(transfers[1].to_stop_id)
            )
        else:
            self.assertEqual(
                "1b", MergedScheduleStopName(transfers[0].to_stop_id)
            )
            self.assertEqual(
                "1a", MergedScheduleStopName(transfers[1].to_stop_id)
            )

    def testFromStopNotMerged(self):
        """When stops aren't merged transfer is duplicated."""
        self.accumulator.ExpectProblemClass(merge.SameIdButNotMerged)
        stop0 = transitfeed.Stop(lat=30.0, lng=30.0, name="0", stop_id="0")
        stop1a = transitfeed.Stop(lat=30.1, lng=30.1, name="1a", stop_id="1")
        stop1b = transitfeed.Stop(lat=30.1, lng=30.1, name="1b", stop_id="1")

        # a_schedule and b_schedule both have a transfer with from_stop_id=1 but the
        # stops are not merged so the transfer must be duplicated. Create a copy
        # of the Stop objects to add to the schedules.
        self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))
        self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1a))
        self.fm.a_schedule.AddTransferObject(
            transitfeed.Transfer(from_stop_id="1", to_stop_id="0")
        )
        self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))
        self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1b))
        self.fm.b_schedule.AddTransferObject(
            transitfeed.Transfer(from_stop_id="1", to_stop_id="0")
        )
        self.fm.AddMerger(merge.StopMerger(self.fm))
        self.fm.AddMerger(merge.TransferMerger(self.fm))
        self.fm.MergeSchedules()

        transfers = self.fm.merged_schedule.GetTransferList()
        self.assertEqual(2, len(transfers))
        self.assertEqual("0", transfers[0].to_stop_id)
        self.assertEqual("0", transfers[1].to_stop_id)

        # transfers are not ordered so allow the migrated from_stop_id values to
        # appear in either order.
        def MergedScheduleStopName(stop_id):
            return self.fm.merged_schedule.GetStop(stop_id).stop_name

        if MergedScheduleStopName(transfers[0].from_stop_id) == "1a":
            self.assertEqual(
                "1b", MergedScheduleStopName(transfers[1].from_stop_id)
            )
        else:
            self.assertEqual(
                "1b", MergedScheduleStopName(transfers[0].from_stop_id)
            )
            self.assertEqual(
                "1a", MergedScheduleStopName(transfers[1].from_stop_id)
            )


class TestExceptionProblemAccumulator(util.TestCase):
    def setUp(self):
        self.dataset_merger = merge.TripMerger(None)

    def testRaisesErrors(self):
        accumulator = transitfeed.ExceptionProblemAccumulator()
        problem_reporter = merge.MergeProblemReporter(accumulator)
        self.assertRaises(
            merge.CalendarsNotDisjoint,
            problem_reporter.CalendarsNotDisjoint,
            self.dataset_merger,
        )

    def testNoRaiseWarnings(self):
        accumulator = transitfeed.ExceptionProblemAccumulator()
        problem_reporter = merge.MergeProblemReporter(accumulator)
        problem_reporter.MergeNotImplemented(self.dataset_merger)

    def testRaiseWarnings(self):
        accumulator = transitfeed.ExceptionProblemAccumulator(True)
        problem_reporter = merge.MergeProblemReporter(accumulator)
        self.assertRaises(
            merge.MergeNotImplemented,
            problem_reporter.MergeNotImplemented,
            self.dataset_merger,
        )


class TestHTMLProblemAccumulator(util.TestCase):
    def setUp(self):
        self.accumulator = merge.HTMLProblemAccumulator()
        self.problem_reporter = merge.MergeProblemReporter(self.accumulator)
        a_schedule = transitfeed.Schedule()
        b_schedule = transitfeed.Schedule()
        merged_schedule = transitfeed.Schedule()
        self.feed_merger = merge.FeedMerger(
            a_schedule, b_schedule, merged_schedule, self.problem_reporter
        )
        self.dataset_merger = merge.TripMerger(None)

    def testGeneratesSomeHTML(self):
        self.problem_reporter.CalendarsNotDisjoint(self.dataset_merger)
        self.problem_reporter.MergeNotImplemented(self.dataset_merger)
        self.problem_reporter.FareRulesBroken(self.dataset_merger)
        self.problem_reporter.SameIdButNotMerged(
            self.dataset_merger, "test", "unknown reason"
        )

        output_file = StringIO()
        old_feed_path = "/path/to/old/feed"
        new_feed_path = "/path/to/new/feed"
        merged_feed_path = "/path/to/merged/feed"
        self.accumulator.WriteOutput(
            output_file,
            self.feed_merger,
            old_feed_path,
            new_feed_path,
            merged_feed_path,
        )

        html = output_file.getvalue()
        self.assertTrue(html.startswith("<html>"))
        self.assertTrue(html.endswith("</html>"))


class MergeInSubprocessTestCase(util.TempDirTestCaseBase):
    def CopyAndModifyTestData(self, zip_path, modify_file, old, new):
        """Return path of zip_path copy with old replaced by new in modify_file."""
        zipfile_mem = BytesIO(open(zip_path, "rb").read())
        old_zip = zipfile.ZipFile(zipfile_mem, "r")

        content_dict = self.ConvertZipToDict(old_zip)
        content_dict[modify_file] = content_dict[modify_file].replace(
            old.encode("utf-8"), new.encode("utf-8")
        )
        new_zipfile_mem = self.ConvertDictToZip(content_dict)

        new_zip_path = os.path.join(self.tempdirpath, "modified.zip")
        open(new_zip_path, "wb").write(new_zipfile_mem.getvalue())
        return new_zip_path

    def testCrashHandler(self):
        (out, err) = self.CheckCallWithPath(
            [
                self.GetPath("merge.py"),
                "--no_browser",
                "IWantMyCrash",
                "file2",
                "fileout.zip",
            ],
            expected_retcode=127,
        )
        self.assertMatchesRegex(r"Yikes", out)
        crashout = open("transitfeedcrash.txt").read()
        self.assertMatchesRegex(
            r"For testing the merge crash handler", crashout
        )

    def testMergeBadCommandLine(self):
        (out, err) = self.CheckCallWithPath(
            [self.GetPath("merge.py"), "--no_browser"], expected_retcode=2
        )
        self.assertFalse(out)
        self.assertMatchesRegex(r"command line arguments", err)
        self.assertFalse(os.path.exists("transitfeedcrash.txt"))

    def testMergeWithWarnings(self):
        # Make a copy of good_feed.zip which is not active until 20110101. This
        # avoids adding another tests/data file. good_feed.zip needs to remain error
        # free so it can't start in the future.
        future_good_feed = self.CopyAndModifyTestData(
            self.GetPath("tests/data/good_feed.zip"),
            "calendar.txt",
            "20070101",
            "20110101",
        )
        (out, err) = self.CheckCallWithPath(
            [
                self.GetPath("merge.py"),
                "--no_browser",
                self.GetPath("tests/data/unused_stop"),
                future_good_feed,
                os.path.join(self.tempdirpath, "merged-warnings.zip"),
            ],
            expected_retcode=0,
        )

    def testMergeWithErrors(self):
        # Make a copy of good_feed.zip which is not active until 20110101. This
        # avoids adding another tests/data file. good_feed.zip needs to remain error
        # free so it can't start in the future.
        future_good_feed = self.CopyAndModifyTestData(
            self.GetPath("tests/data/good_feed.zip"),
            "calendar.txt",
            "20070101",
            "20110101",
        )
        (out, err) = self.CheckCallWithPath(
            [
                self.GetPath("merge.py"),
                "--no_browser",
                self.GetPath("tests/data/unused_stop"),
                future_good_feed,
            ],
            expected_retcode=2,
        )

    def testCheckVersionIsRun(self):
        future_good_feed = self.CopyAndModifyTestData(
            self.GetPath("tests/data/good_feed.zip"),
            "calendar.txt",
            "20070101",
            "20110101",
        )
        (out, err) = self.CheckCallWithPath(
            [
                self.GetPath("merge.py"),
                "--no_browser",
                "--latest_version",
                "100.100.100",
                self.GetPath("tests/data/unused_stop"),
                future_good_feed,
                os.path.join(self.tempdirpath, "merged.zip"),
            ],
            expected_retcode=0,
        )
        print(out)
        htmlout = open("merge-results.html").read()
        self.assertTrue(re.search(r"A new version 100.100.100", htmlout))


if __name__ == "__main__":
    unittest.main()
