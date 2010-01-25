#!/usr/bin/python2.4
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


__author__ = 'timothy.stranex@gmail.com (Timothy Stranex)'


import merge
import os.path
import re
import StringIO
import transitfeed
import unittest
import util
import zipfile


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
  return transitfeed.Agency(name='agency',
                            url='http://agency',
                            timezone='Africa/Johannesburg',
                            id='agency')


class TestingProblemReporter(merge.MergeProblemReporterBase):
  """This problem reporter keeps track of all problems.

  Attributes:
    problems: The list of problems reported.
  """

  def __init__(self):
    merge.MergeProblemReporterBase.__init__(self)
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
      testcase.assert_(self.CheckReported(problem_class))


class TestApproximateDistanceBetweenPoints(util.TestCase):

  def _assertWithinEpsilon(self, a, b, epsilon=1.0):
    """Asserts that a and b are equal to within an epsilon.

    Args:
      a: The first value (float).
      b: The second value (float).
      epsilon: The epsilon value (float).
    """
    self.assert_(abs(a-b) < epsilon)

  def testDegenerate(self):
    p = (30.0, 30.0)
    self._assertWithinEpsilon(
        merge.ApproximateDistanceBetweenPoints(p, p), 0.0)

  def testFar(self):
    p1 = (30.0, 30.0)
    p2 = (40.0, 40.0)
    self.assert_(merge.ApproximateDistanceBetweenPoints(p1, p2) > 1e4)


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
    self.fm = merge.FeedMerger(a_schedule, b_schedule,
                               merged_schedule,
                               TestingProblemReporter())
    self.ds = merge.DataSetMerger(self.fm)

    def Migrate(ent, sched, newid):
      """A migration function for the mock entity."""
      return self.TestEntity(ent.x, ent.y, ent.z)
    self.ds._Migrate = Migrate

  def testMergeIdentical(self):
    class TestAttrib:
      """An object that is equal to everything."""

      def __cmp__(self, b):
        return 0

    x = 99
    a = TestAttrib()
    b = TestAttrib()

    self.assert_(self.ds._MergeIdentical(x, x) == x)
    self.assert_(self.ds._MergeIdentical(a, b) is b)
    self.assertRaises(merge.MergeError, self.ds._MergeIdentical, 1, 2)

  def testMergeIdenticalCaseInsensitive(self):
    self.assert_(self.ds._MergeIdenticalCaseInsensitive('abc', 'ABC') == 'ABC')
    self.assert_(self.ds._MergeIdenticalCaseInsensitive('abc', 'AbC') == 'AbC')
    self.assertRaises(merge.MergeError,
                      self.ds._MergeIdenticalCaseInsensitive, 'abc', 'bcd')
    self.assertRaises(merge.MergeError,
                      self.ds._MergeIdenticalCaseInsensitive, 'abc', 'ABCD')

  def testMergeOptional(self):
    x = 99
    y = 100

    self.assertEquals(self.ds._MergeOptional(None, None), None)
    self.assertEquals(self.ds._MergeOptional(None, x), x)
    self.assertEquals(self.ds._MergeOptional(x, None), x)
    self.assertEquals(self.ds._MergeOptional(x, x), x)
    self.assertRaises(merge.MergeError, self.ds._MergeOptional, x, y)

  def testMergeSameAgency(self):
    kwargs = {'name': 'xxx',
              'agency_url': 'http://www.example.com',
              'agency_timezone': 'Europe/Zurich'}
    id1 = 'agency1'
    id2 = 'agency2'
    id3 = 'agency3'
    id4 = 'agency4'
    id5 = 'agency5'

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

    self.assertEquals(self.ds._MergeSameAgency(id1, id2), id3)
    self.assertEquals(self.ds._MergeSameAgency(None, None), id3)
    self.assertEquals(self.ds._MergeSameAgency(id1, None), id3)
    self.assertEquals(self.ds._MergeSameAgency(None, id2), id3)

    # id1 is not a valid agency_id in the new schedule so it cannot be merged
    self.assertRaises(KeyError, self.ds._MergeSameAgency, id1, id1)

    # this fails because d (id4) and b (id2) don't map to the same agency
    # in the merged schedule
    self.assertRaises(merge.MergeError, self.ds._MergeSameAgency, id4, id2)

  def testSchemedMerge_Success(self):

    def Merger(a, b):
      return a + b

    scheme = {'x': Merger, 'y': Merger, 'z': Merger}
    a = self.TestEntity(1, 2, 3)
    b = self.TestEntity(4, 5, 6)
    c = self.ds._SchemedMerge(scheme, a, b)

    self.assertEquals(c.x, 5)
    self.assertEquals(c.y, 7)
    self.assertEquals(c.z, 9)

  def testSchemedMerge_Failure(self):

    def Merger(a, b):
      raise merge.MergeError()

    scheme = {'x': Merger, 'y': Merger, 'z': Merger}
    a = self.TestEntity(1, 2, 3)
    b = self.TestEntity(4, 5, 6)

    self.assertRaises(merge.MergeError, self.ds._SchemedMerge,
                      scheme, a, b)

  def testSchemedMerge_NoNewId(self):
    class TestDataSetMerger(merge.DataSetMerger):
      def _Migrate(self, entity, schedule, newid):
        self.newid = newid
        return entity
    dataset_merger = TestDataSetMerger(self.fm)
    a = self.TestEntity(1, 2, 3)
    b = self.TestEntity(4, 5, 6)
    dataset_merger._SchemedMerge({}, a, b)
    self.assertEquals(dataset_merger.newid, False)

  def testSchemedMerge_ErrorTextContainsAttributeNameAndReason(self):
    reason = 'my reason'
    attribute_name = 'long_attribute_name'

    def GoodMerger(a, b):
      return a + b

    def BadMerger(a, b):
      raise merge.MergeError(reason)

    a = self.TestEntity(1, 2, 3)
    setattr(a, attribute_name, 1)
    b = self.TestEntity(4, 5, 6)
    setattr(b, attribute_name, 2)
    scheme = {'x': GoodMerger, 'y': GoodMerger, 'z': GoodMerger,
              attribute_name: BadMerger}

    try:
      self.ds._SchemedMerge(scheme, a, b)
    except merge.MergeError, merge_error:
      error_text = str(merge_error)
      self.assert_(reason in error_text)
      self.assert_(attribute_name in error_text)


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
    self.fm = merge.FeedMerger(a_schedule, b_schedule,
                               merged_schedule,
                               TestingProblemReporter())
    self.called = []

  def testDefaultProblemReporter(self):
    feed_merger = merge.FeedMerger(self.fm.a_schedule,
                                   self.fm.b_schedule,
                                   self.fm.merged_schedule,
                                   None)
    self.assert_(isinstance(feed_merger.problem_reporter,
                            merge.MergeProblemReporterBase))

  def testSequence(self):
    for i in range(10):
      self.fm.AddMerger(TestFeedMerger.Merger(self, i))
    self.assert_(self.fm.MergeSchedules())
    self.assertEquals(self.called, range(10))

  def testStopsAfterError(self):
    for i in range(10):
      self.fm.AddMerger(TestFeedMerger.Merger(self, i, i == 5))
    self.assert_(not self.fm.MergeSchedules())
    self.assertEquals(self.called, range(6))

  def testRegister(self):
    s1 = transitfeed.Stop(stop_id='1')
    s2 = transitfeed.Stop(stop_id='2')
    s3 = transitfeed.Stop(stop_id='3')
    self.fm.Register(s1, s2, s3)
    self.assertEquals(self.fm.a_merge_map, {s1: s3})
    self.assertEquals('3', s1._migrated_entity.stop_id)
    self.assertEquals(self.fm.b_merge_map, {s2: s3})
    self.assertEquals('3', s2._migrated_entity.stop_id)

  def testRegisterNone(self):
    s2 = transitfeed.Stop(stop_id='2')
    s3 = transitfeed.Stop(stop_id='3')
    self.fm.Register(None, s2, s3)
    self.assertEquals(self.fm.a_merge_map, {})
    self.assertEquals(self.fm.b_merge_map, {s2: s3})
    self.assertEquals('3', s2._migrated_entity.stop_id)

  def testGenerateId_Prefix(self):
    x = 'test'
    a = self.fm.GenerateId(x)
    b = self.fm.GenerateId(x)
    self.assertNotEqual(a, b)
    self.assert_(a.startswith(x))
    self.assert_(b.startswith(x))

  def testGenerateId_None(self):
    a = self.fm.GenerateId(None)
    b = self.fm.GenerateId(None)
    self.assertNotEqual(a, b)

  def testGenerateId_InitialCounter(self):
    a_schedule = transitfeed.Schedule()
    b_schedule = transitfeed.Schedule()
    merged_schedule = transitfeed.Schedule()

    for i in range(10):
      agency = transitfeed.Agency(name='agency', url='http://agency',
                                  timezone='Africa/Johannesburg',
                                  id='agency_%d' % i)
      if i % 2:
        b_schedule.AddAgencyObject(agency)
      else:
        a_schedule.AddAgencyObject(agency)

    feed_merger = merge.FeedMerger(a_schedule, b_schedule,
                                   merged_schedule,
                                   TestingProblemReporter())

    # check that the postfix number of any generated ids are greater than
    # the postfix numbers of any ids in the old and new schedules
    gen_id = feed_merger.GenerateId(None)
    postfix_num = int(gen_id[gen_id.rfind('_')+1:])
    self.assert_(postfix_num >= 10)

  def testGetMerger(self):
    class MergerA(merge.DataSetMerger):
      pass

    class MergerB(merge.DataSetMerger):
      pass

    a = MergerA(self.fm)
    b = MergerB(self.fm)

    self.fm.AddMerger(a)
    self.fm.AddMerger(b)

    self.assertEquals(self.fm.GetMerger(MergerA), a)
    self.assertEquals(self.fm.GetMerger(MergerB), b)

  def testGetMerger_Error(self):
    self.assertRaises(LookupError, self.fm.GetMerger, TestFeedMerger.Merger)


class TestServicePeriodMerger(util.TestCase):

  def setUp(self):
    a_schedule = transitfeed.Schedule()
    b_schedule = transitfeed.Schedule()
    merged_schedule = transitfeed.Schedule()
    self.fm = merge.FeedMerger(a_schedule, b_schedule, merged_schedule,
                               TestingProblemReporter())
    self.spm = merge.ServicePeriodMerger(self.fm)
    self.fm.AddMerger(self.spm)

  def _AddTwoPeriods(self, start1, end1, start2, end2):
    sp1fields = ['test1', start1, end1] + ['1']*7
    self.sp1 = transitfeed.ServicePeriod(field_list=sp1fields)
    sp2fields = ['test2', start2, end2] + ['1']*7
    self.sp2 = transitfeed.ServicePeriod(field_list=sp2fields)

    self.fm.a_schedule.AddServicePeriodObject(self.sp1)
    self.fm.b_schedule.AddServicePeriodObject(self.sp2)

  def testCheckDisjoint_True(self):
    self._AddTwoPeriods('20071213', '20071231',
                        '20080101', '20080201')
    self.assert_(self.spm.CheckDisjointCalendars())

  def testCheckDisjoint_False1(self):
    self._AddTwoPeriods('20071213', '20080201',
                        '20080101', '20080301')
    self.assert_(not self.spm.CheckDisjointCalendars())

  def testCheckDisjoint_False2(self):
    self._AddTwoPeriods('20080101', '20090101',
                        '20070101', '20080601')
    self.assert_(not self.spm.CheckDisjointCalendars())

  def testCheckDisjoint_False3(self):
    self._AddTwoPeriods('20080301', '20080901',
                        '20080101', '20090101')
    self.assert_(not self.spm.CheckDisjointCalendars())

  def testDisjoinCalendars(self):
    self._AddTwoPeriods('20071213', '20080201',
                        '20080101', '20080301')
    self.spm.DisjoinCalendars('20080101')
    self.assertEquals(self.sp1.start_date, '20071213')
    self.assertEquals(self.sp1.end_date, '20071231')
    self.assertEquals(self.sp2.start_date, '20080101')
    self.assertEquals(self.sp2.end_date, '20080301')

  def testDisjoinCalendars_Dates(self):
    self._AddTwoPeriods('20071213', '20080201',
                        '20080101', '20080301')
    self.sp1.SetDateHasService('20071201')
    self.sp1.SetDateHasService('20081231')
    self.sp2.SetDateHasService('20071201')
    self.sp2.SetDateHasService('20081231')

    self.spm.DisjoinCalendars('20080101')

    self.assert_('20071201' in self.sp1.date_exceptions.keys())
    self.assert_('20081231' not in self.sp1.date_exceptions.keys())
    self.assert_('20071201' not in self.sp2.date_exceptions.keys())
    self.assert_('20081231' in self.sp2.date_exceptions.keys())

  def testUnion(self):
    self._AddTwoPeriods('20071213', '20071231',
                        '20080101', '20080201')
    self.fm.problem_reporter.ExpectProblemClass(merge.MergeNotImplemented)
    self.fm.MergeSchedules()
    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetServicePeriodList()), 2)

    # make fields a copy of the service period attributes except service_id
    fields = list(transitfeed.ServicePeriod._DAYS_OF_WEEK)
    fields += ['start_date', 'end_date']

    # now check that these attributes are preserved in the merge
    CheckAttribs(self.sp1, self.fm.a_merge_map[self.sp1], fields,
                 self.assertEquals)
    CheckAttribs(self.sp2, self.fm.b_merge_map[self.sp2], fields,
                 self.assertEquals)

    self.fm.problem_reporter.assertExpectedProblemsReported(self)

  def testMerge_RequiredButNotDisjoint(self):
    self._AddTwoPeriods('20070101', '20090101',
                        '20080101', '20100101')
    self.fm.problem_reporter.ExpectProblemClass(merge.CalendarsNotDisjoint)
    self.assertEquals(self.spm.MergeDataSets(), False)
    self.fm.problem_reporter.assertExpectedProblemsReported(self)

  def testMerge_NotRequiredAndNotDisjoint(self):
    self._AddTwoPeriods('20070101', '20090101',
                        '20080101', '20100101')
    self.spm.require_disjoint_calendars = False
    self.fm.problem_reporter.ExpectProblemClass(merge.MergeNotImplemented)
    self.fm.MergeSchedules()
    self.fm.problem_reporter.assertExpectedProblemsReported(self)


class TestAgencyMerger(util.TestCase):

  def setUp(self):
    a_schedule = transitfeed.Schedule()
    b_schedule = transitfeed.Schedule()
    merged_schedule = transitfeed.Schedule()
    self.fm = merge.FeedMerger(a_schedule, b_schedule, merged_schedule,
                               TestingProblemReporter())
    self.am = merge.AgencyMerger(self.fm)
    self.fm.AddMerger(self.am)

    self.a1 = transitfeed.Agency(id='a1', agency_name='a1',
                                 agency_url='http://www.a1.com',
                                 agency_timezone='Africa/Johannesburg',
                                 agency_phone='123 456 78 90')
    self.a2 = transitfeed.Agency(id='a2', agency_name='a1',
                                 agency_url='http://www.a1.com',
                                 agency_timezone='Africa/Johannesburg',
                                 agency_phone='789 65 43 21')

  def testMerge(self):
    self.a2.agency_id = self.a1.agency_id
    self.fm.a_schedule.AddAgencyObject(self.a1)
    self.fm.b_schedule.AddAgencyObject(self.a2)
    self.fm.MergeSchedules()

    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetAgencyList()), 1)
    self.assertEquals(merged_schedule.GetAgencyList()[0],
                      self.fm.a_merge_map[self.a1])
    self.assertEquals(self.fm.a_merge_map[self.a1],
                      self.fm.b_merge_map[self.a2])
    # differing values such as agency_phone should be taken from self.a2
    self.assertEquals(merged_schedule.GetAgencyList()[0], self.a2)
    self.assertEquals(self.am.GetMergeStats(), (1, 0, 0))

    # check that id is preserved
    self.assertEquals(self.fm.a_merge_map[self.a1].agency_id,
                      self.a1.agency_id)

  def testNoMerge_DifferentId(self):
    self.fm.a_schedule.AddAgencyObject(self.a1)
    self.fm.b_schedule.AddAgencyObject(self.a2)
    self.fm.MergeSchedules()

    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetAgencyList()), 2)

    self.assert_(self.fm.a_merge_map[self.a1] in
                 merged_schedule.GetAgencyList())
    self.assert_(self.fm.b_merge_map[self.a2] in
                 merged_schedule.GetAgencyList())
    self.assertEquals(self.a1, self.fm.a_merge_map[self.a1])
    self.assertEquals(self.a2, self.fm.b_merge_map[self.a2])
    self.assertEquals(self.am.GetMergeStats(), (0, 1, 1))

    # check that the ids are preserved
    self.assertEquals(self.fm.a_merge_map[self.a1].agency_id,
                      self.a1.agency_id)
    self.assertEquals(self.fm.b_merge_map[self.a2].agency_id,
                      self.a2.agency_id)

  def testNoMerge_SameId(self):
    # Force a1.agency_id to be unicode to make sure it is correctly encoded
    # to utf-8 before concatinating to the agency_name containing non-ascii
    # characters.
    self.a1.agency_id = unicode(self.a1.agency_id)
    self.a2.agency_id = str(self.a1.agency_id)
    self.a2.agency_name = 'different \xc3\xa9'
    self.fm.a_schedule.AddAgencyObject(self.a1)
    self.fm.b_schedule.AddAgencyObject(self.a2)

    self.fm.problem_reporter.ExpectProblemClass(merge.SameIdButNotMerged)
    self.fm.MergeSchedules()

    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetAgencyList()), 2)
    self.assertEquals(self.am.GetMergeStats(), (0, 1, 1))

    # check that the merged entities have different ids
    self.assertNotEqual(self.fm.a_merge_map[self.a1].agency_id,
                        self.fm.b_merge_map[self.a2].agency_id)

    self.fm.problem_reporter.assertExpectedProblemsReported(self)


class TestStopMerger(util.TestCase):

  def setUp(self):
    a_schedule = transitfeed.Schedule()
    b_schedule = transitfeed.Schedule()
    merged_schedule = transitfeed.Schedule()
    self.fm = merge.FeedMerger(a_schedule, b_schedule, merged_schedule,
                               TestingProblemReporter())
    self.sm = merge.StopMerger(self.fm)
    self.fm.AddMerger(self.sm)

    self.s1 = transitfeed.Stop(30.0, 30.0,
                               u'Andr\202' , 's1')
    self.s1.stop_desc = 'stop 1'
    self.s1.stop_url = 'http://stop/1'
    self.s1.zone_id = 'zone1'
    self.s2 = transitfeed.Stop(30.0, 30.0, 's2', 's2')
    self.s2.stop_desc = 'stop 2'
    self.s2.stop_url = 'http://stop/2'
    self.s2.zone_id = 'zone1'

  def testMerge(self):
    self.s2.stop_id = self.s1.stop_id
    self.s2.stop_name = self.s1.stop_name
    self.s1.location_type = 1
    self.s2.location_type = 1

    self.fm.a_schedule.AddStopObject(self.s1)
    self.fm.b_schedule.AddStopObject(self.s2)
    self.fm.MergeSchedules()

    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetStopList()), 1)
    self.assertEquals(merged_schedule.GetStopList()[0],
                      self.fm.a_merge_map[self.s1])
    self.assertEquals(self.fm.a_merge_map[self.s1],
                      self.fm.b_merge_map[self.s2])
    self.assertEquals(self.sm.GetMergeStats(), (1, 0, 0))

    # check that the remaining attributes are taken from the new stop
    fields = ['stop_name', 'stop_lat', 'stop_lon', 'stop_desc', 'stop_url',
              'location_type']
    CheckAttribs(self.fm.a_merge_map[self.s1], self.s2, fields,
                 self.assertEquals)

    # check that the id is preserved
    self.assertEquals(self.fm.a_merge_map[self.s1].stop_id, self.s1.stop_id)

    # check that the zone_id is preserved
    self.assertEquals(self.fm.a_merge_map[self.s1].zone_id, self.s1.zone_id)

  def testNoMerge_DifferentId(self):
    self.fm.a_schedule.AddStopObject(self.s1)
    self.fm.b_schedule.AddStopObject(self.s2)
    self.fm.MergeSchedules()

    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetStopList()), 2)
    self.assert_(self.fm.a_merge_map[self.s1] in merged_schedule.GetStopList())
    self.assert_(self.fm.b_merge_map[self.s2] in merged_schedule.GetStopList())
    self.assertEquals(self.sm.GetMergeStats(), (0, 1, 1))

  def testNoMerge_DifferentName(self):
    self.s2.stop_id = self.s1.stop_id
    self.fm.a_schedule.AddStopObject(self.s1)
    self.fm.b_schedule.AddStopObject(self.s2)
    self.fm.problem_reporter.ExpectProblemClass(merge.SameIdButNotMerged)
    self.fm.MergeSchedules()

    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetStopList()), 2)
    self.assert_(self.fm.a_merge_map[self.s1] in merged_schedule.GetStopList())
    self.assert_(self.fm.b_merge_map[self.s2] in merged_schedule.GetStopList())
    self.assertEquals(self.sm.GetMergeStats(), (0, 1, 1))

  def testNoMerge_FarApart(self):
    self.s2.stop_id = self.s1.stop_id
    self.s2.stop_name = self.s1.stop_name
    self.s2.stop_lat = 40.0
    self.s2.stop_lon = 40.0

    self.fm.a_schedule.AddStopObject(self.s1)
    self.fm.b_schedule.AddStopObject(self.s2)
    self.fm.problem_reporter.ExpectProblemClass(merge.SameIdButNotMerged)
    self.fm.MergeSchedules()

    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetStopList()), 2)
    self.assert_(self.fm.a_merge_map[self.s1] in merged_schedule.GetStopList())
    self.assert_(self.fm.b_merge_map[self.s2] in merged_schedule.GetStopList())
    self.assertEquals(self.sm.GetMergeStats(), (0, 1, 1))

    # check that the merged ids are different
    self.assertNotEquals(self.fm.a_merge_map[self.s1].stop_id,
                         self.fm.b_merge_map[self.s2].stop_id)

    self.fm.problem_reporter.assertExpectedProblemsReported(self)

  def testMerge_CaseInsensitive(self):
    self.s2.stop_id = self.s1.stop_id
    self.s2.stop_name = self.s1.stop_name.upper()
    self.fm.a_schedule.AddStopObject(self.s1)
    self.fm.b_schedule.AddStopObject(self.s2)
    self.fm.MergeSchedules()
    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetStopList()), 1)
    self.assertEquals(self.sm.GetMergeStats(), (1, 0, 0))

  def testNoMerge_ZoneId(self):
    self.s2.zone_id = 'zone2'
    self.fm.a_schedule.AddStopObject(self.s1)
    self.fm.b_schedule.AddStopObject(self.s2)
    self.fm.MergeSchedules()

    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetStopList()), 2)

    self.assert_(self.s1.zone_id in self.fm.a_zone_map)
    self.assert_(self.s2.zone_id in self.fm.b_zone_map)
    self.assertEquals(self.sm.GetMergeStats(), (0, 1, 1))

    # check that the zones are still different
    self.assertNotEqual(self.fm.a_merge_map[self.s1].zone_id,
                        self.fm.b_merge_map[self.s2].zone_id)

  def testZoneId_SamePreservation(self):
    # checks that if the zone_ids of some stops are the same before the
    # merge, they are still the same after.
    self.fm.a_schedule.AddStopObject(self.s1)
    self.fm.a_schedule.AddStopObject(self.s2)
    self.fm.MergeSchedules()
    self.assertEquals(self.fm.a_merge_map[self.s1].zone_id,
                      self.fm.a_merge_map[self.s2].zone_id)

  def testZoneId_DifferentSchedules(self):
    # zone_ids may be the same in different schedules but unless the stops
    # are merged, they should map to different zone_ids
    self.fm.a_schedule.AddStopObject(self.s1)
    self.fm.b_schedule.AddStopObject(self.s2)
    self.fm.MergeSchedules()
    self.assertNotEquals(self.fm.a_merge_map[self.s1].zone_id,
                         self.fm.b_merge_map[self.s2].zone_id)

  def testZoneId_MergePreservation(self):
    # check that if two stops are merged, the zone mapping is used for all
    # other stops too
    self.s2.stop_id = self.s1.stop_id
    self.s2.stop_name = self.s1.stop_name
    s3 = transitfeed.Stop(field_dict=self.s1)
    s3.stop_id = 'different'

    self.fm.a_schedule.AddStopObject(self.s1)
    self.fm.a_schedule.AddStopObject(s3)
    self.fm.b_schedule.AddStopObject(self.s2)
    self.fm.MergeSchedules()

    self.assertEquals(self.fm.a_merge_map[self.s1].zone_id,
                      self.fm.a_merge_map[s3].zone_id)
    self.assertEquals(self.fm.a_merge_map[s3].zone_id,
                      self.fm.b_merge_map[self.s2].zone_id)

  def testMergeStationType(self):
    self.s2.stop_id = self.s1.stop_id
    self.s2.stop_name = self.s1.stop_name
    self.s1.location_type = 1
    self.s2.location_type = 1
    self.fm.a_schedule.AddStopObject(self.s1)
    self.fm.b_schedule.AddStopObject(self.s2)
    self.fm.MergeSchedules()
    merged_stops = self.fm.GetMergedSchedule().GetStopList()
    self.assertEquals(len(merged_stops), 1)
    self.assertEquals(merged_stops[0].location_type, 1)

  def testMergeDifferentTypes(self):
    self.s2.stop_id = self.s1.stop_id
    self.s2.stop_name = self.s1.stop_name
    self.s2.location_type = 1
    self.fm.a_schedule.AddStopObject(self.s1)
    self.fm.b_schedule.AddStopObject(self.s2)
    try:
      self.fm.MergeSchedules()
      self.fail("Expecting MergeError")
    except merge.SameIdButNotMerged, merge_error:
      self.assertTrue(("%s" % merge_error).find("location_type") != -1)

  def AssertS1ParentIsS2(self):
    """Assert that the merged s1 has parent s2."""
    new_s1 = self.s1._migrated_entity
    new_s2 = self.s2._migrated_entity
    self.assertEquals(new_s1.parent_station, new_s2.stop_id)
    self.assertEquals(new_s2.parent_station, None)
    self.assertEquals(new_s1.location_type, 0)
    self.assertEquals(new_s2.location_type, 1)

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
    self.fm.problem_reporter.ExpectProblemClass(merge.SameIdButNotMerged)
    self.fm.MergeSchedules()
    self.assertNotEquals(s3._migrated_entity.stop_id,
                         self.s2._migrated_entity.stop_id)
    # Check that s2 got a new id
    self.assertNotEquals(self.s2.stop_id,
                         self.s2._migrated_entity.stop_id)
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
    self.assertEquals(self.sm.largest_stop_distance, largest_stop_distance)
    self.fm.problem_reporter.ExpectProblemClass(merge.SameIdButNotMerged)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.GetMergedSchedule().GetStopList()), 2)
    self.fm.problem_reporter.assertExpectedProblemsReported(self)

  def testSetLargestStopDistanceLarge(self):
    largest_stop_distance = self._AddStopsApart() * 2.0
    self.sm.SetLargestStopDistance(largest_stop_distance)
    self.assertEquals(self.sm.largest_stop_distance, largest_stop_distance)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.GetMergedSchedule().GetStopList()), 1)


class TestRouteMerger(util.TestCase):

  fields = ['route_short_name', 'route_long_name', 'route_type',
            'route_url']

  def setUp(self):
    a_schedule = transitfeed.Schedule()
    b_schedule = transitfeed.Schedule()
    merged_schedule = transitfeed.Schedule()
    self.fm = merge.FeedMerger(a_schedule, b_schedule, merged_schedule,
                               TestingProblemReporter())
    self.fm.AddMerger(merge.AgencyMerger(self.fm))
    self.rm = merge.RouteMerger(self.fm)
    self.fm.AddMerger(self.rm)

    akwargs = {'id': 'a1',
               'agency_name': 'a1',
               'agency_url': 'http://www.a1.com',
               'agency_timezone': 'Europe/Zurich'}
    self.a1 = transitfeed.Agency(**akwargs)
    self.a2 = transitfeed.Agency(**akwargs)
    a_schedule.AddAgencyObject(self.a1)
    b_schedule.AddAgencyObject(self.a2)

    rkwargs = {'route_id': 'r1',
               'agency_id': 'a1',
               'short_name': 'r1',
               'long_name': 'r1r1',
               'route_type': '0'}
    self.r1 = transitfeed.Route(**rkwargs)
    self.r2 = transitfeed.Route(**rkwargs)
    self.r2.route_url = 'http://route/2'

  def testMerge(self):
    self.fm.a_schedule.AddRouteObject(self.r1)
    self.fm.b_schedule.AddRouteObject(self.r2)
    self.fm.MergeSchedules()

    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetRouteList()), 1)
    r = merged_schedule.GetRouteList()[0]
    self.assert_(self.fm.a_merge_map[self.r1] is r)
    self.assert_(self.fm.b_merge_map[self.r2] is r)
    CheckAttribs(self.r2, r, self.fields, self.assertEquals)
    self.assertEquals(r.agency_id, self.fm.a_merge_map[self.a1].agency_id)
    self.assertEquals(self.rm.GetMergeStats(), (1, 0, 0))

    # check that the id is preserved
    self.assertEquals(self.fm.a_merge_map[self.r1].route_id, self.r1.route_id)

  def testMergeNoAgency(self):
    self.r1.agency_id = None
    self.r2.agency_id = None
    self.fm.a_schedule.AddRouteObject(self.r1)
    self.fm.b_schedule.AddRouteObject(self.r2)
    self.fm.MergeSchedules()

    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetRouteList()), 1)
    r = merged_schedule.GetRouteList()[0]
    CheckAttribs(self.r2, r, self.fields, self.assertEquals)
    # Merged route has copy of default agency_id
    self.assertEquals(r.agency_id, self.a1.agency_id)
    self.assertEquals(self.rm.GetMergeStats(), (1, 0, 0))

    # check that the id is preserved
    self.assertEquals(self.fm.a_merge_map[self.r1].route_id, self.r1.route_id)

  def testMigrateNoAgency(self):
    self.r1.agency_id = None
    self.fm.a_schedule.AddRouteObject(self.r1)
    self.fm.MergeSchedules()
    merged_schedule = self.fm.GetMergedSchedule()
    self.assertEquals(len(merged_schedule.GetRouteList()), 1)
    r = merged_schedule.GetRouteList()[0]
    CheckAttribs(self.r1, r, self.fields, self.assertEquals)
    # Migrated route has copy of default agency_id
    self.assertEquals(r.agency_id, self.a1.agency_id)

  def testNoMerge_DifferentId(self):
    self.r2.route_id = 'r2'
    self.fm.a_schedule.AddRouteObject(self.r1)
    self.fm.b_schedule.AddRouteObject(self.r2)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.GetMergedSchedule().GetRouteList()), 2)
    self.assertEquals(self.rm.GetMergeStats(), (0, 1, 1))

  def testNoMerge_SameId(self):
    self.r2.route_short_name = 'different'
    self.fm.a_schedule.AddRouteObject(self.r1)
    self.fm.b_schedule.AddRouteObject(self.r2)
    self.fm.problem_reporter.ExpectProblemClass(merge.SameIdButNotMerged)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.GetMergedSchedule().GetRouteList()), 2)
    self.assertEquals(self.rm.GetMergeStats(), (0, 1, 1))

    # check that the merged ids are different
    self.assertNotEquals(self.fm.a_merge_map[self.r1].route_id,
                         self.fm.b_merge_map[self.r2].route_id)

    self.fm.problem_reporter.assertExpectedProblemsReported(self)


class TestTripMerger(util.TestCase):

  def setUp(self):
    a_schedule = transitfeed.Schedule()
    b_schedule = transitfeed.Schedule()
    merged_schedule = transitfeed.Schedule()
    self.fm = merge.FeedMerger(a_schedule, b_schedule, merged_schedule,
                               TestingProblemReporter())
    self.fm.AddDefaultMergers()
    self.tm = self.fm.GetMerger(merge.TripMerger)

    akwargs = {'id': 'a1',
               'agency_name': 'a1',
               'agency_url': 'http://www.a1.com',
               'agency_timezone': 'Europe/Zurich'}
    self.a1 = transitfeed.Agency(**akwargs)

    rkwargs = {'route_id': 'r1',
               'agency_id': 'a1',
               'short_name': 'r1',
               'long_name': 'r1r1',
               'route_type': '0'}
    self.r1 = transitfeed.Route(**rkwargs)

    self.s1 = transitfeed.ServicePeriod('s1')
    self.s1.start_date = '20071201'
    self.s1.end_date = '20071231'
    self.s1.SetWeekdayService()

    self.shape = transitfeed.Shape('shape1')
    self.shape.AddPoint(30.0, 30.0)

    self.t1 = transitfeed.Trip(service_period=self.s1,
                               route=self.r1, trip_id='t1')
    self.t2 = transitfeed.Trip(service_period=self.s1,
                               route=self.r1, trip_id='t2')
    # Must add self.t1 to a schedule before calling self.t1.AddStopTime
    a_schedule.AddTripObject(self.t1, validate=False)
    a_schedule.AddTripObject(self.t2, validate=False)
    self.t1.block_id = 'b1'
    self.t2.block_id = 'b1'
    self.t1.shape_id = 'shape1'

    self.stop = transitfeed.Stop(30.0, 30.0, stop_id='stop1')
    self.t1.AddStopTime(self.stop, arrival_secs=0, departure_secs=0)

    a_schedule.AddAgencyObject(self.a1)
    a_schedule.AddStopObject(self.stop)
    a_schedule.AddRouteObject(self.r1)
    a_schedule.AddServicePeriodObject(self.s1)
    a_schedule.AddShapeObject(self.shape)

  def testMigrate(self):
    self.fm.problem_reporter.ExpectProblemClass(merge.MergeNotImplemented)
    self.fm.MergeSchedules()
    self.fm.problem_reporter.assertExpectedProblemsReported(self)

    r = self.fm.a_merge_map[self.r1]
    s = self.fm.a_merge_map[self.s1]
    shape = self.fm.a_merge_map[self.shape]
    t1 = self.fm.a_merge_map[self.t1]
    t2 = self.fm.a_merge_map[self.t2]

    self.assertEquals(t1.route_id, r.route_id)
    self.assertEquals(t1.service_id, s.service_id)
    self.assertEquals(t1.shape_id, shape.shape_id)
    self.assertEquals(t1.block_id, t2.block_id)

    self.assertEquals(len(t1.GetStopTimes()), 1)
    st = t1.GetStopTimes()[0]
    self.assertEquals(st.stop, self.fm.a_merge_map[self.stop])

  def testReportsNotImplementedProblem(self):
    self.fm.problem_reporter.ExpectProblemClass(merge.MergeNotImplemented)
    self.fm.MergeSchedules()
    self.fm.problem_reporter.assertExpectedProblemsReported(self)

  def testMergeStats(self):
    self.assert_(self.tm.GetMergeStats() is None)

  def testConflictingTripid(self):
    a1_in_b = transitfeed.Agency(field_dict=self.a1)
    r1_in_b = transitfeed.Route(field_dict=self.r1)
    t1_in_b = transitfeed.Trip(field_dict=self.t1)
    shape_in_b = transitfeed.Shape('shape1')
    shape_in_b.AddPoint(30.0, 30.0)
    s_in_b = transitfeed.ServicePeriod('s1')
    s_in_b.start_date = '20080101'
    s_in_b.end_date = '20080131'
    s_in_b.SetWeekdayService()

    self.fm.b_schedule.AddAgencyObject(a1_in_b)
    self.fm.b_schedule.AddRouteObject(r1_in_b)
    self.fm.b_schedule.AddShapeObject(shape_in_b)
    self.fm.b_schedule.AddTripObject(t1_in_b, validate=False)
    self.fm.b_schedule.AddServicePeriodObject(s_in_b, validate=False)
    self.fm.problem_reporter.ExpectProblemClass(merge.MergeNotImplemented)
    self.fm.MergeSchedules()
    # 3 trips moved to merged_schedule: from a_schedule t1, t2 and from
    # b_schedule t1
    self.assertEquals(len(self.fm.merged_schedule.GetTripList()), 3)


class TestFareMerger(util.TestCase):

  def setUp(self):
    a_schedule = transitfeed.Schedule()
    b_schedule = transitfeed.Schedule()
    merged_schedule = transitfeed.Schedule()
    self.fm = merge.FeedMerger(a_schedule, b_schedule, merged_schedule,
                               TestingProblemReporter())
    self.faremerger = merge.FareMerger(self.fm)
    self.fm.AddMerger(self.faremerger)

    self.f1 = transitfeed.Fare('f1', '10', 'ZAR', '1', '0')
    self.f2 = transitfeed.Fare('f2', '10', 'ZAR', '1', '0')

  def testMerge(self):
    self.f2.fare_id = self.f1.fare_id
    self.fm.a_schedule.AddFareObject(self.f1)
    self.fm.b_schedule.AddFareObject(self.f2)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.merged_schedule.GetFareList()), 1)
    self.assertEquals(self.faremerger.GetMergeStats(), (1, 0, 0))

    # check that the id is preserved
    self.assertEquals(self.fm.a_merge_map[self.f1].fare_id, self.f1.fare_id)

  def testNoMerge_DifferentPrice(self):
    self.f2.fare_id = self.f1.fare_id
    self.f2.price = 11.0
    self.fm.a_schedule.AddFareObject(self.f1)
    self.fm.b_schedule.AddFareObject(self.f2)
    self.fm.problem_reporter.ExpectProblemClass(merge.SameIdButNotMerged)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.merged_schedule.GetFareList()), 2)
    self.assertEquals(self.faremerger.GetMergeStats(), (0, 1, 1))

    # check that the merged ids are different
    self.assertNotEquals(self.fm.a_merge_map[self.f1].fare_id,
                         self.fm.b_merge_map[self.f2].fare_id)

    self.fm.problem_reporter.assertExpectedProblemsReported(self)

  def testNoMerge_DifferentId(self):
    self.fm.a_schedule.AddFareObject(self.f1)
    self.fm.b_schedule.AddFareObject(self.f2)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.merged_schedule.GetFareList()), 2)
    self.assertEquals(self.faremerger.GetMergeStats(), (0, 1, 1))

    # check that the ids are preserved
    self.assertEquals(self.fm.a_merge_map[self.f1].fare_id, self.f1.fare_id)
    self.assertEquals(self.fm.b_merge_map[self.f2].fare_id, self.f2.fare_id)


class TestShapeMerger(util.TestCase):

  def setUp(self):
    a_schedule = transitfeed.Schedule()
    b_schedule = transitfeed.Schedule()
    merged_schedule = transitfeed.Schedule()
    self.fm = merge.FeedMerger(a_schedule, b_schedule, merged_schedule,
                               TestingProblemReporter())
    self.sm = merge.ShapeMerger(self.fm)
    self.fm.AddMerger(self.sm)

    # setup some shapes
    # s1 and s2 have the same endpoints but take different paths
    # s3 has different endpoints to s1 and s2

    self.s1 = transitfeed.Shape('s1')
    self.s1.AddPoint(30.0, 30.0)
    self.s1.AddPoint(40.0, 30.0)
    self.s1.AddPoint(50.0, 50.0)

    self.s2 = transitfeed.Shape('s2')
    self.s2.AddPoint(30.0, 30.0)
    self.s2.AddPoint(40.0, 35.0)
    self.s2.AddPoint(50.0, 50.0)

    self.s3 = transitfeed.Shape('s3')
    self.s3.AddPoint(31.0, 31.0)
    self.s3.AddPoint(45.0, 35.0)
    self.s3.AddPoint(51.0, 51.0)

  def testMerge(self):
    self.s2.shape_id = self.s1.shape_id
    self.fm.a_schedule.AddShapeObject(self.s1)
    self.fm.b_schedule.AddShapeObject(self.s2)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.merged_schedule.GetShapeList()), 1)
    self.assertEquals(self.fm.merged_schedule.GetShapeList()[0], self.s2)
    self.assertEquals(self.sm.GetMergeStats(), (1, 0, 0))

    # check that the id is preserved
    self.assertEquals(self.fm.a_merge_map[self.s1].shape_id, self.s1.shape_id)

  def testNoMerge_DifferentId(self):
    self.fm.a_schedule.AddShapeObject(self.s1)
    self.fm.b_schedule.AddShapeObject(self.s2)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.merged_schedule.GetShapeList()), 2)
    self.assertEquals(self.s1, self.fm.a_merge_map[self.s1])
    self.assertEquals(self.s2, self.fm.b_merge_map[self.s2])
    self.assertEquals(self.sm.GetMergeStats(), (0, 1, 1))

    # check that the ids are preserved
    self.assertEquals(self.fm.a_merge_map[self.s1].shape_id, self.s1.shape_id)
    self.assertEquals(self.fm.b_merge_map[self.s2].shape_id, self.s2.shape_id)

  def testNoMerge_FarEndpoints(self):
    self.s3.shape_id = self.s1.shape_id
    self.fm.a_schedule.AddShapeObject(self.s1)
    self.fm.b_schedule.AddShapeObject(self.s3)
    self.fm.problem_reporter.ExpectProblemClass(merge.SameIdButNotMerged)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.merged_schedule.GetShapeList()), 2)
    self.assertEquals(self.s1, self.fm.a_merge_map[self.s1])
    self.assertEquals(self.s3, self.fm.b_merge_map[self.s3])
    self.assertEquals(self.sm.GetMergeStats(), (0, 1, 1))

    # check that the ids are different
    self.assertNotEquals(self.fm.a_merge_map[self.s1].shape_id,
                         self.fm.b_merge_map[self.s3].shape_id)

    self.fm.problem_reporter.assertExpectedProblemsReported(self)

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
        self.s1.points[0][:2], self.s3.points[0][:2])
    distance2 = merge.ApproximateDistanceBetweenPoints(
        self.s1.points[-1][:2], self.s3.points[-1][:2])
    return max(distance1, distance2)

  def testSetLargestShapeDistanceSmall(self):
    largest_shape_distance = self._AddShapesApart() * 0.5
    self.sm.SetLargestShapeDistance(largest_shape_distance)
    self.assertEquals(self.sm.largest_shape_distance, largest_shape_distance)
    self.fm.problem_reporter.ExpectProblemClass(merge.SameIdButNotMerged)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.GetMergedSchedule().GetShapeList()), 2)
    self.fm.problem_reporter.assertExpectedProblemsReported(self)

  def testSetLargestShapeDistanceLarge(self):
    largest_shape_distance = self._AddShapesApart() * 2.0
    self.sm.SetLargestShapeDistance(largest_shape_distance)
    self.assertEquals(self.sm.largest_shape_distance, largest_shape_distance)
    self.fm.MergeSchedules()
    self.assertEquals(len(self.fm.GetMergedSchedule().GetShapeList()), 1)


class TestFareRuleMerger(util.TestCase):

  def setUp(self):
    a_schedule = transitfeed.Schedule()
    b_schedule = transitfeed.Schedule()
    merged_schedule = transitfeed.Schedule()
    self.fm = merge.FeedMerger(a_schedule, b_schedule, merged_schedule,
                               TestingProblemReporter())
    self.fm.AddDefaultMergers()
    self.fare_rule_merger = self.fm.GetMerger(merge.FareRuleMerger)

    akwargs = {'id': 'a1',
               'agency_name': 'a1',
               'agency_url': 'http://www.a1.com',
               'agency_timezone': 'Europe/Zurich'}
    self.a1 = transitfeed.Agency(**akwargs)
    self.a2 = transitfeed.Agency(**akwargs)

    rkwargs = {'route_id': 'r1',
               'agency_id': 'a1',
               'short_name': 'r1',
               'long_name': 'r1r1',
               'route_type': '0'}
    self.r1 = transitfeed.Route(**rkwargs)
    self.r2 = transitfeed.Route(**rkwargs)

    self.f1 = transitfeed.Fare('f1', '10', 'ZAR', '1', '0')
    self.f2 = transitfeed.Fare('f1', '10', 'ZAR', '1', '0')
    self.f3 = transitfeed.Fare('f3', '11', 'USD', '1', '0')

    self.fr1 = transitfeed.FareRule('f1', 'r1')
    self.fr2 = transitfeed.FareRule('f1', 'r1')
    self.fr3 = transitfeed.FareRule('f3', 'r1')

    self.fm.a_schedule.AddAgencyObject(self.a1)
    self.fm.a_schedule.AddRouteObject(self.r1)
    self.fm.a_schedule.AddFareObject(self.f1)
    self.fm.a_schedule.AddFareObject(self.f3)
    self.fm.a_schedule.AddFareRuleObject(self.fr1)
    self.fm.a_schedule.AddFareRuleObject(self.fr3)

    self.fm.b_schedule.AddAgencyObject(self.a2)
    self.fm.b_schedule.AddRouteObject(self.r2)
    self.fm.b_schedule.AddFareObject(self.f2)
    self.fm.b_schedule.AddFareRuleObject(self.fr2)

  def testMerge(self):
    self.fm.problem_reporter.ExpectProblemClass(merge.FareRulesBroken)
    self.fm.problem_reporter.ExpectProblemClass(merge.MergeNotImplemented)
    self.fm.MergeSchedules()

    self.assertEquals(len(self.fm.merged_schedule.GetFareList()), 2)

    fare_1 = self.fm.a_merge_map[self.f1]
    fare_2 = self.fm.a_merge_map[self.f3]

    self.assertEquals(len(fare_1.GetFareRuleList()), 1)
    fare_rule_1 = fare_1.GetFareRuleList()[0]
    self.assertEquals(len(fare_2.GetFareRuleList()), 1)
    fare_rule_2 = fare_2.GetFareRuleList()[0]

    self.assertEquals(fare_rule_1.fare_id,
                      self.fm.a_merge_map[self.f1].fare_id)
    self.assertEquals(fare_rule_1.route_id,
                      self.fm.a_merge_map[self.r1].route_id)
    self.assertEqual(fare_rule_2.fare_id,
                     self.fm.a_merge_map[self.f3].fare_id)
    self.assertEqual(fare_rule_2.route_id,
                     self.fm.a_merge_map[self.r1].route_id)

    self.fm.problem_reporter.assertExpectedProblemsReported(self)

  def testMergeStats(self):
    self.assert_(self.fare_rule_merger.GetMergeStats() is None)


class TestTransferMerger(util.TestCase):
  def setUp(self):
    a_schedule = transitfeed.Schedule()
    b_schedule = transitfeed.Schedule()
    merged_schedule = transitfeed.Schedule()
    self.fm = merge.FeedMerger(a_schedule, b_schedule, merged_schedule,
                               TestingProblemReporter())

  def testStopsMerged(self):
    stop0 = transitfeed.Stop(lat=30.0, lng=30.0, name="0", stop_id="0")
    stop1 = transitfeed.Stop(lat=30.1, lng=30.1, name="1", stop_id="1")
    self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))
    self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))

    self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1))
    self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1))
    self.fm.a_schedule.AddTransferObject(transitfeed.Transfer(from_stop_id="0",
                                                              to_stop_id="1"))
    self.fm.b_schedule.AddTransferObject(transitfeed.Transfer(from_stop_id="0",
                                                              to_stop_id="1"))
    self.fm.AddMerger(merge.StopMerger(self.fm))
    self.fm.AddMerger(merge.TransferMerger(self.fm))
    self.fm.MergeSchedules()
    transfers = self.fm.merged_schedule.GetTransferList()
    self.assertEquals(1, len(transfers))
    self.assertEquals("0", transfers[0].from_stop_id)
    self.assertEquals("1", transfers[0].to_stop_id)

  def testToStopNotMerged(self):
    """When stops aren't merged transfer is duplicated."""
    self.fm.problem_reporter.ExpectProblemClass(merge.SameIdButNotMerged)
    stop0 = transitfeed.Stop(lat=30.0, lng=30.0, name="0", stop_id="0")
    stop1a = transitfeed.Stop(lat=30.1, lng=30.1, name="1a", stop_id="1")
    stop1b = transitfeed.Stop(lat=30.1, lng=30.1, name="1b", stop_id="1")

    # a_schedule and b_schedule both have a transfer with to_stop_id=1 but the
    # stops are not merged so the transfer must be duplicated. Create a copy
    # of the Stop objects to add to the schedules.
    self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))
    self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1a))
    self.fm.a_schedule.AddTransferObject(
        transitfeed.Transfer(from_stop_id="0", to_stop_id="1"))
    self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))
    self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1b))
    self.fm.b_schedule.AddTransferObject(
        transitfeed.Transfer(from_stop_id="0", to_stop_id="1"))
    self.fm.AddMerger(merge.StopMerger(self.fm))
    self.fm.AddMerger(merge.TransferMerger(self.fm))
    self.fm.MergeSchedules()

    transfers = self.fm.merged_schedule.GetTransferList()
    self.assertEquals(2, len(transfers))
    self.assertEquals("0", transfers[0].from_stop_id)
    self.assertEquals("0", transfers[1].from_stop_id)
    # transfers are not ordered so allow the migrated to_stop_id values to
    # appear in either order.
    def MergedScheduleStopName(stop_id):
      return self.fm.merged_schedule.GetStop(stop_id).stop_name
    if MergedScheduleStopName(transfers[0].to_stop_id) == "1a":
      self.assertEquals("1b", MergedScheduleStopName(transfers[1].to_stop_id))
    else:
      self.assertEquals("1b", MergedScheduleStopName(transfers[0].to_stop_id))
      self.assertEquals("1a", MergedScheduleStopName(transfers[1].to_stop_id))

  def testFromStopNotMerged(self):
    """When stops aren't merged transfer is duplicated."""
    self.fm.problem_reporter.ExpectProblemClass(merge.SameIdButNotMerged)
    stop0 = transitfeed.Stop(lat=30.0, lng=30.0, name="0", stop_id="0")
    stop1a = transitfeed.Stop(lat=30.1, lng=30.1, name="1a", stop_id="1")
    stop1b = transitfeed.Stop(lat=30.1, lng=30.1, name="1b", stop_id="1")

    # a_schedule and b_schedule both have a transfer with from_stop_id=1 but the
    # stops are not merged so the transfer must be duplicated. Create a copy
    # of the Stop objects to add to the schedules.
    self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))
    self.fm.a_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1a))
    self.fm.a_schedule.AddTransferObject(
        transitfeed.Transfer(from_stop_id="1", to_stop_id="0"))
    self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop0))
    self.fm.b_schedule.AddStopObject(transitfeed.Stop(field_dict=stop1b))
    self.fm.b_schedule.AddTransferObject(
        transitfeed.Transfer(from_stop_id="1", to_stop_id="0"))
    self.fm.AddMerger(merge.StopMerger(self.fm))
    self.fm.AddMerger(merge.TransferMerger(self.fm))
    self.fm.MergeSchedules()

    transfers = self.fm.merged_schedule.GetTransferList()
    self.assertEquals(2, len(transfers))
    self.assertEquals("0", transfers[0].to_stop_id)
    self.assertEquals("0", transfers[1].to_stop_id)
    # transfers are not ordered so allow the migrated from_stop_id values to
    # appear in either order.
    def MergedScheduleStopName(stop_id):
      return self.fm.merged_schedule.GetStop(stop_id).stop_name
    if MergedScheduleStopName(transfers[0].from_stop_id) == "1a":
      self.assertEquals("1b", MergedScheduleStopName(transfers[1].from_stop_id))
    else:
      self.assertEquals("1b", MergedScheduleStopName(transfers[0].from_stop_id))
      self.assertEquals("1a", MergedScheduleStopName(transfers[1].from_stop_id))


class TestExceptionProblemReporter(util.TestCase):

  def setUp(self):
    self.dataset_merger = merge.TripMerger(None)

  def testRaisesErrors(self):
    problem_reporter = merge.ExceptionProblemReporter()
    self.assertRaises(merge.CalendarsNotDisjoint,
                      problem_reporter.CalendarsNotDisjoint,
                      self.dataset_merger)

  def testNoRaiseWarnings(self):
    problem_reporter = merge.ExceptionProblemReporter()
    problem_reporter.MergeNotImplemented(self.dataset_merger)

  def testRaiseWarnings(self):
    problem_reporter = merge.ExceptionProblemReporter(True)
    self.assertRaises(merge.MergeNotImplemented,
                      problem_reporter.MergeNotImplemented,
                      self.dataset_merger)


class TestHTMLProblemReporter(util.TestCase):

  def setUp(self):
    self.problem_reporter = merge.HTMLProblemReporter()
    a_schedule = transitfeed.Schedule()
    b_schedule = transitfeed.Schedule()
    merged_schedule = transitfeed.Schedule()
    self.feed_merger = merge.FeedMerger(a_schedule, b_schedule,
                                        merged_schedule,
                                        self.problem_reporter)
    self.dataset_merger = merge.TripMerger(None)

  def testGeneratesSomeHTML(self):
    self.problem_reporter.CalendarsNotDisjoint(self.dataset_merger)
    self.problem_reporter.MergeNotImplemented(self.dataset_merger)
    self.problem_reporter.FareRulesBroken(self.dataset_merger)
    self.problem_reporter.SameIdButNotMerged(self.dataset_merger,
                                             'test', 'unknown reason')

    output_file = StringIO.StringIO()
    old_feed_path = '/path/to/old/feed'
    new_feed_path = '/path/to/new/feed'
    merged_feed_path = '/path/to/merged/feed'
    self.problem_reporter.WriteOutput(output_file, self.feed_merger,
                                      old_feed_path, new_feed_path,
                                      merged_feed_path)

    html = output_file.getvalue()
    self.assert_(html.startswith('<html>'))
    self.assert_(html.endswith('</html>'))


class MergeInSubprocessTestCase(util.TempDirTestCaseBase):
  def CopyAndModifyTestData(self, zip_path, modify_file, old, new):
    """Return path of zip_path copy with old replaced by new in modify_file."""
    zipfile_mem = StringIO.StringIO(open(zip_path, 'rb').read())
    new_zip_path = os.path.join(self.tempdirpath, "modified.zip")
    zip = zipfile.ZipFile(zipfile_mem, 'a')
    modified_contents = zip.read(modify_file).replace(old, new)
    zip.writestr(modify_file, modified_contents)
    zip.close()
    open(new_zip_path, 'wb').write(zipfile_mem.getvalue())
    return new_zip_path

  def testCrashHandler(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('merge.py'), '--no_browser',
         'IWantMyCrash', 'file2', 'fileout.zip'],
        expected_retcode=127)
    self.assertMatchesRegex(r'Yikes', out)
    crashout = open('transitfeedcrash.txt').read()
    self.assertMatchesRegex(r'For testing the merge crash handler', crashout)

  def testMergeBadCommandLine(self):
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('merge.py'), '--no_browser'],
        expected_retcode=2)
    self.assertFalse(out)
    self.assertMatchesRegex(r'command line arguments', err)
    self.assertFalse(os.path.exists('transitfeedcrash.txt'))

  def testMergeWithWarnings(self):
    # Make a copy of good_feed.zip which is not active until 20110101. This
    # avoids adding another test/data file. good_feed.zip needs to remain error
    # free so it can't start in the future.
    future_good_feed = self.CopyAndModifyTestData(
        self.GetPath('test/data/good_feed.zip'), 'calendar.txt',
        '20070101', '20110101')
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('merge.py'), '--no_browser',
         self.GetPath('test/data/unused_stop'),
         future_good_feed,
         os.path.join(self.tempdirpath, 'merged-warnings.zip')],
        expected_retcode=0)

  def testMergeWithErrors(self):
    # Make a copy of good_feed.zip which is not active until 20110101. This
    # avoids adding another test/data file. good_feed.zip needs to remain error
    # free so it can't start in the future.
    future_good_feed = self.CopyAndModifyTestData(
        self.GetPath('test/data/good_feed.zip'), 'calendar.txt',
        '20070101', '20110101')
    (out, err) = self.CheckCallWithPath(
        [self.GetPath('merge.py'), '--no_browser',
         self.GetPath('test/data/unused_stop'),
         future_good_feed],
        expected_retcode=2)


if __name__ == '__main__':
  unittest.main()
