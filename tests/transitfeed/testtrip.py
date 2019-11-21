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

# Unit tests for the trip module.
from __future__ import absolute_import

from StringIO import StringIO
from tests import util
import transitfeed


class DuplicateStopSequenceTestCase(util.TestCase):
  def runTest(self):
    accumulator = util.RecordingProblemAccumulator(
      self, ("ExpirationDate", "NoServiceExceptions"))
    problems = transitfeed.ProblemReporter(accumulator)
    schedule = transitfeed.Schedule(problem_reporter=problems)
    schedule.Load(util.DataPath('duplicate_stop_sequence'),
                  extra_validation=True)
    e = accumulator.PopException('InvalidValue')
    self.assertEqual('stop_sequence', e.column_name)
    self.assertEqual(10, e.value)
    accumulator.AssertNoMoreExceptions()


class MissingEndpointTimesTestCase(util.TestCase):
  def runTest(self):
    accumulator = util.RecordingProblemAccumulator(
      self, ('ExpirationDate', 'NoServiceExceptions'))
    problems = transitfeed.ProblemReporter(accumulator)
    schedule = transitfeed.Schedule(problem_reporter=problems)
    schedule.Load(util.DataPath('missing_endpoint_times'),
                  extra_validation=True)
    e = accumulator.PopInvalidValue('arrival_time')
    self.assertEqual('', e.value)
    e = accumulator.PopInvalidValue('departure_time')
    self.assertEqual('', e.value)


class TripMemoryZipTestCase(util.MemoryZipTestCase):
  def assertLoadAndCheckExtraValues(self, schedule_file):
    """Load file-like schedule_file and check for extra trip columns."""
    load_problems = util.GetTestFailureProblemReporter(
        self, ("ExpirationDate", "UnrecognizedColumn"))
    loaded_schedule = transitfeed.Loader(schedule_file,
                                         problems=load_problems,
                                         extra_validation=True).Load()
    self.assertEqual("foo", loaded_schedule.GetTrip("AB1")["t_foo"])
    self.assertEqual("", loaded_schedule.GetTrip("AB2")["t_foo"])
    self.assertEqual("", loaded_schedule.GetTrip("AB1")["n_foo"])
    self.assertEqual("bar", loaded_schedule.GetTrip("AB2")["n_foo"])
    # Uncomment the following lines to print the string in testExtraFileColumn
    # print repr(zipfile.ZipFile(schedule_file).read("trips.txt"))
    # self.fail()

  def testExtraObjectAttribute(self):
    """Extra columns added to an object are preserved when writing."""
    schedule = self.MakeLoaderAndLoad()
    # Add an attribute to an existing trip
    trip1 = schedule.GetTrip("AB1")
    trip1.t_foo = "foo"
    # Make a copy of trip_id=AB1 and add an attribute before AddTripObject
    trip2 = transitfeed.Trip(field_dict=trip1)
    trip2.trip_id = "AB2"
    trip2.t_foo = ""
    trip2.n_foo = "bar"
    schedule.AddTripObject(trip2)
    trip2.AddStopTime(stop=schedule.GetStop("BULLFROG"), stop_time="09:00:00")
    trip2.AddStopTime(stop=schedule.GetStop("STAGECOACH"), stop_time="09:30:00")
    saved_schedule_file = StringIO()
    schedule.WriteGoogleTransitFeed(saved_schedule_file)
    self.accumulator.AssertNoMoreExceptions()

    self.assertLoadAndCheckExtraValues(saved_schedule_file)

  def testExtraFileColumn(self):
    """Extra columns loaded from a file are preserved when writing."""
    # Uncomment the code in assertLoadAndCheckExtraValues to generate this
    # string.
    self.SetArchiveContents(
        "trips.txt",
        "route_id,service_id,trip_id,t_foo,n_foo\n"
        "AB,FULLW,AB1,foo,\n"
        "AB,FULLW,AB2,,bar\n")
    self.AppendToArchiveContents(
        "stop_times.txt",
        "AB2,09:00:00,09:00:00,BULLFROG,1\n"
        "AB2,09:30:00,09:30:00,STAGECOACH,2\n")
    load1_problems = util.GetTestFailureProblemReporter(
        self, ("ExpirationDate", "UnrecognizedColumn"))
    schedule = self.MakeLoaderAndLoad(problems=load1_problems)
    saved_schedule_file = StringIO()
    schedule.WriteGoogleTransitFeed(saved_schedule_file)

    self.assertLoadAndCheckExtraValues(saved_schedule_file)


class TripValidationTestCase(util.ValidationTestCase):
  def runTest(self):
    trip = transitfeed.Trip()
    repr(trip)  # shouldn't crash

    schedule = self.SimpleSchedule()
    trip = transitfeed.Trip()
    repr(trip)  # shouldn't crash

    trip = transitfeed.Trip()
    trip.trip_headsign = '\xBA\xDF\x0D'  # Not valid ascii or utf8
    repr(trip)  # shouldn't crash

    trip.route_id = '054C'
    trip.service_id = 'WEEK'
    trip.trip_id = '054C-00'
    trip.trip_headsign = 'via Polish Hill'
    trip.trip_short_name = 'X12'
    trip.direction_id = '0'
    trip.block_id = None
    trip.shape_id = None
    trip.bikes_allowed = '1'
    trip.wheelchair_accessible = '2'
    trip.Validate(self.problems)
    self.accumulator.AssertNoMoreExceptions()
    repr(trip)  # shouldn't crash

    # missing route ID
    trip.route_id = None
    self.ValidateAndExpectMissingValue(trip, 'route_id')
    trip.route_id = '054C'

    # missing service ID
    trip.service_id = None
    self.ValidateAndExpectMissingValue(trip, 'service_id')
    trip.service_id = 'WEEK'

    # missing trip ID
    trip.trip_id = None
    self.ValidateAndExpectMissingValue(trip, 'trip_id')
    trip.trip_id = '054C-00'

    # invalid direction ID
    trip.direction_id = 'NORTH'
    self.ValidateAndExpectInvalidValue(trip, 'direction_id')
    trip.direction_id = '0'

    # invalid bikes_allowed
    trip.bikes_allowed = '3'
    self.ValidateAndExpectInvalidValue(trip, 'bikes_allowed')
    trip.bikes_allowed = None

    # invalid wheelchair_accessible
    trip.wheelchair_accessible = '3'
    self.ValidateAndExpectInvalidValue(trip, 'wheelchair_accessible')
    trip.wheelchair_accessible = None

    # AddTripObject validates that route_id, service_id, .... are found in the
    # schedule. The Validate calls made by self.Expect... above can't make this
    # check because trip is not in a schedule.
    trip.route_id = '054C-notfound'
    schedule.AddTripObject(trip, self.problems, True)
    e = self.accumulator.PopException('InvalidValue')
    self.assertEqual('route_id', e.column_name)
    self.accumulator.AssertNoMoreExceptions()
    trip.route_id = '054C'

    # Make sure calling Trip.Validate validates that route_id and service_id
    # are found in the schedule.
    trip.service_id = 'WEEK-notfound'
    trip.Validate(self.problems)
    e = self.accumulator.PopException('InvalidValue')
    self.assertEqual('service_id', e.column_name)
    self.accumulator.AssertNoMoreExceptions()
    trip.service_id = 'WEEK'

    trip.Validate(self.problems)
    self.accumulator.AssertNoMoreExceptions()

    # expect no problems for non-overlapping periods
    trip.AddFrequency("06:00:00", "12:00:00", 600)
    trip.AddFrequency("01:00:00", "02:00:00", 1200)
    trip.AddFrequency("04:00:00", "05:00:00", 1000)
    trip.AddFrequency("12:00:00", "19:00:00", 700)
    trip.Validate(self.problems)
    self.accumulator.AssertNoMoreExceptions()
    trip.ClearFrequencies()

    # overlapping headway periods
    trip.AddFrequency("00:00:00", "12:00:00", 600)
    trip.AddFrequency("06:00:00", "18:00:00", 1200)
    self.ValidateAndExpectOtherProblem(trip)
    trip.ClearFrequencies()
    trip.AddFrequency("12:00:00", "20:00:00", 600)
    trip.AddFrequency("06:00:00", "18:00:00", 1200)
    self.ValidateAndExpectOtherProblem(trip)
    trip.ClearFrequencies()
    trip.AddFrequency("06:00:00", "12:00:00", 600)
    trip.AddFrequency("00:00:00", "25:00:00", 1200)
    self.ValidateAndExpectOtherProblem(trip)
    trip.ClearFrequencies()
    trip.AddFrequency("00:00:00", "20:00:00", 600)
    trip.AddFrequency("06:00:00", "18:00:00", 1200)
    self.ValidateAndExpectOtherProblem(trip)
    trip.ClearFrequencies()
    self.accumulator.AssertNoMoreExceptions()


class TripSequenceValidationTestCase(util.ValidationTestCase):
  def runTest(self):
    schedule = self.SimpleSchedule()
    # Make a new trip without any stop times
    trip = schedule.GetRoute("054C").AddTrip(trip_id="054C-00")
    stop1 = schedule.GetStop('stop1')
    stop2 = schedule.GetStop('stop2')
    stop3 = schedule.GetStop('stop3')
    stoptime1 = transitfeed.StopTime(self.problems, stop1,
                                     stop_time='12:00:00', stop_sequence=1)
    stoptime2 = transitfeed.StopTime(self.problems, stop2,
                                     stop_time='11:30:00', stop_sequence=2)
    stoptime3 = transitfeed.StopTime(self.problems, stop3,
                                     stop_time='12:15:00', stop_sequence=3)
    trip._AddStopTimeObjectUnordered(stoptime1, schedule)
    trip._AddStopTimeObjectUnordered(stoptime2, schedule)
    trip._AddStopTimeObjectUnordered(stoptime3, schedule)
    trip.Validate(self.problems)
    e = self.accumulator.PopException('OtherProblem')
    self.assertTrue(e.FormatProblem().find('Timetravel detected') != -1)
    self.assertTrue(e.FormatProblem().find('number 2 in trip 054C-00') != -1)
    self.accumulator.AssertNoMoreExceptions()


class TripServiceIDValidationTestCase(util.ValidationTestCase):
  def runTest(self):
    schedule = self.SimpleSchedule()
    trip1 = transitfeed.Trip()
    trip1.route_id = "054C"
    trip1.service_id = "WEEKDAY"
    trip1.trip_id = "054C_WEEK"
    self.ExpectInvalidValueInClosure(column_name="service_id",
                                     value="WEEKDAY",
                                     c=lambda: schedule.AddTripObject(trip1,
                                                            validate=True))


class TripDistanceFromStopToShapeValidationTestCase(util.ValidationTestCase):
  def runTest(self):
    schedule = self.SimpleSchedule()
    stop1 = schedule.stops["stop1"]
    stop2 = schedule.stops["stop2"]
    stop3 = schedule.stops["stop3"]

    # Set shape_dist_traveled
    trip = schedule.trips["CITY1"]
    trip.ClearStopTimes()
    trip.AddStopTime(stop1, stop_time="12:00:00", shape_dist_traveled=0)
    trip.AddStopTime(stop2, stop_time="12:00:45", shape_dist_traveled=500)
    trip.AddStopTime(stop3, stop_time="12:02:30", shape_dist_traveled=1500)
    trip.shape_id = "shape1"

    # Add a valid shape for the trip to the current schedule.
    shape = transitfeed.Shape("shape1")
    shape.AddPoint(48.2, 1.00, 0)
    shape.AddPoint(48.2, 1.01, 500)
    shape.AddPoint(48.2, 1.03, 1500)
    shape.max_distance = 1500
    schedule.AddShapeObject(shape)

    # The schedule should validate with no problems.
    self.ExpectNoProblems(schedule)

    # Delete a stop latitude. This should not crash validation.
    stop1.stop_lat = None
    self.ValidateAndExpectMissingValue(schedule, "stop_lat")


class TripHasStopTimeValidationTestCase(util.ValidationTestCase):
  def runTest(self):
    schedule = self.SimpleSchedule()
    trip = schedule.GetRoute("054C").AddTrip(trip_id="054C-00")

    # We should get an OtherProblem here because the trip has no stops.
    self.ValidateAndExpectOtherProblem(schedule)

    # It should trigger a TYPE_ERROR if there are frequencies for the trip
    # but no stops
    trip.AddFrequency("01:00:00", "12:00:00", 600)
    schedule.Validate(self.problems)
    self.accumulator.PopException('OtherProblem')  # pop first warning
    e = self.accumulator.PopException('OtherProblem')  # pop frequency error
    self.assertTrue(e.FormatProblem().find('Frequencies defined, but') != -1)
    self.assertTrue(e.FormatProblem().find('given in trip 054C-00') != -1)
    self.assertEquals(transitfeed.TYPE_ERROR, e.type)
    self.accumulator.AssertNoMoreExceptions()
    trip.ClearFrequencies()

    # Add a stop, but with only one stop passengers have nowhere to exit!
    stop = transitfeed.Stop(36.425288, -117.133162, "Demo Stop 1", "STOP1")
    schedule.AddStopObject(stop)
    trip.AddStopTime(stop, arrival_time="5:11:00", departure_time="5:12:00")
    self.ValidateAndExpectOtherProblem(schedule)

    # Add another stop, and then validation should be happy.
    stop = transitfeed.Stop(36.424288, -117.133142, "Demo Stop 2", "STOP2")
    schedule.AddStopObject(stop)
    trip.AddStopTime(stop, arrival_time="5:15:00", departure_time="5:16:00")
    schedule.Validate(self.problems)

    trip.AddStopTime(stop, stop_time="05:20:00")
    trip.AddStopTime(stop, stop_time="05:22:00")

    # Last stop must always have a time
    trip.AddStopTime(stop, arrival_secs=None, departure_secs=None)
    self.ExpectInvalidValueInClosure(
        'arrival_time', c=lambda: trip.GetEndTime(problems=self.problems))


class ShapeDistTraveledOfStopTimeValidationTestCase(util.ValidationTestCase):
  def runTest(self):
    schedule = self.SimpleSchedule()

    shape = transitfeed.Shape("shape_1")
    shape.AddPoint(36.425288, -117.133162, 0)
    shape.AddPoint(36.424288, -117.133142, 1)
    schedule.AddShapeObject(shape)

    trip = schedule.GetRoute("054C").AddTrip(trip_id="054C-00")
    trip.shape_id = "shape_1"

    stop = transitfeed.Stop(36.425288, -117.133162, "Demo Stop 1", "STOP1")
    schedule.AddStopObject(stop)
    trip.AddStopTime(stop, arrival_time="5:11:00", departure_time="5:12:00",
                     stop_sequence=0, shape_dist_traveled=0)
    stop = transitfeed.Stop(36.424288, -117.133142, "Demo Stop 2", "STOP2")
    schedule.AddStopObject(stop)
    trip.AddStopTime(stop, arrival_time="5:15:00", departure_time="5:16:00",
                     stop_sequence=1, shape_dist_traveled=1)

    stop = transitfeed.Stop(36.423288, -117.133122, "Demo Stop 3", "STOP3")
    schedule.AddStopObject(stop)
    trip.AddStopTime(stop, arrival_time="5:18:00", departure_time="5:19:00",
                     stop_sequence=2, shape_dist_traveled=2)
    self.accumulator.AssertNoMoreExceptions()
    schedule.Validate(self.problems)
    e = self.accumulator.PopException('OtherProblem')
    self.assertMatchesRegex('shape_dist_traveled=2', e.FormatProblem())
    self.accumulator.AssertNoMoreExceptions()

    # Error if the distance decreases.
    shape.AddPoint(36.421288, -117.133132, 2)
    stop = transitfeed.Stop(36.421288, -117.133122, "Demo Stop 4", "STOP4")
    schedule.AddStopObject(stop)
    stoptime = transitfeed.StopTime(self.problems, stop,
                                    arrival_time="5:29:00",
                                    departure_time="5:29:00", stop_sequence=3,
                                    shape_dist_traveled=1.7)
    trip.AddStopTimeObject(stoptime, schedule=schedule)
    self.accumulator.AssertNoMoreExceptions()
    schedule.Validate(self.problems)
    e = self.accumulator.PopException('InvalidValue')
    self.assertMatchesRegex('stop STOP4 has', e.FormatProblem())
    self.assertMatchesRegex('shape_dist_traveled=1.7', e.FormatProblem())
    self.assertMatchesRegex('distance was 2.0.', e.FormatProblem())
    self.assertEqual(e.type, transitfeed.TYPE_ERROR)
    self.accumulator.AssertNoMoreExceptions()

    # Warning if distance remains the same between two stop_times
    stoptime.shape_dist_traveled = 2.0
    trip.ReplaceStopTimeObject(stoptime, schedule=schedule)
    schedule.Validate(self.problems)
    e = self.accumulator.PopException('InvalidValue')
    self.assertMatchesRegex('stop STOP4 has', e.FormatProblem())
    self.assertMatchesRegex('shape_dist_traveled=2.0', e.FormatProblem())
    self.assertMatchesRegex('distance was 2.0.', e.FormatProblem())
    self.assertEqual(e.type, transitfeed.TYPE_WARNING)
    self.accumulator.AssertNoMoreExceptions()


class StopMatchWithShapeTestCase(util.ValidationTestCase):
  def runTest(self):
    schedule = self.SimpleSchedule()

    shape = transitfeed.Shape("shape_1")
    shape.AddPoint(36.425288, -117.133162, 0)
    shape.AddPoint(36.424288, -117.143142, 1)
    schedule.AddShapeObject(shape)

    trip = schedule.GetRoute("054C").AddTrip(trip_id="054C-00")
    trip.shape_id = "shape_1"

    # Stop 1 is only 600 meters away from shape, which is allowed.
    stop = transitfeed.Stop(36.425288, -117.139162, "Demo Stop 1", "STOP1")
    schedule.AddStopObject(stop)
    trip.AddStopTime(stop, arrival_time="5:11:00", departure_time="5:12:00",
                     stop_sequence=0, shape_dist_traveled=0)
    # Stop 2 is more than 1000 meters away from shape, which is not allowed.
    stop = transitfeed.Stop(36.424288, -117.158142, "Demo Stop 2", "STOP2")
    schedule.AddStopObject(stop)
    trip.AddStopTime(stop, arrival_time="5:15:00", departure_time="5:16:00",
                     stop_sequence=1, shape_dist_traveled=1)

    schedule.Validate(self.problems)
    e = self.accumulator.PopException('StopTooFarFromShapeWithDistTraveled')
    self.assertTrue(e.FormatProblem().find('Demo Stop 2') != -1)
    self.assertTrue(e.FormatProblem().find('1344 meters away') != -1)
    self.accumulator.AssertNoMoreExceptions()


class TripAddStopTimeObjectTestCase(util.ValidationTestCase):
  def runTest(self):
    schedule = transitfeed.Schedule(problem_reporter=self.problems)
    schedule.AddAgency("\xc8\x8b Fly Agency", "http://iflyagency.com",
                       "America/Los_Angeles")
    service_period = schedule.GetDefaultServicePeriod().SetDateHasService('20070101')
    stop1 = schedule.AddStop(lng=140, lat=48.2, name="Stop 1")
    stop2 = schedule.AddStop(lng=140.001, lat=48.201, name="Stop 2")
    route = schedule.AddRoute("B", "Beta", "Bus")
    trip = route.AddTrip(schedule, "bus trip")
    trip.AddStopTimeObject(transitfeed.StopTime(self.problems, stop1,
                                                arrival_secs=10,
                                                departure_secs=10),
                           schedule=schedule, problems=self.problems)
    trip.AddStopTimeObject(transitfeed.StopTime(self.problems, stop2,
                                                arrival_secs=20,
                                                departure_secs=20),
                           schedule=schedule, problems=self.problems)
    # TODO: Factor out checks or use mock problems object
    self.ExpectOtherProblemInClosure(lambda:
      trip.AddStopTimeObject(transitfeed.StopTime(self.problems, stop1,
                                                  arrival_secs=15,
                                                  departure_secs=15),
                             schedule=schedule, problems=self.problems))
    trip.AddStopTimeObject(transitfeed.StopTime(self.problems, stop1),
                           schedule=schedule, problems=self.problems)
    self.ExpectOtherProblemInClosure(lambda:
        trip.AddStopTimeObject(transitfeed.StopTime(self.problems, stop1,
                                                    arrival_secs=15,
                                                    departure_secs=15),
                               schedule=schedule, problems=self.problems))
    trip.AddStopTimeObject(transitfeed.StopTime(self.problems, stop1,
                                                arrival_secs=30,
                                                departure_secs=30),
                           schedule=schedule, problems=self.problems)
    self.accumulator.AssertNoMoreExceptions()


class TripReplaceStopTimeObjectTestCase(util.TestCase):
  def runTest(self):
    schedule = transitfeed.Schedule()
    schedule.AddAgency("\xc8\x8b Fly Agency", "http://iflyagency.com",
                       "America/Los_Angeles")
    service_period = \
      schedule.GetDefaultServicePeriod().SetDateHasService('20070101')
    stop1 = schedule.AddStop(lng=140, lat=48.2, name="Stop 1")
    route = schedule.AddRoute("B", "Beta", "Bus")
    trip = route.AddTrip(schedule, "bus trip")
    stoptime = transitfeed.StopTime(transitfeed.default_problem_reporter, stop1,
                                    arrival_secs=10,
                                    departure_secs=10)
    trip.AddStopTimeObject(stoptime, schedule=schedule)
    stoptimes = trip.GetStopTimes()
    stoptime.departure_secs = 20
    trip.ReplaceStopTimeObject(stoptime, schedule=schedule)
    stoptimes = trip.GetStopTimes()
    self.assertEqual(len(stoptimes), 1)
    self.assertEqual(stoptimes[0].departure_secs, 20)

    unknown_stop = schedule.AddStop(lng=140, lat=48.2, name="unknown")
    unknown_stoptime = transitfeed.StopTime(
        transitfeed.default_problem_reporter, unknown_stop,
        arrival_secs=10,
        departure_secs=10)
    unknown_stoptime.stop_sequence = 5
    # Attempting to replace a non-existent StopTime raises an error
    self.assertRaises(transitfeed.Error, trip.ReplaceStopTimeObject,
        unknown_stoptime, schedule=schedule)


class SingleTripTestCase(util.TestCase):
  def setUp(self):
    schedule = transitfeed.Schedule(
        problem_reporter=util.ExceptionProblemReporterNoExpiration())
    schedule.NewDefaultAgency(agency_name="Test Agency",
                              agency_url="http://example.com",
                              agency_timezone="America/Los_Angeles")
    route = schedule.AddRoute(short_name="54C", long_name="Polish Hill",
                              route_type=3)

    service_period = schedule.GetDefaultServicePeriod()
    service_period.SetDateHasService("20070101")

    trip = route.AddTrip(schedule, 'via Polish Hill')

    stop1 = schedule.AddStop(36.425288, -117.133162, "Demo Stop 1")
    stop2 = schedule.AddStop(36.424288, -117.133142, "Demo Stop 2")

    self.schedule = schedule
    self.trip = trip
    self.stop1 = stop1
    self.stop2 = stop2


class TripStopTimeAccessorsTestCase(SingleTripTestCase):
  def runTest(self):
    self.trip.AddStopTime(
        self.stop1, arrival_time="5:11:00", departure_time="5:12:00")
    self.trip.AddStopTime(
        self.stop2, arrival_time="5:15:00", departure_time="5:16:00")

    # Add some more stop times and test GetEndTime does the correct thing
    self.assertEqual(transitfeed.FormatSecondsSinceMidnight(
        self.trip.GetStartTime()), "05:11:00")
    self.assertEqual(transitfeed.FormatSecondsSinceMidnight(
        self.trip.GetEndTime()), "05:16:00")

    self.trip.AddStopTime(self.stop1, stop_time="05:20:00")
    self.assertEqual(
        transitfeed.FormatSecondsSinceMidnight(self.trip.GetEndTime()),
        "05:20:00")

    self.trip.AddStopTime(self.stop2, stop_time="05:22:00")
    self.assertEqual(
        transitfeed.FormatSecondsSinceMidnight(self.trip.GetEndTime()),
        "05:22:00")


class TripGetStopTimesTestCase(SingleTripTestCase):
  def runTest(self):
    self.trip.AddStopTime(
        self.stop1,
        arrival_time="5:11:00",
        departure_time="5:12:00",
        stop_headsign='Stop Headsign',
        pickup_type=1,
        drop_off_type=2,
        shape_dist_traveled=100,
        timepoint=1)
    self.trip.AddStopTime(
        self.stop2, arrival_time="5:15:00", departure_time="5:16:00")

    stop_times = self.trip.GetStopTimes()
    self.assertEquals(2, len(stop_times))
    st = stop_times[0]
    self.assertEquals(self.stop1.stop_id, st.stop_id)
    self.assertEquals('05:11:00', st.arrival_time)
    self.assertEquals('05:12:00', st.departure_time)
    self.assertEquals(u'Stop Headsign', st.stop_headsign)
    self.assertEquals(1, st.pickup_type)
    self.assertEquals(2, st.drop_off_type)
    self.assertEquals(100.0, st.shape_dist_traveled)
    self.assertEquals(1, st.timepoint)

    st = stop_times[1]
    self.assertEquals(self.stop2.stop_id, st.stop_id)
    self.assertEquals('05:15:00', st.arrival_time)
    self.assertEquals('05:16:00', st.departure_time)

    tuples = self.trip.GetStopTimesTuples()
    self.assertEquals(2, len(tuples))
    self.assertEqual(
        (self.trip.trip_id, "05:11:00", "05:12:00", self.stop1.stop_id,
         1, u'Stop Headsign', 1, 2, 100.0, 1),
        tuples[0])
    self.assertEqual(
        (self.trip.trip_id, "05:15:00", "05:16:00", self.stop2.stop_id,
         2, '', '', '', '', ''),
        tuples[1])


class TripClearStopTimesTestCase(util.TestCase):
  def runTest(self):
    schedule = transitfeed.Schedule(
        problem_reporter=util.ExceptionProblemReporterNoExpiration())
    schedule.NewDefaultAgency(agency_name="Test Agency",
                              agency_timezone="America/Los_Angeles")
    route = schedule.AddRoute(short_name="54C", long_name="Hill", route_type=3)
    schedule.GetDefaultServicePeriod().SetDateHasService("20070101")
    stop1 = schedule.AddStop(36, -117.1, "Demo Stop 1")
    stop2 = schedule.AddStop(36, -117.2, "Demo Stop 2")
    stop3 = schedule.AddStop(36, -117.3, "Demo Stop 3")

    trip = route.AddTrip(schedule, "via Polish Hill")
    trip.ClearStopTimes()
    self.assertFalse(trip.GetStopTimes())
    trip.AddStopTime(stop1, stop_time="5:11:00")
    self.assertTrue(trip.GetStopTimes())
    trip.ClearStopTimes()
    self.assertFalse(trip.GetStopTimes())
    trip.AddStopTime(stop3, stop_time="4:00:00")  # Can insert earlier time
    trip.AddStopTime(stop2, stop_time="4:15:00")
    trip.AddStopTime(stop1, stop_time="4:21:00")
    old_stop_times = trip.GetStopTimes()
    self.assertTrue(old_stop_times)
    trip.ClearStopTimes()
    self.assertFalse(trip.GetStopTimes())
    for st in old_stop_times:
      trip.AddStopTimeObject(st)
    self.assertEqual(trip.GetStartTime(), 4 * 3600)
    self.assertEqual(trip.GetEndTime(), 4 * 3600 + 21 * 60)


class InvalidRouteAgencyTestCase(util.LoadTestCase):
  def runTest(self):
    self.Load('invalid_route_agency')
    self.accumulator.PopInvalidValue("agency_id", "routes.txt")
    self.accumulator.PopInvalidValue("route_id", "trips.txt")
    self.accumulator.AssertNoMoreExceptions()


class InvalidAgencyIdsTestCase(util.LoadTestCase):
  def runTest(self):
    self.Load('invalid_agency_ids')
    self.accumulator.PopException('OtherProblem')
    self.accumulator.AssertNoMoreExceptions()


class AddStopTimeParametersTestCase(util.TestCase):
  def runTest(self):
    problem_reporter = util.GetTestFailureProblemReporter(self)
    schedule = transitfeed.Schedule(problem_reporter=problem_reporter)
    route = schedule.AddRoute(short_name="10", long_name="", route_type="Bus")
    stop = schedule.AddStop(40, -128, "My stop")
    # Stop must be added to schedule so that the call
    # AddStopTime -> AddStopTimeObject -> GetStopTimes -> GetStop can work
    trip = transitfeed.Trip()
    trip.route_id = route.route_id
    trip.service_id = schedule.GetDefaultServicePeriod().service_id
    trip.trip_id = "SAMPLE_TRIP"
    schedule.AddTripObject(trip)

    # First stop must have time
    trip.AddStopTime(stop, arrival_secs=300, departure_secs=360)
    trip.AddStopTime(stop)
    trip.AddStopTime(stop, arrival_time="00:07:00", departure_time="00:07:30")
    trip.Validate(problem_reporter)


class AddFrequencyValidationTestCase(util.ValidationTestCase):
  def ExpectInvalidValue(self, start_time, end_time, headway,
                         column_name, value):
    try:
      trip = transitfeed.Trip()
      trip.AddFrequency(start_time, end_time, headway)
      self.fail("Expected InvalidValue error on %s" % column_name)
    except transitfeed.InvalidValue as e:
      self.assertEqual(column_name, e.column_name)
      self.assertEqual(value, e.value)
      self.assertEqual(0, len(trip.GetFrequencyTuples()))

  def ExpectMissingValue(self, start_time, end_time, headway, column_name):
    try:
      trip = transitfeed.Trip()
      trip.AddFrequency(start_time, end_time, headway)
      self.fail("Expected MissingValue error on %s" % column_name)
    except transitfeed.MissingValue as e:
      self.assertEqual(column_name, e.column_name)
      self.assertEqual(0, len(trip.GetFrequencyTuples()))

  def runTest(self):
    # these should work fine
    trip = transitfeed.Trip()
    trip.trip_id = "SAMPLE_ID"
    trip.AddFrequency(0, 50, 1200)
    trip.AddFrequency("01:00:00", "02:00:00", "600")
    trip.AddFrequency(u"02:00:00", u"03:00:00", u"1800")
    headways = trip.GetFrequencyTuples()
    self.assertEqual(3, len(headways))
    self.assertEqual((0, 50, 1200, 0), headways[0])
    self.assertEqual((3600, 7200, 600, 0), headways[1])
    self.assertEqual((7200, 10800, 1800, 0), headways[2])
    self.assertEqual([("SAMPLE_ID", "00:00:00", "00:00:50", "1200", "0"),
                      ("SAMPLE_ID", "01:00:00", "02:00:00", "600", "0"),
                      ("SAMPLE_ID", "02:00:00", "03:00:00", "1800", "0")],
                     trip.GetFrequencyOutputTuples())

    # now test invalid input
    self.ExpectMissingValue(None, 50, 1200, "start_time")
    self.ExpectMissingValue("", 50, 1200, "start_time")
    self.ExpectInvalidValue("midnight", 50, 1200, "start_time",
                                       "midnight")
    self.ExpectInvalidValue(-50, 50, 1200, "start_time", -50)
    self.ExpectMissingValue(0, None, 1200, "end_time")
    self.ExpectMissingValue(0, "", 1200, "end_time")
    self.ExpectInvalidValue(0, "noon", 1200, "end_time", "noon")
    self.ExpectInvalidValue(0, -50, 1200, "end_time", -50)
    self.ExpectMissingValue(0, 600, 0, "headway_secs")
    self.ExpectMissingValue(0, 600, None, "headway_secs")
    self.ExpectMissingValue(0, 600, "", "headway_secs")
    self.ExpectInvalidValue(0, 600, "test", "headway_secs", "test")
    self.ExpectInvalidValue(0, 600, -60, "headway_secs", -60)
    self.ExpectInvalidValue(0, 0, 1200, "end_time", 0)
    self.ExpectInvalidValue("12:00:00", "06:00:00", 1200, "end_time",
                                       21600)


class GetTripTimeTestCase(util.TestCase):
  """Test for GetStopTimeTrips and GetTimeInterpolatedStops"""
  def setUp(self):
    problems = util.GetTestFailureProblemReporter(self)
    schedule = transitfeed.Schedule(problem_reporter=problems)
    self.schedule = schedule
    schedule.AddAgency("Agency", "http://iflyagency.com",
                       "America/Los_Angeles")
    service_period = schedule.GetDefaultServicePeriod()
    service_period.SetDateHasService('20070101')
    self.stop1 = schedule.AddStop(lng=140.01, lat=0, name="140.01,0")
    self.stop2 = schedule.AddStop(lng=140.02, lat=0, name="140.02,0")
    self.stop3 = schedule.AddStop(lng=140.03, lat=0, name="140.03,0")
    self.stop4 = schedule.AddStop(lng=140.04, lat=0, name="140.04,0")
    self.stop5 = schedule.AddStop(lng=140.05, lat=0, name="140.05,0")
    self.route1 = schedule.AddRoute("1", "One", "Bus")

    self.trip1 = self.route1.AddTrip(schedule, "trip 1", trip_id='trip1')
    self.trip1.AddStopTime(self.stop1, schedule=schedule, departure_secs=100,
                           arrival_secs=100)
    self.trip1.AddStopTime(self.stop2, schedule=schedule)
    self.trip1.AddStopTime(self.stop3, schedule=schedule)
    # loop back to stop2 to test that interpolated stops work ok even when
    # a stop between timepoints is further from the timepoint than the
    # preceding
    self.trip1.AddStopTime(self.stop2, schedule=schedule)
    self.trip1.AddStopTime(self.stop4, schedule=schedule, departure_secs=400,
                           arrival_secs=400)

    self.trip2 = self.route1.AddTrip(schedule, "trip 2", trip_id='trip2')
    self.trip2.AddStopTime(self.stop2, schedule=schedule, departure_secs=500,
                           arrival_secs=500)
    self.trip2.AddStopTime(self.stop3, schedule=schedule, departure_secs=600,
                           arrival_secs=600)
    self.trip2.AddStopTime(self.stop4, schedule=schedule, departure_secs=700,
                           arrival_secs=700)
    self.trip2.AddStopTime(self.stop3, schedule=schedule, departure_secs=800,
                           arrival_secs=800)

    self.trip3 = self.route1.AddTrip(schedule, "trip 3", trip_id='trip3')

  def testGetTimeInterpolatedStops(self):
    rv = self.trip1.GetTimeInterpolatedStops()
    self.assertEqual(5, len(rv))
    (secs, stoptimes, istimepoints) = tuple(zip(*rv))

    self.assertEqual((100, 160, 220, 280, 400), secs)
    self.assertEqual(("140.01,0", "140.02,0", "140.03,0", "140.02,0", "140.04,0"),
                     tuple([st.stop.stop_name for st in stoptimes]))
    self.assertEqual((True, False, False, False, True), istimepoints)

    self.assertEqual([], self.trip3.GetTimeInterpolatedStops())

  def testGetTimeInterpolatedStopsUntimedEnd(self):
    self.trip2.AddStopTime(self.stop3, schedule=self.schedule)
    self.assertRaises(ValueError, self.trip2.GetTimeInterpolatedStops)

  def testGetTimeInterpolatedStopsUntimedStart(self):
    # Temporarily replace the problem reporter so that adding the first
    # StopTime without a time doesn't throw an exception.
    old_problems = self.schedule.problem_reporter
    self.schedule.problem_reporter = util.GetTestFailureProblemReporter(
        self, ("OtherProblem",))
    self.trip3.AddStopTime(self.stop3, schedule=self.schedule)
    self.schedule.problem_reporter = old_problems
    self.trip3.AddStopTime(self.stop2, schedule=self.schedule,
                           departure_secs=500, arrival_secs=500)
    self.assertRaises(ValueError, self.trip3.GetTimeInterpolatedStops)

  def testGetTimeInterpolatedStopsSingleStopTime(self):
    self.trip3.AddStopTime(self.stop3, schedule=self.schedule,
                           departure_secs=500, arrival_secs=500)
    rv = self.trip3.GetTimeInterpolatedStops()
    self.assertEqual(1, len(rv))
    self.assertEqual(500, rv[0][0])
    self.assertEqual(True, rv[0][2])

  def testGetStopTimeTrips(self):
    stopa = self.schedule.GetNearestStops(lon=140.03, lat=0)[0]
    self.assertEqual("140.03,0", stopa.stop_name)  # Got stop3?
    rv = stopa.GetStopTimeTrips(self.schedule)
    self.assertEqual(3, len(rv))
    (secs, trip_index, istimepoints) = tuple(zip(*rv))
    self.assertEqual((220, 600, 800), secs)
    self.assertEqual(("trip1", "trip2", "trip2"), tuple([ti[0].trip_id for ti in trip_index]))
    self.assertEqual((2, 1, 3), tuple([ti[1] for ti in trip_index]))
    self.assertEqual((False, True, True), istimepoints)

  def testStopTripIndex(self):
    trip_index = self.stop3.trip_index
    trip_ids = [t.trip_id for t, i in trip_index]
    self.assertEqual(["trip1", "trip2", "trip2"], trip_ids)
    self.assertEqual([2, 1, 3], [i for t, i in trip_index])

  def testGetTrips(self):
    self.assertEqual(
      set([t.trip_id for t in self.stop1.GetTrips(self.schedule)]),
      set([self.trip1.trip_id]))
    self.assertEqual(
      set([t.trip_id for t in self.stop2.GetTrips(self.schedule)]),
      set([self.trip1.trip_id, self.trip2.trip_id]))
    self.assertEqual(
      set([t.trip_id for t in self.stop3.GetTrips(self.schedule)]),
      set([self.trip1.trip_id, self.trip2.trip_id]))
    self.assertEqual(
      set([t.trip_id for t in self.stop4.GetTrips(self.schedule)]),
      set([self.trip1.trip_id, self.trip2.trip_id]))
    self.assertEqual(
      set([t.trip_id for t in self.stop5.GetTrips(self.schedule)]),
      set())

class GetFrequencyTimesTestCase(util.TestCase):
  """Test for GetFrequencyStartTimes and GetFrequencyStopTimes"""
  def setUp(self):
    problems = util.GetTestFailureProblemReporter(self)
    schedule = transitfeed.Schedule(problem_reporter=problems)
    self.schedule = schedule
    schedule.AddAgency("Agency", "http://iflyagency.com",
                       "America/Los_Angeles")
    service_period = schedule.GetDefaultServicePeriod()
    service_period.SetStartDate("20080101")
    service_period.SetEndDate("20090101")
    service_period.SetWeekdayService(True)
    self.stop1 = schedule.AddStop(lng=140.01, lat=0, name="140.01,0")
    self.stop2 = schedule.AddStop(lng=140.02, lat=0, name="140.02,0")
    self.stop3 = schedule.AddStop(lng=140.03, lat=0, name="140.03,0")
    self.stop4 = schedule.AddStop(lng=140.04, lat=0, name="140.04,0")
    self.stop5 = schedule.AddStop(lng=140.05, lat=0, name="140.05,0")
    self.route1 = schedule.AddRoute("1", "One", "Bus")

    self.trip1 = self.route1.AddTrip(schedule, "trip 1", trip_id="trip1")
    # add different types of stop times
    self.trip1.AddStopTime(self.stop1, arrival_time="17:00:00",
        departure_time="17:01:00") # both arrival and departure time
    self.trip1.AddStopTime(self.stop2, schedule=schedule) # non timed
    self.trip1.AddStopTime(self.stop3, stop_time="17:45:00") # only stop_time

    # add headways starting before the trip
    self.trip1.AddFrequency("16:00:00", "18:00:00", 1800) # each 30 min
    self.trip1.AddFrequency("18:00:00", "20:00:00", 2700) # each 45 min

  def testGetFrequencyStartTimes(self):
    start_times = self.trip1.GetFrequencyStartTimes()
    self.assertEqual(
        ["16:00:00", "16:30:00", "17:00:00", "17:30:00",
         "18:00:00", "18:45:00", "19:30:00"],
        [transitfeed.FormatSecondsSinceMidnight(secs) for secs in start_times])
    # GetHeadwayStartTimes is deprecated, but should still return the same
    # result as GetFrequencyStartTimes
    self.assertEqual(start_times,
                     self.trip1.GetFrequencyStartTimes())

  def testGetFrequencyStopTimes(self):
    stoptimes_list = self.trip1.GetFrequencyStopTimes()
    arrival_secs = []
    departure_secs = []
    for stoptimes in stoptimes_list:
      arrival_secs.append([st.arrival_secs for st in stoptimes])
      departure_secs.append([st.departure_secs for st in stoptimes])

    # GetHeadwayStopTimes is deprecated, but should still return the same
    # result as GetFrequencyStopTimes
    # StopTimes are instantiated as they're read from the DB so they can't be
    # compared directly, but checking {arrival,departure}_secs should be enough
    # to catch most errors.
    headway_stoptimes_list = self.trip1.GetFrequencyStopTimes()
    headway_arrival_secs = []
    headway_departure_secs = []
    for stoptimes in stoptimes_list:
      headway_arrival_secs.append([st.arrival_secs for st in stoptimes])
      headway_departure_secs.append([st.departure_secs for st in stoptimes])
    self.assertEqual(arrival_secs, headway_arrival_secs)
    self.assertEqual(departure_secs, headway_departure_secs)

    self.assertEqual(([57600,None,60300],[59400,None,62100],[61200,None,63900],
                      [63000,None,65700],[64800,None,67500],[67500,None,70200],
                      [70200,None,72900]),
                     tuple(arrival_secs))
    self.assertEqual(([57660,None,60300],[59460,None,62100],[61260,None,63900],
                      [63060,None,65700],[64860,None,67500],[67560,None,70200],
                      [70260,None,72900]),
                     tuple(departure_secs))

    # test if stoptimes are created with same parameters than the ones from the original trip
    stoptimes = self.trip1.GetStopTimes()
    for stoptimes_clone in stoptimes_list:
      self.assertEqual(len(stoptimes_clone), len(stoptimes))
      for st_clone, st in zip(stoptimes_clone, stoptimes):
        for name in st.__slots__:
          if name not in ('arrival_secs', 'departure_secs'):
            self.assertEqual(getattr(st, name), getattr(st_clone, name))
