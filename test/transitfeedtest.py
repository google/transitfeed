#!/usr/bin/python2.4

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

# Unit tests for the transitfeed module.

import transitfeed
import unittest
import sys
import os.path


def DataPath(path):
  return os.path.join('data', path)


class TestFailureProblemReporter(transitfeed.ProblemReporter):
  """Causes a test failure immediately on any problem."""
  def __init__(self, test_case):
    transitfeed.ProblemReporter.__init__(self)
    self.test_case = test_case

  def _Report(self, problem_text):
    self.test_case.fail(problem_text)


class LoadTestCase(unittest.TestCase):
  problems = transitfeed.ExceptionProblemReporter()

  def ExpectInvalidValue(self, feed_name, field_name):
    loader = transitfeed.Loader(
      DataPath(feed_name), problems=self.problems, extra_validation=True)
    try:
      loader.Load()
      self.fail('InvalidValue exception expected')
    except transitfeed.InvalidValue, e:
      self.assertEqual(field_name, e.field_name)

  def ExpectMissingFile(self, feed_name, file_name):
    loader = transitfeed.Loader(
      DataPath(feed_name), problems=self.problems, extra_validation=True)
    try:
      loader.Load()
      self.fail('MissingFile exception expected')
    except transitfeed.MissingFile, e:
      self.assertEqual(file_name, e.file_name)


class LoadFromZipTestCase(unittest.TestCase):
  def runTest(self):
    loader = transitfeed.Loader(
      DataPath('good_feed.zip'),
      problems = TestFailureProblemReporter(self),
      extra_validation = True)
    loader.Load()

    # now try using Schedule.Load
    schedule = transitfeed.Schedule(
        problem_reporter=transitfeed.ExceptionProblemReporter())
    schedule.Load(DataPath('good_feed.zip'), extra_validation=True)
    

class LoadFromDirectoryTestCase(unittest.TestCase):
  def runTest(self):
    loader = transitfeed.Loader(
      DataPath('good_feed'),
      problems = TestFailureProblemReporter(self),
      extra_validation = True)
    loader.Load()


class LoadUnknownFeedTestCase(unittest.TestCase):
  def runTest(self):
    feed_name = DataPath('unknown_feed')
    loader = transitfeed.Loader(
      feed_name,
      problems = transitfeed.ExceptionProblemReporter(),
      extra_validation = True)
    try:
      loader.Load()
      self.fail('FeedNotFound exception expected')
    except transitfeed.FeedNotFound, e:
      self.assertEqual(feed_name, e.feed_name)

class LoadUnknownFormatTestCase(unittest.TestCase):
  def runTest(self):
    feed_name = DataPath('unknown_format.zip')
    loader = transitfeed.Loader(
      feed_name,
      problems = transitfeed.ExceptionProblemReporter(),
      extra_validation = True)
    try:
      loader.Load()
      self.fail('UnknownFormat exception expected')
    except transitfeed.UnknownFormat, e:
      self.assertEqual(feed_name, e.feed_name)


class LoadUTF8BOMTestCase(unittest.TestCase):
  def runTest(self):
    loader = transitfeed.Loader(
      DataPath('utf8bom'),
      problems = TestFailureProblemReporter(self),
      extra_validation = True)
    loader.Load()


class LoadMissingAgencyTestCase(LoadTestCase):
  def runTest(self):
    self.ExpectMissingFile('missing_agency', 'agency.txt')


class LoadMissingStopsTestCase(LoadTestCase):
  def runTest(self):
    self.ExpectMissingFile('missing_stops', 'stops.txt')


class LoadMissingRoutesTestCase(LoadTestCase):
  def runTest(self):
    self.ExpectMissingFile('missing_routes', 'routes.txt')


class LoadMissingTripsTestCase(LoadTestCase):
  def runTest(self):
    self.ExpectMissingFile('missing_trips', 'trips.txt')


class LoadMissingStopTimesTestCase(LoadTestCase):
  def runTest(self):
    self.ExpectMissingFile('missing_stop_times', 'stop_times.txt')


class LoadMissingCalendarTestCase(LoadTestCase):
  def runTest(self):
    self.ExpectMissingFile('missing_calendar', 'calendar.txt')


class EmptyFileTestCase(unittest.TestCase):
  def runTest(self):
    loader = transitfeed.Loader(
      DataPath('empty_file'),
      problems = transitfeed.ExceptionProblemReporter(),
      extra_validation = True)
    try:
      loader.Load()
      self.fail('EmptyFile exception expected')
    except transitfeed.EmptyFile, e:
      self.assertEqual('agency.txt', e.file_name)


class MissingColumnTestCase(unittest.TestCase):
  def runTest(self):
    loader = transitfeed.Loader(
      DataPath('missing_column'),
      problems = transitfeed.ExceptionProblemReporter(),
      extra_validation = True)
    try:
      loader.Load()
      self.fail('MissingColumn exception expected')
    except transitfeed.MissingColumn, e:
      self.assertEqual('agency.txt', e.file_name)
      self.assertEqual('agency_name', e.column_name)


class ZeroBasedStopSequenceTestCase(LoadTestCase):
  def runTest(self):
    self.ExpectInvalidValue('zero_based_stop_sequence', 'stop_sequence')
    
    
class DuplicateStopTestCase(unittest.TestCase):
  def runTest(self):
    schedule = transitfeed.Schedule(
        problem_reporter=transitfeed.ExceptionProblemReporter())
    try:
      schedule.Load(DataPath('duplicate_stop'), extra_validation=True)
      self.fail('OtherProblem exception expected')
    except transitfeed.OtherProblem:
      pass


INVALID_VALUE = Exception()
class ValidationTestCase(unittest.TestCase):
  problems = transitfeed.ExceptionProblemReporter()

  def ExpectMissingValue(self, object, field_name):
    try:
      object.Validate(self.problems)
      self.fail('MissingValue exception expected')
    except transitfeed.MissingValue, e:
      self.assertEqual(field_name, e.field_name)

  def ExpectInvalidValue(self, object, field_name, value=INVALID_VALUE):
    if value==INVALID_VALUE:
      value = object.__getattribute__(field_name)
    try:
      object.Validate(self.problems)
      self.fail('InvalidValue exception expected')
    except transitfeed.InvalidValue, e:
      self.assertEqual(field_name, e.field_name)
      self.assertEqual(value, e.value)

  def ExpectOtherProblem(self, object):
    try:
      object.Validate(self.problems)
      self.fail('OtherProblem exception expected')
    except transitfeed.OtherProblem:
      pass


class AgencyValidationTestCase(ValidationTestCase):
  def runTest(self):
    # success case
    agency = transitfeed.Agency(name='Test Agency', url='http://example.com',
                                timezone='America/Los_Angeles', id='TA')
    agency.Validate(self.problems)

    # bad agency
    agency = transitfeed.Agency(name='   ', url='http://example.com',
                                timezone='America/Los_Angeles', id='TA')
    self.ExpectMissingValue(agency, 'agency_name')

    # missing url
    agency = transitfeed.Agency(name='Test Agency',
                                timezone='America/Los_Angeles', id='TA')
    self.ExpectMissingValue(agency, 'agency_url')

    # bad url
    agency = transitfeed.Agency(name='Test Agency', url='www.example.com',
                                timezone='America/Los_Angeles', id='TA')
    self.ExpectInvalidValue(agency, 'agency_url')

    # bad time zone
    agency = transitfeed.Agency(name='Test Agency', url='http://example.com',
                                timezone='America/Alviso', id='TA')
    self.ExpectInvalidValue(agency, 'agency_timezone')


class StopValidationTestCase(ValidationTestCase):
  def runTest(self):
    # success case
    stop = transitfeed.Stop()
    stop.stop_id = '45'
    stop.stop_name = 'Couch AT End Table'
    stop.stop_lat = 50.0
    stop.stop_lon = 50.0
    stop.stop_desc = 'Edge of the Couch'
    stop.zone_id = 'A'
    stop.stop_url = 'http://example.com'
    stop.Validate(self.problems)

    # latitute too large
    stop.stop_lat = 100.0
    self.ExpectInvalidValue(stop, 'stop_lat')
    stop.stop_lat = 50.0

    # longitude too large
    stop.stop_lon = 200.0
    self.ExpectInvalidValue(stop, 'stop_lon')
    stop.stop_lon = 50.0

    # lat, lon too close to 0, 0
    stop.stop_lat = 0.0
    stop.stop_lon = 0.0
    self.ExpectInvalidValue(stop, 'stop_lat')
    stop.stop_lat = 50.0
    stop.stop_lon = 50.0

    # invalid stop_url
    stop.stop_url = 'www.example.com'
    self.ExpectInvalidValue(stop, 'stop_url')
    stop.stop_url = 'http://example.com'

    stop.stop_id = '   '
    self.ExpectMissingValue(stop, 'stop_id')
    stop.stop_id = '45'

    stop.stop_name = None
    self.ExpectMissingValue(stop, 'stop_name')
    stop.stop_name = 'Couch AT End Table'

    # description same as name
    stop.stop_desc = 'Couch AT End Table'
    self.ExpectInvalidValue(stop, 'stop_desc')
    stop.stop_desc = 'Edge of the Couch'


class RouteValidationTestCase(ValidationTestCase):
  def runTest(self):
    # success case
    route = transitfeed.Route()
    route.route_id = '054C'
    route.route_short_name = '54C'
    route.route_long_name = 'South Side - North Side'
    route.route_type = 7
    route.Validate(self.problems)

    # blank short & long names
    route.route_short_name = ''
    route.route_long_name = '    '
    self.ExpectInvalidValue(route, 'route_short_name')
    route.route_short_name = '54C'
    route.route_long_name = 'South Side - North Side'

    # long name contains short name
    route.route_long_name = '54C South Side - North Side'
    self.ExpectInvalidValue(route, 'route_long_name')
    route.route_long_name = '54C-South Side - North Side'
    self.ExpectInvalidValue(route, 'route_long_name')
    route.route_long_name = 'South Side - North Side'

    # long name is same as short name
    route.route_long_name = '54C'
    self.ExpectInvalidValue(route, 'route_long_name')
    route.route_long_name = 'South Side - North Side'

    # route description is same as short name
    route.route_desc = '54C'
    self.ExpectInvalidValue(route, 'route_desc')
    route.route_desc = None

    # route description is same as long name
    route.route_desc = 'South Side - North Side'
    self.ExpectInvalidValue(route, 'route_desc')
    route.route_desc = None

    # invalid route types
    route.route_type = 8
    self.ExpectInvalidValue(route, 'route_type')
    route.route_type = -1
    self.ExpectInvalidValue(route, 'route_type')
    route.route_type = 7

    # invalid route URL
    route.route_url = 'www.example.com'
    self.ExpectInvalidValue(route, 'route_url')
    route.route_url = None

    # invalid route color
    route.route_color = 'orange'
    self.ExpectInvalidValue(route, 'route_color')
    route.route_color = None

    # invalid route text color
    route.route_text_color = 'orange'
    self.ExpectInvalidValue(route, 'route_text_color')
    route.route_text_color = None

    # missing route ID
    route.route_id = None
    self.ExpectMissingValue(route, 'route_id')
    route.route_id = '054C'


class ShapeValidationTestCase(ValidationTestCase):
  def ExpectFailedAdd(self, shape, lat, lon, dist, field_name, value):
    try:
      shape.AddPoint(lat, lon, dist, self.problems)
      self.fail('Expected validation exception!')
    except transitfeed.InvalidValue, e:
      self.assertEqual(field_name, e.field_name)
      self.assertEqual(value, e.value)

  def runTest(self):
    shape = transitfeed.Shape('TEST')
    self.ExpectOtherProblem(shape)  # no points!

    self.ExpectFailedAdd(shape, 36.905019, -116.763207, -1,
                         'shape_dist_traveled', -1)

    shape.AddPoint(36.915760, -116.751709, 0, self.problems)
    shape.AddPoint(36.905018, -116.763206, 5, self.problems)
    shape.Validate(self.problems)

    shape.shape_id = None
    self.ExpectMissingValue(shape, 'shape_id')
    shape.shape_id = 'TEST'

    self.ExpectFailedAdd(shape, 91, -116.751709, 6, 'shape_pt_lat', 91)
    self.ExpectFailedAdd(shape, -91, -116.751709, 6, 'shape_pt_lat', -91)

    self.ExpectFailedAdd(shape, 36.915760, -181, 6, 'shape_pt_lon', -181)
    self.ExpectFailedAdd(shape, 36.915760, 181, 6, 'shape_pt_lon', 181)

    self.ExpectFailedAdd(shape, 0.5, -0.5, 6, 'shape_pt_lat', 0.5)
    self.ExpectFailedAdd(shape, 0, 0, 6, 'shape_pt_lat', 0)

    # distance decreasing is bad, but staying the same is OK
    self.ExpectFailedAdd(shape, 36.905019, -116.763206, 4,
                         'shape_dist_traveled', 4)
    shape.AddPoint(36.905019, -116.763206, 5, self.problems)

class FareValidationTestCase(ValidationTestCase):
  def runTest(self):
    fare = transitfeed.Fare()
    fare.fare_id = "normal"
    fare.price = 1.50
    fare.currency_type = "USD"
    fare.payment_method = 0
    fare.transfers = 1
    fare.transfer_duration = 7200
    fare.Validate(self.problems)

    fare.fare_id = None
    self.ExpectMissingValue(fare, "fare_id")
    fare.fare_id = ''
    self.ExpectMissingValue(fare, "fare_id")
    fare.fare_id = "normal"

    fare.price = "1.50"
    self.ExpectInvalidValue(fare, "price")
    fare.price = 1
    fare.Validate(self.problems)
    fare.price = None
    self.ExpectMissingValue(fare, "price")
    fare.price = 0.0
    fare.Validate(self.problems)
    fare.price = -1.50
    self.ExpectInvalidValue(fare, "price")
    fare.price = 1.50

    fare.currency_type = ""
    self.ExpectMissingValue(fare, "currency_type")
    fare.currency_type = None
    self.ExpectMissingValue(fare, "currency_type")
    fare.currency_type = "usd"
    self.ExpectInvalidValue(fare, "currency_type")
    fare.currency_type = "KML"
    self.ExpectInvalidValue(fare, "currency_type")
    fare.currency_type = "USD"

    fare.payment_method = "0"
    self.ExpectInvalidValue(fare, "payment_method")
    fare.payment_method = -1
    self.ExpectInvalidValue(fare, "payment_method")
    fare.payment_method = 1
    fare.Validate(self.problems)
    fare.payment_method = 2
    self.ExpectInvalidValue(fare, "payment_method")
    fare.payment_method = None
    self.ExpectMissingValue(fare, "payment_method")
    fare.payment_method = ""
    self.ExpectMissingValue(fare, "payment_method")
    fare.payment_method = 0

    fare.transfers = "1"
    self.ExpectInvalidValue(fare, "transfers")
    fare.transfers = -1
    self.ExpectInvalidValue(fare, "transfers")
    fare.transfers = 2
    fare.Validate(self.problems)
    fare.transfers = 3
    self.ExpectInvalidValue(fare, "transfers")
    fare.transfers = None
    fare.Validate(self.problems)
    fare.transfers = 1

    fare.transfer_duration = 0
    fare.Validate(self.problems)
    fare.transfer_duration = None
    fare.Validate(self.problems)
    fare.transfer_duration = -3600
    self.ExpectInvalidValue(fare, "transfer_duration")
    fare.transfers = 0  # no transfers allowed but duration specified!
    fare.transfer_duration = 3600
    self.ExpectInvalidValue(fare, "transfer_duration")
    fare.transfers = 1
    fare.transfer_duration = "3600"
    self.ExpectInvalidValue(fare, "transfer_duration")
    fare.transfer_duration = 7200


class ServicePeriodValidationTestCase(ValidationTestCase):
  def runTest(self):
    # success case
    period = transitfeed.ServicePeriod()
    period.service_id = 'WEEKDAY'
    period.start_date = '20070101'
    period.end_date = '20071231'
    period.day_of_week[0] = True
    period.Validate(self.problems)

    # missing start_date
    period.start_date = None
    self.ExpectMissingValue(period, 'start_date')
    period.start_date = '20070101'

    # missing end_date
    period.end_date = None
    self.ExpectMissingValue(period, 'end_date')
    period.end_date = '20071231'

    # invalid start_date
    period.start_date = '2007-01-01'
    self.ExpectInvalidValue(period, 'start_date')
    period.start_date = '20070101'

    # invalid end_date
    period.end_date = '2007/12/31'
    self.ExpectInvalidValue(period, 'end_date')
    period.end_date = '20071231'

    # start & end dates out of order
    period.end_date = '20060101'
    self.ExpectInvalidValue(period, 'end_date')
    period.end_date = '20071231'

    # no service in period
    period.day_of_week[0] = False
    self.ExpectOtherProblem(period)
    period.day_of_week[0] = True

    # invalid exception date
    period.SetDateHasService('2007', False)
    self.ExpectInvalidValue(period, 'date', '2007')
    period.ResetDateToNormalService('2007')


class TripValidationTestCase(ValidationTestCase):
  def runTest(self):
    trip = transitfeed.Trip()
    trip.route_id = '054C'
    trip.service_id = 'WEEK'
    trip.trip_id = '054C-00'
    trip.trip_headsign = 'via Polish Hill'
    trip.direction_id = '0'
    trip.block_id = None
    trip.shape_id = None
    trip.Validate(self.problems)

    # missing route ID
    trip.route_id = None
    self.ExpectMissingValue(trip, 'route_id')
    trip.route_id = '054C'

    # missing service ID
    trip.service_id = None
    self.ExpectMissingValue(trip, 'service_id')
    trip.service_id = 'WEEK'

    # missing trip ID
    trip.trip_id = None
    self.ExpectMissingValue(trip, 'trip_id')
    trip.trip_id = '054C-00'

    # invalid direction ID
    trip.direction_id = 'NORTH'
    self.ExpectInvalidValue(trip, 'direction_id')
    trip.direction_id = '0'

    # expect no problems for non-overlapping periods
    trip.AddHeadwayPeriod("06:00:00", "12:00:00", 600)
    trip.AddHeadwayPeriod("01:00:00", "02:00:00", 1200)
    trip.AddHeadwayPeriod("04:00:00", "05:00:00", 1000)
    trip.AddHeadwayPeriod("12:00:00", "19:00:00", 700)
    trip.Validate(self.problems)
    trip.ClearHeadwayPeriods()

    # overlapping headway periods
    trip.AddHeadwayPeriod("00:00:00", "12:00:00", 600)
    trip.AddHeadwayPeriod("06:00:00", "18:00:00", 1200)
    self.ExpectOtherProblem(trip)
    trip.ClearHeadwayPeriods()
    trip.AddHeadwayPeriod("12:00:00", "20:00:00", 600)
    trip.AddHeadwayPeriod("06:00:00", "18:00:00", 1200)
    self.ExpectOtherProblem(trip)
    trip.ClearHeadwayPeriods()
    trip.AddHeadwayPeriod("06:00:00", "12:00:00", 600)
    trip.AddHeadwayPeriod("00:00:00", "25:00:00", 1200)
    self.ExpectOtherProblem(trip)
    trip.ClearHeadwayPeriods()
    trip.AddHeadwayPeriod("00:00:00", "20:00:00", 600)
    trip.AddHeadwayPeriod("06:00:00", "18:00:00", 1200)
    self.ExpectOtherProblem(trip)
    trip.ClearHeadwayPeriods()


class TripHasStopTimeValidationTestCase(ValidationTestCase):
  def runTest(self):
    schedule = transitfeed.Schedule(self.problems)
    schedule.AddAgency("Test Agency", "http://example.com",
                       "America/Los_Angeles")
    schedule.AddRouteObject(
        transitfeed.Route("54C", "Polish Hill", 3, "054C"))

    trip = transitfeed.Trip()
    trip.route_id = '054C'
    trip.service_id = 'WEEK'
    trip.trip_id = '054C-00'
    trip.trip_headsign = 'via Polish Hill'
    trip.direction_id = '0'
    trip.block_id = None
    trip.shape_id = None
    schedule.AddTripObject(trip)

    # We should get an OtherProblem here because the trip has no stops.
    self.ExpectOtherProblem(schedule)

    # Add a stop, but with only one stop passengers have nowhere to exit!
    stop = transitfeed.Stop(36.425288, -117.133162, "Demo Stop 1", "STOP1")
    schedule.AddStopObject(stop)
    trip.AddStopTime(stop, "5:11:00", "5:12:00")
    self.ExpectOtherProblem(schedule)

    # Add another stop, and then validation should be happy.
    stop = transitfeed.Stop(36.424288, -117.133142, "Demo Stop 2", "STOP2")
    schedule.AddStopObject(stop)
    trip.AddStopTime(stop, "5:15:00", "5:16:00")
    schedule.Validate(self.problems)

    # Add some more stop times and test GetEndTime does the correct thing
    self.assertEqual(transitfeed.FormatSecondsSinceMidnight(trip.GetStartTime()),
        "05:11:00")
    self.assertEqual(transitfeed.FormatSecondsSinceMidnight(trip.GetEndTime()),
        "05:16:00")

    trip.AddStopTime(stop, None, "05:20:00")
    self.assertEqual(transitfeed.FormatSecondsSinceMidnight(trip.GetEndTime()),
        "05:20:00")

    trip.AddStopTime(stop, "05:22:00", None)
    self.assertEqual(transitfeed.FormatSecondsSinceMidnight(trip.GetEndTime()),
        "05:22:00")

    # Last stop must always have a time
    trip.AddStopTime(stop, None, None)
    try:
      trip.GetEndTime()
      self.fail('exception expected')
    except transitfeed.Error, e:
      pass


class BasicParsingTestCase(unittest.TestCase):
  """Checks that we're getting the number of child objects that we expect."""
  def runTest(self):
    loader = transitfeed.Loader(
      DataPath('good_feed.zip'),
      problems = transitfeed.ExceptionProblemReporter(),
      extra_validation = True)
    schedule = loader.Load()
    self.assertEqual(1, len(schedule.agencies))
    self.assertEqual(5, len(schedule.routes))
    self.assertEqual(2, len(schedule.service_periods))
    self.assertEqual(9, len(schedule.stops))
    self.assertEqual(11, len(schedule.trips))
    self.assertEqual(0, len(schedule.fare_zones))


class RepeatedRouteNameTestCase(LoadTestCase):
  def runTest(self):
    self.ExpectInvalidValue('repeated_route_name', 'route_long_name')


class InvalidRouteAgencyTestCase(LoadTestCase):
  def runTest(self):
    self.ExpectInvalidValue('invalid_route_agency', 'agency_id')


class UndefinedStopAgencyTestCase(LoadTestCase):
  def runTest(self):
    self.ExpectInvalidValue('undefined_stop', 'stop_id')


class SameShortLongNameTestCase(LoadTestCase):
  def runTest(self):
    self.ExpectInvalidValue('same_short_long_name', 'route_long_name')


class UnusedStopAgencyTestCase(unittest.TestCase):
  def runTest(self):
    loader = transitfeed.Loader(
      DataPath('unused_stop'),
      problems = transitfeed.ExceptionProblemReporter(),
      extra_validation = True)
    try:
      loader.Load()
      self.fail('OtherProblem exception expected')
    except transitfeed.OtherProblem:
      pass


class OnlyCalendarDatesTestCase(unittest.TestCase):
  def runTest(self):
    loader = transitfeed.Loader(
      DataPath('only_calendar_dates'),
      problems = TestFailureProblemReporter(self),
      extra_validation = True)
    loader.Load()


class AddStopTimeParametersTestCase(unittest.TestCase):
  def runTest(self):
    stop = transitfeed.Stop()
    trip = transitfeed.Trip()
    trip.route_id = "SAMPLE_ID"
    trip.service_id = "WEEK"
    trip.trip_id = "SAMPLE_TRIP"

    trip.AddStopTime(stop, None, None)
    trip.AddStopTime(stop, 300, 360)
    trip.AddStopTime(stop, "00:03:00", "00:05:00")
    trip.Validate(TestFailureProblemReporter(self))


class DuplicateTripIDValidationTestCase(unittest.TestCase):
  def runTest(self):
    schedule = transitfeed.Schedule(
        problem_reporter=transitfeed.ExceptionProblemReporter())
    schedule.AddAgency("Sample Agency", "http://example.com",
                       "America/Los_Angeles")
    route = transitfeed.Route()
    route.route_id = "SAMPLE_ID"
    route.route_type = 3
    route.route_long_name = "Sample Route"
    schedule.AddRouteObject(route)

    trip1 = transitfeed.Trip()
    trip1.route_id = "SAMPLE_ID"
    trip1.service_id = "WEEK"
    trip1.trip_id = "SAMPLE_TRIP"
    schedule.AddTripObject(trip1)

    trip2 = transitfeed.Trip()
    trip2.route_id = "SAMPLE_ID"
    trip2.service_id = "SATU"
    trip2.trip_id = "SAMPLE_TRIP"
    try:
      schedule.AddTripObject(trip2)
      self.fail("Expected Duplicate ID validation failure")
    except transitfeed.DuplicateID, e:
      self.assertEqual("trip_id", e.column_name)
      self.assertEqual("SAMPLE_TRIP", e.value)


class DuplicateStopValidationTestCase(ValidationTestCase):
  def runTest(self):
    schedule = transitfeed.Schedule(problem_reporter=self.problems)
    schedule.AddAgency("Sample Agency", "http://example.com",
                       "America/Los_Angeles")
    route = transitfeed.Route()
    route.route_id = "SAMPLE_ID"
    route.route_type = 3
    route.route_long_name = "Sample Route"
    schedule.AddRouteObject(route)

    trip = transitfeed.Trip()
    trip.route_id = "SAMPLE_ID"
    trip.service_id = "WEEK"
    trip.trip_id = "SAMPLE_TRIP"
    schedule.AddTripObject(trip)

    stop1 = transitfeed.Stop()
    stop1.stop_id = "STOP1"
    stop1.stop_name = "Stop 1"
    stop1.stop_lat = 78.243587
    stop1.stop_lon = 32.258937
    schedule.AddStopObject(stop1)
    trip.AddStopTime(stop1, "12:00:00", "12:00:00")
    
    stop2 = transitfeed.Stop()
    stop2.stop_id = "STOP2"
    stop2.stop_name = "Stop 2"
    stop2.stop_lat = 78.253587
    stop2.stop_lon = 32.258937
    schedule.AddStopObject(stop2)
    trip.AddStopTime(stop2, "12:05:00", "12:05:00")
    schedule.Validate()

    stop3 = transitfeed.Stop()
    stop3.stop_id = "STOP3"
    stop3.stop_name = "Stop 3"
    stop3.stop_lat = 78.243587
    stop3.stop_lon = 32.268937
    schedule.AddStopObject(stop3)
    trip.AddStopTime(stop3, "12:10:00", "12:10:00")
    schedule.Validate()

    stop4 = transitfeed.Stop()
    stop4.stop_id = "STOP4"
    stop4.stop_name = "Stop 4"
    stop4.stop_lat = 78.243588
    stop4.stop_lon = 32.268936
    schedule.AddStopObject(stop4)
    trip.AddStopTime(stop4, "12:15:00", "12:15:00")
    self.ExpectOtherProblem(schedule)


class AgencyIDValidationTestCase(unittest.TestCase):
  def runTest(self):
    schedule = transitfeed.Schedule(
        problem_reporter=transitfeed.ExceptionProblemReporter())
    route = transitfeed.Route()
    route.route_id = "SAMPLE_ID"
    route.route_type = 3
    route.route_long_name = "Sample Route"
    # no agency defined yet, failure.
    try:
      schedule.AddRouteObject(route)
      self.fail("Expected validation error")
    except transitfeed.InvalidValue, e:
      self.assertEqual('agency_id', e.field_name)
      self.assertEqual(None, e.value)

    # one agency defined, assume that the route belongs to it
    schedule.AddAgency("Test Agency", "http://example.com",
                       "America/Los_Angeles", "TEST_AGENCY")
    schedule.AddRouteObject(route)

    schedule.AddAgency("Test Agency 2", "http://example.com",
                       "America/Los_Angeles", "TEST_AGENCY_2")
    route = transitfeed.Route()
    route.route_id = "SAMPLE_ID_2"
    route.route_type = 3
    route.route_long_name = "Sample Route 2"
    # multiple agencies defined, don't know what omitted agency_id should be
    try:
      schedule.AddRouteObject(route)
      self.fail("Expected validation error")
    except transitfeed.InvalidValue, e:
      self.assertEqual('agency_id', e.field_name)
      self.assertEqual(None, e.value)

    # agency with no agency_id defined, matches route with no agency id
    schedule.AddAgency("Test Agency 3", "http://example.com",
                       "America/Los_Angeles")
    schedule.AddRouteObject(route)


class AddHeadwayPeriodValidationTestCase(ValidationTestCase):
  def ExpectInvalidValue(self, start_time, end_time, headway,
                         field_name, value):
    try:
      trip = transitfeed.Trip()
      trip.AddHeadwayPeriod(start_time, end_time, headway)
      self.fail("Expected InvalidValue error on %s" % field_name)
    except transitfeed.InvalidValue, e:
      self.assertEqual(field_name, e.field_name)
      self.assertEqual(value, e.value)
      self.assertEqual(0, len(trip.GetHeadwayPeriodTuples()))

  def ExpectMissingValue(self, start_time, end_time, headway, field_name):
    try:
      trip = transitfeed.Trip()
      trip.AddHeadwayPeriod(start_time, end_time, headway)
      self.fail("Expected MissingValue error on %s" % field_name)
    except transitfeed.MissingValue, e:
      self.assertEqual(field_name, e.field_name)
      self.assertEqual(0, len(trip.GetHeadwayPeriodTuples()))

  def runTest(self):
    # these should work fine
    trip = transitfeed.Trip()
    trip.trip_id = "SAMPLE_ID"
    trip.AddHeadwayPeriod(0, 50, 1200)
    trip.AddHeadwayPeriod("01:00:00", "02:00:00", "600")
    trip.AddHeadwayPeriod(u"02:00:00", u"03:00:00", u"1800")
    headways = trip.GetHeadwayPeriodTuples()
    self.assertEqual(3, len(headways))
    self.assertEqual((0, 50, 1200), headways[0])
    self.assertEqual((3600, 7200, 600), headways[1])
    self.assertEqual((7200, 10800, 1800), headways[2])
    self.assertEqual([("SAMPLE_ID", "00:00:00", "00:00:50", "1200"),
                      ("SAMPLE_ID", "01:00:00", "02:00:00", "600"),
                      ("SAMPLE_ID", "02:00:00", "03:00:00", "1800")],
                     trip.GetHeadwayPeriodOutputTuples())

    # now test invalid input
    self.ExpectMissingValue(None, 50, 1200, "start_time")
    self.ExpectMissingValue("", 50, 1200, "start_time")
    self.ExpectInvalidValue("midnight", 50, 1200, "start_time", "midnight")
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
    self.ExpectInvalidValue("12:00:00", "06:00:00", 1200, "end_time", 21600)


class ScheduleBuilderTestCase(unittest.TestCase):
  def runTest(self):
    schedule = transitfeed.Schedule()

    schedule.AddAgency("Test Agency", "http://example.com",
                       "America/Los_Angeles")

    service_period = schedule.GetActiveServicePeriod()
    self.assertTrue(service_period.service_id)
    service_period.SetWeekdayService(has_service=True)
    service_period.SetStartDate("20070320")
    service_period.SetEndDate("20071232")

    stop1 = schedule.AddStop(lng=-140.12, lat=48.921, name="one forty at forty eight")
    stop2 = schedule.AddStop(lng=-140.22, lat=48.421, name="west and south")
    stop3 = schedule.AddStop(lng=-140.32, lat=48.121, name="more away")

    route = schedule.AddRoute(short_name="R", long_name="My Route",
                              route_type="Bus")
    self.assertTrue(route.route_id)
    self.assertEqual(route.route_short_name, "R")
    self.assertEqual(route.route_type, 3)

    trip = route.AddTrip(schedule, headsign="To The End", service_period=service_period)
    trip_id = trip.trip_id
    self.assertTrue(trip_id)
    trip = schedule.GetTrip(trip_id)
    self.assertEqual("To The End", trip.trip_headsign)
    self.assertEqual(service_period, trip.service_period)

    trip.AddStopTime(stop=stop1, time_arr=3600*8, time_dep=3600*8)
    trip.AddStopTime(stop=stop2)
    trip.AddStopTime(stop=stop3, time_arr=3600*8 + 60*15, time_dep=3600*8 + 60*15)

    schedule.Validate()
    self.assertEqual(3, len(trip.GetTimeStops()))
    self.assertEqual(1, len(schedule.GetRouteList()))
    self.assertEqual(3, len(schedule.GetStopList()))

class WriteSampleFeedTestCase(unittest.TestCase):
  def runTest(self):
    problems = TestFailureProblemReporter(self)
    schedule = transitfeed.Schedule(problem_reporter=problems)
    agency = transitfeed.Agency()
    agency.agency_id = "DTA"
    agency.agency_name = "Demo Transit Authority"
    agency.agency_url = "http://google.com"
    agency.agency_timezone = "America/Los_Angeles"
    schedule.AddAgencyObject(agency)

    routes = []
    route_data = [
        ("AB", "DTA", "10", "Airport - Bullfrog", 3),
        ("BFC", "DTA", "20", "Bullfrog - Furnace Creek Resort", 3),
        ("STBA", "DTA", "30", "Stagecoach - Airport Shuttle", 3),
        ("CITY", "DTA", "40", "City", 3),
        ("AAMV", "DTA", "50", "Airport - Amargosa Valley", 3)
      ]

    for route_entry in route_data:
      route = transitfeed.Route()
      (route.route_id, route.agency_id, route.route_short_name,
       route.route_long_name, route.route_type) = route_entry
      routes.append(route)
      schedule.AddRouteObject(route)

    shape_data = [
      (36.915760, -116.751709),
      (36.905018, -116.763206),
      (36.902134, -116.777969),
      (36.904091, -116.788185),
      (36.883602, -116.814537),
      (36.874523, -116.795593),
      (36.873302, -116.786491),
      (36.869202, -116.784241),
      (36.868515, -116.784729),
    ]

    shape = transitfeed.Shape("BFC1S")
    for (lat, lon) in shape_data:
      shape.AddPoint(lat, lon)
    schedule.AddShapeObject(shape)

    stops = []
    stop_data = [
        ("FUR_CREEK_RES", "Furnace Creek Resort (Demo)",
         36.425288, -117.133162),
        ("BEATTY_AIRPORT", "Nye County Airport (Demo)",
         36.868446, -116.784682),
        ("BULLFROG", "Bullfrog (Demo)", 36.88108, -116.81797),
        ("STAGECOACH", "Stagecoach Hotel & Casino (Demo)",
         36.915682, -116.751677),
        ("NADAV", "North Ave / D Ave N (Demo)", 36.914893, -116.76821),
        ("NANAA", "North Ave / N A Ave (Demo)", 36.914944, -116.761472),
        ("DADAN", "Doing AVe / D Ave N (Demo)", 36.909489, -116.768242),
        ("EMSI", "E Main St / S Irving St (Demo)", 36.905697, -116.76218),
        ("AMV", "Amargosa Valley (Demo)", 36.641496, -116.40094),
      ]
    for stop_entry in stop_data:
      stop = transitfeed.Stop()
      (stop.stop_id, stop.stop_name, stop.stop_lat, stop_stop_lon) = stop_entry
      schedule.AddStopObject(stop)
      stops.append(stop)

    trip_data = [
        ("AB", "FULLW", "AB1", "to Bullfrog", "0", "1", None),
        ("AB", "FULLW", "AB2", "to Airport", "1", "2", None),
        ("STBA", "FULLW", "STBA", "Shuttle", None, None, None),
        ("CITY", "FULLW", "CITY1", None, "0", None, None),
        ("CITY", "FULLW", "CITY2", None, "1", None, None),
        ("BFC", "FULLW", "BFC1", "to Furnace Creek Resort", "0", "1", "BFC1S"),
        ("BFC", "FULLW", "BFC2", "to Bullfrog", "1", "2", None),
        ("AAMV", "WE", "AAMV1", "to Amargosa Valley", "0", None, None),
        ("AAMV", "WE", "AAMV2", "to Airport", "1", None, None),
        ("AAMV", "WE", "AAMV3", "to Amargosa Valley", "0", None, None),
        ("AAMV", "WE", "AAMV4", "to Airport", "1", None, None),
      ]

    trips = []
    for trip_entry in trip_data:
      trip = transitfeed.Trip()
      (trip.route_id, trip.service_id, trip.trip_id, trip.trip_headsign,
       trip.direction_id, trip.block_id, trip.shape_id) = trip_entry
      trips.append(trip)
      schedule.AddTripObject(trip)

    stop_time_data = [
        ("STBA", "6:00:00", "6:00:00", "STAGECOACH"),
        ("STBA", "6:20:00", "6:20:00", "BEATTY_AIRPORT"),
        ("CITY1", "6:00:00", "6:00:00", "STAGECOACH"),
        ("CITY1", "6:05:00", "6:07:00", "NANAA"),
        ("CITY1", "6:12:00", "6:14:00", "NADAV"),
        ("CITY1", "6:19:00", "6:21:00", "DADAN"),
        ("CITY1", "6:26:00", "6:28:00", "EMSI"),
        ("CITY2", "6:28:00", "6:28:00", "EMSI"),
        ("CITY2", "6:35:00", "6:37:00", "DADAN"),
        ("CITY2", "6:42:00", "6:44:00", "NADAV"),
        ("CITY2", "6:49:00", "6:51:00", "NANAA"),
        ("CITY2", "6:56:00", "6:58:00", "STAGECOACH"),
        ("AB1", "8:00:00", "8:00:00", "BEATTY_AIRPORT"),
        ("AB1", "8:10:00", "8:15:00", "BULLFROG"),
        ("AB2", "12:05:00", "12:05:00", "BULLFROG"),
        ("AB2", "12:15:00", "12:15:00", "BEATTY_AIRPORT"),
        ("BFC1", "8:20:00", "8:20:00", "BULLFROG"),
        ("BFC1", "9:20:00", "9:20:00", "FUR_CREEK_RES"),
        ("BFC2", "11:00:00", "11:00:00", "FUR_CREEK_RES"),
        ("BFC2", "12:00:00", "12:00:00", "BULLFROG"),
        ("AAMV1", "8:00:00", "8:00:00", "BEATTY_AIRPORT"),
        ("AAMV1", "9:00:00", "9:00:00", "AMV"),
        ("AAMV2", "10:00:00", "10:00:00", "AMV"),
        ("AAMV2", "11:00:00", "11:00:00", "BEATTY_AIRPORT"),
        ("AAMV3", "13:00:00", "13:00:00", "BEATTY_AIRPORT"),
        ("AAMV3", "14:00:00", "14:00:00", "AMV"),
        ("AAMV4", "15:00:00", "15:00:00", "AMV"),
        ("AAMV4", "16:00:00", "16:00:00", "BEATTY_AIRPORT"),
      ]

    for stop_time_entry in stop_time_data:
      (trip_id, arrival_time, departure_time, stop_id) = \
          stop_time_entry
      trip = schedule.GetTrip(trip_id)
      stop = schedule.GetStop(stop_id)
      trip.AddStopTime(stop, arrival_time, departure_time)

    headway_data = [
        ("STBA", "6:00:00", "22:00:00", 1800),
        ("CITY1", "6:00:00", "7:59:59", 1800),
        ("CITY2", "6:00:00", "7:59:59", 1800),
        ("CITY1", "8:00:00", "9:59:59", 600),
        ("CITY2", "8:00:00", "9:59:59", 600),
        ("CITY1", "10:00:00", "15:59:59", 1800),
        ("CITY2", "10:00:00", "15:59:59", 1800),
        ("CITY1", "16:00:00", "18:59:59", 600),
        ("CITY2", "16:00:00", "18:59:59", 600),
        ("CITY1", "19:00:00", "22:00:00", 1800),
        ("CITY2", "19:00:00", "22:00:00", 1800),
      ]

    headway_trips = {}
    for headway_entry in headway_data:
      (trip_id, start_time, end_time, headway) = headway_entry
      headway_trips[trip_id] = []  # adding to set to check later
      trip = schedule.GetTrip(trip_id)
      trip.AddHeadwayPeriod(start_time, end_time, headway, problems)
    for trip_id in headway_trips:
      headway_trips[trip_id] = \
          schedule.GetTrip(trip_id).GetHeadwayPeriodTuples()

    week_period = transitfeed.ServicePeriod()
    week_period.service_id = "FULLW"
    week_period.start_date = "20070101"
    week_period.end_date = "20071231"
    week_period.SetWeekdayService()
    week_period.SetWeekendService()
    week_period.SetDateHasService("20070604", False)
    schedule.AddServicePeriodObject(week_period)

    weekend_period = transitfeed.ServicePeriod()
    weekend_period.service_id = "WE"
    weekend_period.start_date = "20070101"
    weekend_period.end_date = "20071231"
    weekend_period.SetWeekendService()
    schedule.AddServicePeriodObject(weekend_period)

    fare_data = [
        ("p", 1.25, "USD", 0, 0),
        ("a", 5.25, "USD", 0, 0),
      ]

    fares = []
    for fare_entry in fare_data:
      fare = transitfeed.Fare(fare_entry[0], fare_entry[1], fare_entry[2],
                              fare_entry[3], fare_entry[4])
      fares.append(fare)
      schedule.AddFareObject(fare)

    fare_rule_data = [
        ("p", "AB"),
        ("p", "STBA"),
        ("p", "BFC"),
        ("a", "AAMV"),
      ]

    for fare_rule_entry in fare_rule_data:
      rule = transitfeed.FareRule(fare_rule_entry[0], fare_rule_entry[1])
      schedule.AddFareRuleObject(rule, problems)

    schedule.Validate(problems)
    schedule.WriteGoogleTransitFeed("test-output.zip")

    read_schedule = \
        transitfeed.Loader("test-output.zip", problems=problems,
                           extra_validation=True).Load()

    self.assertEqual(1, len(read_schedule.GetAgencyList()))
    self.assertEqual(agency, read_schedule.GetAgency(agency.agency_id))

    self.assertEqual(len(routes), len(read_schedule.GetRouteList()))
    for route in routes:
      self.assertEqual(route, read_schedule.GetRoute(route.route_id))

    self.assertEqual(2, len(read_schedule.GetServicePeriodList()))
    self.assertEqual(week_period,
                     read_schedule.GetServicePeriod(week_period.service_id))
    self.assertEqual(weekend_period,
                     read_schedule.GetServicePeriod(weekend_period.service_id))

    self.assertEqual(len(stops), len(read_schedule.GetStopList()))
    for stop in stops:
      self.assertEqual(stop, read_schedule.GetStop(stop.stop_id))

    self.assertEqual(len(trips), len(read_schedule.GetTripList()))
    for trip in trips:
      self.assertEqual(trip, read_schedule.GetTrip(trip.trip_id))

    for trip_id in headway_trips:
      self.assertEqual(headway_trips[trip_id],
                       read_schedule.GetTrip(trip_id).GetHeadwayPeriodTuples())

    self.assertEqual(len(fares), len(read_schedule.GetFareList()))
    for fare in fares:
      self.assertEqual(fare, read_schedule.GetFare(fare.fare_id))

    self.assertEqual(1, len(read_schedule.GetShapeList()))
    self.assertEqual(shape, read_schedule.GetShape(shape.shape_id))


class ApproximateDistanceBetweenStopsTestCase(unittest.TestCase):
  def testEquator(self):
    stop1 = transitfeed.Stop(lat=0, lng=100,
                             name='Stop one', stop_id='1')
    stop2 = transitfeed.Stop(lat=0.01, lng=100.01,
                             name='Stop two', stop_id='2')
    self.assertAlmostEqual(
        transitfeed.ApproximateDistanceBetweenStops(stop1, stop2),
        1570, -1)  # Compare first 3 digits

  def testWhati(self):
    stop1 = transitfeed.Stop(lat=63.1, lng=-117.2,
                             name='Stop whati one', stop_id='1')
    stop2 = transitfeed.Stop(lat=63.102, lng=-117.201,
                             name='Stop whati two', stop_id='2')
    self.assertAlmostEqual(
        transitfeed.ApproximateDistanceBetweenStops(stop1, stop2),
        228, 0)

if __name__ == '__main__':
  unittest.main()
