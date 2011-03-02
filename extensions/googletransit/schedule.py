#!/usr/bin/python2.5

# Copyright (C) 2011 Google Inc.
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

import transitfeed
import weakref

class Schedule(transitfeed.Schedule):
  """ extension of transitfeed.Schedule:
      - adding variable to hold the feed_info
      - adding AddFeedInfoObject which is called from FeedInfo.AddToSchedule
      - validates FeedInfo.feed_lang against Agency.agency_lang
      - validates FeedInfo.feed_timezone against Agency.agency_timezone
      - checks that all agencies have the same timezone
      - overrides ValidateFeedStartAndExpirationDates() in order to take
        FeedInfo.feed_valid_from and FeedInfo.feed_valid_until into account
  """

  def __init__(self, problem_reporter=None,
               memory_db=True, check_duplicate_trips=False,
               gtfs_factory=None):
    super(Schedule, self).__init__(problem_reporter, memory_db,
                                   check_duplicate_trips, gtfs_factory)
    self.feed_info = None

  def AddFeedInfoObject(self, feed_info, problem_reporter=None, validate=False):
    assert feed_info._schedule is None

    if not problem_reporter:
      problem_reporter = self.problem_reporter

    feed_info._schedule = weakref.proxy(self)

    if validate:
      feed_info.Validate(problem_reporter)
    self.feed_info = feed_info

  def ValidateFeedInfoLangMatchesAgencyLang(self, problems):
    if self.feed_info is None:
      return
    if self.feed_info.feed_lang is None:
      return
    agencies = self.GetAgencyList()
    for agency in agencies:
      if (not transitfeed.IsEmpty(agency.agency_lang)) and (
          not self.feed_info.feed_lang == agency.agency_lang):
        problems.InvalidValue("feed_lang",
                              "The languages specified in feedinfo.txt and in "
                              "agency.txt for agency with ID %s differ." %
                              agency.agency_id)

  def ValidateFeedInfoTimezoneMatchesAgencyTimezone(self, problems):
    if self.feed_info is None:
      return
    if self.feed_info.feed_timezone is None:
      return
    agencies = self.GetAgencyList()
    for agency in agencies:
      if not self.feed_info.feed_timezone == agency.agency_timezone:
        problems.InvalidValue("feed_timezone",
                              "Only one timezone per feed is supported. " \
                              "However, the timezone %s specified in " \
                              "feedinfo.txt does not match the timezone %s " \
                              "specified in agency.txt for agency with ID %s." %
                              (self.feed_info.feed_timezone,
                               agency.agency_timezone, agency.agency_id))

  def ValidateAgenciesHaveSameAgencyTimezone(self, problems):
    timezones_set = set(map(lambda agency:agency.agency_timezone,
                            self.GetAgencyList()))
    if len(timezones_set) > 1:
      timezones_str = '"%s"' % ('", "'.join(timezones_set))
      problems.InvalidValue('agency_timezone', timezones_str,
                            'All agencies should have the same time zone. ' \
                            'Please review agency.txt.')

  def ValidateFeedStartAndExpirationDates(self, problems, first_date, last_date,
                                          first_date_origin, last_date_origin,
                                          today):
    """extends transitfeed.Schedule.ValidateFeedStartAndExpirationDates in order
    to validate the start and expiration dates combined with the feed_valid_from
    and feed_valid_until dates in FeedInfo.
    """
    if self.feed_info and self.feed_info.feed_valid_from:
      feed_valid_from = transitfeed.DateStringToDateObject(
          self.feed_info.feed_valid_from)
      if feed_valid_from and feed_valid_from > first_date:
        first_date = feed_valid_from
        first_date_origin = "feed_valid_from date in feed_info.txt"

    if self.feed_info and self.feed_info.feed_valid_until:
      feed_valid_until = transitfeed.DateStringToDateObject(
          self.feed_info.feed_valid_until)
      if feed_valid_until and feed_valid_until < last_date:
        last_date = feed_valid_until
        last_date_origin = "feed_valid_until date in feed_info.txt"

    super(Schedule, self).ValidateFeedStartAndExpirationDates(
        problems, first_date, last_date, first_date_origin, last_date_origin,
        today)

  def Validate(self,
               problems=None,
               validate_children=True,
               today=None,
               service_gap_interval=None):
    if not problems:
      problems = self.problem_reporter

    super(Schedule, self).Validate(problems, validate_children, today,
                                   service_gap_interval)

    self.ValidateFeedInfoTimezoneMatchesAgencyTimezone(problems)
    self.ValidateFeedInfoLangMatchesAgencyLang(problems)
    self.ValidateAgenciesHaveSameAgencyTimezone(problems)
