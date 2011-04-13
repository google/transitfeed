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

class FeedInfo(transitfeed.GtfsObjectBase):
  """New class FeedInfo for validating feed_info.txt

  There is a proposal for adding feed_info.txt to the general GTFS spec (see
  https://sites.google.com/site/gtfschanges/spec-changes-summary#feed_info).
  The motivation comes from the fact that the publisher of the feed is sometimes
  a different entity than any of the operators described in agency.txt. There
  are some fields in agency.txt that are really feed-wide rather than
  agency-wide settings and it would be useful for a feed publisher to have an
  identifier to determine which version of their feed is currently being used
  by a client.

  """

  _REQUIRED_FIELD_NAMES = ["feed_publisher_name", "feed_publisher_url",
                           "feed_timezone", "feed_lang"]
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ["feed_valid_from", "feed_valid_until",
                                          "feed_version"]
  _TABLE_NAME = 'feed_info'

  def __init__(self, field_dict=None):
    self._schedule = None
    if field_dict:
      self.__dict__.update(field_dict)

  def ValidateFeedInfoLang(self, problems):
    return not transitfeed.ValidateLanguageCode(self.feed_lang, 'feed_lang',
                                                problems)

  def ValidateFeedInfoTimezone(self, problems):
    return not transitfeed.ValidateTimezone(self.feed_timezone, 'feed_timezone',
                                            problems)

  def ValidateFeedInfoPublisherUrl(self, problems):
    return not transitfeed.ValidateURL(self.feed_publisher_url,
                                       'feed_publisher_url', problems)

  def ValidateDates(self, problems):
    # Both validity dates are currently set to optional, thus they don't have
    # to be provided and it's currently OK to provide one but not the other.
    from_date_valid = transitfeed.ValidateDate(self.feed_valid_from,
                                               'feed_valid_from', problems)

    until_date_valid = transitfeed.ValidateDate(self.feed_valid_until,
                                                'feed_valid_until', problems)

    if (from_date_valid and until_date_valid and
        self.feed_valid_until < self.feed_valid_from):
        problems.InvalidValue('feed_valid_until', self.feed_valid_until,
                              'feed_valid_until %s is earlier than '
                              'feed_valid_from "%s"' %
                              (self.feed_valid_until, self.feed_valid_from))

  def ValidateBeforeAdd(self, problems):
    transitfeed.ValidateRequiredFieldsAreNotEmpty(self,
                                                  self._REQUIRED_FIELD_NAMES,
                                                  problems)
    self.ValidateFeedInfoLang(problems)
    self.ValidateFeedInfoTimezone(problems)
    self.ValidateFeedInfoPublisherUrl(problems)
    self.ValidateDates(problems)
    return True # none of the above validations is blocking

  def ValidateAfterAdd(self, problems):
    # Validation after add is done in extensions.googletransit.Schedule because
    # it has to cross check with other files, e.g. feed_lang vs. agency_lang.
    pass

  def AddToSchedule(self, schedule, problems):
    schedule.AddFeedInfoObject(self, problems)
