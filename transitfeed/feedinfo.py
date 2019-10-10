#!/usr/bin/python3

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
    """Model and validation for feed_info.txt."""

    _REQUIRED_FIELD_NAMES = [
        "feed_publisher_name",
        "feed_publisher_url",
        "feed_lang",
    ]
    _FIELD_NAMES = _REQUIRED_FIELD_NAMES + [
        "feed_start_date",
        "feed_end_date",
        "feed_version",
    ]
    _DEPRECATED_FIELD_NAMES = [
        ("feed_valid_from", "feed_start_date"),
        ("feed_valid_until", "feed_end_date"),
        ("feed_timezone", None),
    ]
    _TABLE_NAME = "feed_info"

    def __init__(self, field_dict=None):
        self._schedule = None
        if field_dict:
            self.__dict__.update(field_dict)

    def ValidateFeedInfoLang(self, problems):
        return not transitfeed.ValidateLanguageCode(
            self.feed_lang, "feed_lang", problems
        )

    def ValidateFeedInfoPublisherUrl(self, problems):
        return not transitfeed.ValidateURL(
            self.feed_publisher_url, "feed_publisher_url", problems
        )

    def ValidateDates(self, problems):
        # Both validity dates are currently set to optional, thus they don't have
        # to be provided and it's currently OK to provide one but not the other.
        start_date_valid = transitfeed.ValidateDate(
            self.feed_start_date, "feed_start_date", problems
        )

        end_date_valid = transitfeed.ValidateDate(
            self.feed_end_date, "feed_end_date", problems
        )

        if self.feed_end_date and self.feed_start_date:
            if (
                start_date_valid
                and end_date_valid
                and self.feed_end_date < self.feed_start_date
            ):
                problems.InvalidValue(
                    "feed_end_date",
                    self.feed_end_date,
                    "feed_end_date %s is earlier than "
                    'feed_start_date "%s"'
                    % (self.feed_end_date, self.feed_start_date),
                )

    def ValidateBeforeAdd(self, problems):
        transitfeed.ValidateRequiredFieldsAreNotEmpty(
            self, self._REQUIRED_FIELD_NAMES, problems
        )
        self.ValidateFeedInfoLang(problems)
        self.ValidateFeedInfoPublisherUrl(problems)
        self.ValidateDates(problems)
        return True  # none of the above validations is blocking

    def ValidateAfterAdd(self, problems):
        # Validation after add is done in extensions.googletransit.Schedule because
        # it has to cross check with other files, e.g. feed_lang vs. agency_lang.
        pass

    def AddToSchedule(self, schedule, problems):
        schedule.AddFeedInfoObject(self, problems)
