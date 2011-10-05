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

import extension_util
import transitfeed
from transitfeed import util

class Agency(transitfeed.Agency):
  """Extension of transitfeed.Agency:
  - Overriding ValidateAgencyLang() for supporting BCP-47 agency_lang codes.
  - Deprecating field 'agency_lang' and 'agency_timezone', new feeds for Google
    Transit should use the fields 'feed_lang' and 'feed_timezone' in
    feed_info.txt instead.
  """

  _REQUIRED_FIELD_NAMES = transitfeed.Agency._REQUIRED_FIELD_NAMES[:]
  _FIELD_NAMES = transitfeed.Agency._FIELD_NAMES[:]

  # Removing the deprecated field names from the inherited field names lists.
  _REQUIRED_FIELD_NAMES.remove('agency_timezone')
  _FIELD_NAMES.remove('agency_timezone')
  _FIELD_NAMES.remove('agency_lang')

  _DEPRECATED_FIELD_NAMES = transitfeed.Agency._DEPRECATED_FIELD_NAMES[:] + [
                            ('agency_lang','feed_info.feed_lang'),
                            ('agency_timezone','feed_info.feed_timezone')]

  # Overrides transitfeed.Agency.ValidateAgencyLang() and validates agency_lang
  # using the new pybcp47 module via extension_util.py
  def ValidateAgencyLang(self, problems):
    if not self.agency_lang:
      return False
    return not extension_util.ValidateLanguageCode(
        self.agency_lang, 'agency_lang', problems)

  # Overrides transitfeed.Agency.ValidateAgencyTimezone()
  def ValidateAgencyTimezone(self, problems):
    if not self.agency_timezone:
      return False
    return not util.ValidateTimezone(
        self.agency_timezone, 'agency_timezone', problems)
