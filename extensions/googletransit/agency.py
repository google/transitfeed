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
from . import extension_util


class Agency(transitfeed.Agency):
    """Extension of transitfeed.Agency:
  - Overriding ValidateAgencyLang() for supporting BCP-47 agency_lang codes.
  """

    # Overrides transitfeed.Agency.ValidateAgencyLang() and validates agency_lang
    # using the new pybcp47 module via extension_util.py
    def ValidateAgencyLang(self, problems):
        if not self.agency_lang:
            return False
        return not extension_util.ValidateLanguageCode(
            self.agency_lang, "agency_lang", problems
        )
