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


from transitfeed import problems as problems_class
from transitfeed import util
from .pybcp47 import Bcp47LanguageParser

parser = Bcp47LanguageParser()


def IsValidLanguageCode(lang):
    """
  Checks the validity of a language code value:
    - checks whether the code, as lower case, is well formed and valid BCP47
      using the pybcp47 module
  """
    bcp47_obj = parser.ParseLanguage(str(lang.lower()))
    return bcp47_obj.IsWellformed() and bcp47_obj.IsValid()


def ValidateLanguageCode(lang, column_name=None, problems=None):
    """
  Validates a non-required language code value using the pybcp47 module:
    - if invalid adds InvalidValue error (if problems accumulator is provided)
    - distinguishes between 'not well-formed' and 'not valid' and adds error
      reasons accordingly
    - an empty language code is regarded as valid! Otherwise we might end up
      with many duplicate errors because of the required field checks.
    - returns true if the language is valid, false if not well-formed or
      invalid.
  """
    if util.IsEmpty(lang):
        return True
    bcp47_obj = parser.ParseLanguage(str(lang.lower()))
    if not bcp47_obj.wellformed:
        if problems:
            problems.InvalidValue(
                column_name,
                lang,
                'language code "%s" is not well-formed' % lang,
                type=problems_class.TYPE_ERROR,
            )
        return False
    if not bcp47_obj.valid:
        if problems:
            problems.InvalidValue(
                column_name,
                lang,
                'language code "%s" is not valid, parses as: %s'
                % (lang, bcp47_obj),
                type=problems_class.TYPE_WARNING,
            )
        return False
    return True
