#!/usr/bin/python2.5

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

from genericgtfsobject import GenericGTFSObject
from problems import default_problem_reporter
import util

class Agency(GenericGTFSObject):
  """Represents an agency in a schedule.

  Callers may assign arbitrary values to instance attributes. __init__ makes no
  attempt at validating the attributes. Call Validate() to check that
  attributes are valid and the agency object is consistent with itself.

  Attributes:
    All attributes are strings.
  """
  _REQUIRED_FIELD_NAMES = ['agency_name', 'agency_url', 'agency_timezone']
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ['agency_id', 'agency_lang',
                                          'agency_phone']
  _TABLE_NAME = 'agency'

  def __init__(self, name=None, url=None, timezone=None, id=None,
               field_dict=None, lang=None, **kwargs):
    """Initialize a new Agency object.

    Args:
      field_dict: A dictionary mapping attribute name to unicode string
      name: a string, ignored when field_dict is present
      url: a string, ignored when field_dict is present
      timezone: a string, ignored when field_dict is present
      id: a string, ignored when field_dict is present
      kwargs: arbitrary keyword arguments may be used to add attributes to the
        new object, ignored when field_dict is present
    """
    self._schedule = None

    if not field_dict:
      if name:
        kwargs['agency_name'] = name
      if url:
        kwargs['agency_url'] = url
      if timezone:
        kwargs['agency_timezone'] = timezone
      if id:
        kwargs['agency_id'] = id
      if lang:
        kwargs['agency_lang'] = lang
      field_dict = kwargs

    self.__dict__.update(field_dict)

  def Validate(self, problems=default_problem_reporter):
    """Validate attribute values and this object's internal consistency.

    Returns:
      True iff all validation checks passed.
    """
    found_problem = False
    for required in Agency._REQUIRED_FIELD_NAMES:
      if util.IsEmpty(getattr(self, required, None)):
        problems.MissingValue(required)
        found_problem = True

    if self.agency_url and not util.IsValidURL(self.agency_url):
      problems.InvalidValue('agency_url', self.agency_url)
      found_problem = True

    if (not util.IsEmpty(self.agency_lang) and
        self.agency_lang.lower() not in ISO639.codes_2letter):
      problems.InvalidValue('agency_lang', self.agency_lang)
      found_problem = True

    try:
      import pytz
      if self.agency_timezone not in pytz.common_timezones:
        problems.InvalidValue(
            'agency_timezone',
            self.agency_timezone,
            '"%s" is not a common timezone name according to pytz version %s' %
            (self.agency_timezone, pytz.VERSION))
        found_problem = True
    except ImportError:  # no pytz
      print ("Timezone not checked "
             "(install pytz package for timezone validation)")
    return not found_problem

class ISO639(object):
  # Set of all the 2-letter ISO 639-1 language codes.
  codes_2letter = set([
    'aa', 'ab', 'ae', 'af', 'ak', 'am', 'an', 'ar', 'as', 'av', 'ay', 'az',
    'ba', 'be', 'bg', 'bh', 'bi', 'bm', 'bn', 'bo', 'br', 'bs', 'ca', 'ce',
    'ch', 'co', 'cr', 'cs', 'cu', 'cv', 'cy', 'da', 'de', 'dv', 'dz', 'ee',
    'el', 'en', 'eo', 'es', 'et', 'eu', 'fa', 'ff', 'fi', 'fj', 'fo', 'fr',
    'fy', 'ga', 'gd', 'gl', 'gn', 'gu', 'gv', 'ha', 'he', 'hi', 'ho', 'hr',
    'ht', 'hu', 'hy', 'hz', 'ia', 'id', 'ie', 'ig', 'ii', 'ik', 'io', 'is',
    'it', 'iu', 'ja', 'jv', 'ka', 'kg', 'ki', 'kj', 'kk', 'kl', 'km', 'kn',
    'ko', 'kr', 'ks', 'ku', 'kv', 'kw', 'ky', 'la', 'lb', 'lg', 'li', 'ln',
    'lo', 'lt', 'lu', 'lv', 'mg', 'mh', 'mi', 'mk', 'ml', 'mn', 'mo', 'mr',
    'ms', 'mt', 'my', 'na', 'nb', 'nd', 'ne', 'ng', 'nl', 'nn', 'no', 'nr',
    'nv', 'ny', 'oc', 'oj', 'om', 'or', 'os', 'pa', 'pi', 'pl', 'ps', 'pt',
    'qu', 'rm', 'rn', 'ro', 'ru', 'rw', 'sa', 'sc', 'sd', 'se', 'sg', 'si',
    'sk', 'sl', 'sm', 'sn', 'so', 'sq', 'sr', 'ss', 'st', 'su', 'sv', 'sw',
    'ta', 'te', 'tg', 'th', 'ti', 'tk', 'tl', 'tn', 'to', 'tr', 'ts', 'tt',
    'tw', 'ty', 'ug', 'uk', 'ur', 'uz', 've', 'vi', 'vo', 'wa', 'wo', 'xh',
    'yi', 'yo', 'za', 'zh', 'zu',
  ])
