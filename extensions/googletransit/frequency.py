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

class Frequency(transitfeed.Frequency):
  """Extension of transitfeed.Frequency:
  - Adding field 'exact_times' and ValidateExactTimes() function
  - See proposal at
    https://sites.google.com/site/gtfschanges/spec-changes-summary#frequencies
  """

  _FIELD_NAMES = transitfeed.Frequency._FIELD_NAMES + [ 'exact_times' ]

  def __init__(self, field_dict=None):
    super(Frequency, self).__init__(field_dict)
    if not field_dict:
      return
    self._exact_times = field_dict.setdefault('exact_times')

  def ExactTimes(self):
    return self._exact_times

  def ValidateExactTimes(self, problems):
    if transitfeed.IsEmpty(self._exact_times):
      self._exact_times = 0
      return
    try:
      self._exact_times = int(self._exact_times)
    except (ValueError, TypeError):
      problems.InvalidValue('exact_times', self._exact_times,
                            'Should be 0 (no fixed schedule) or 1 (fixed and ' \
                            'regular schedule, shortcut for a repetitive ' \
                            'stop_times file).')
      del self._exact_times
      return
    if self._exact_times not in (0, 1):
      problems.InvalidValue('exact_times', self._exact_times,
                            'Should be 0 (no fixed schedule) or 1 (fixed and ' \
                            'regular schedule, shortcut for a repetitive ' \
                            'stop_times file).')

  def ValidateBeforeAdd(self, problems):
    self.ValidateExactTimes(problems)
    return super(Frequency, self).ValidateBeforeAdd(problems)