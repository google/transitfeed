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

class Transfer(GenericGTFSObject):
  """Represents a transfer in a schedule"""
  _REQUIRED_FIELD_NAMES = ['from_stop_id', 'to_stop_id', 'transfer_type']
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ['min_transfer_time']
  _TABLE_NAME = 'transfers'
  _ID_COLUMNS = ['from_stop_id', 'to_stop_id']

  def __init__(self, schedule=None, from_stop_id=None, to_stop_id=None, transfer_type=None,
               min_transfer_time=None, field_dict=None):
    self._schedule = None
    if field_dict:
      self.__dict__.update(field_dict)
    else:
      self.from_stop_id = from_stop_id
      self.to_stop_id = to_stop_id
      self.transfer_type = transfer_type
      self.min_transfer_time = min_transfer_time

    if getattr(self, 'transfer_type', None) in ("", None):
      # Use the default, recommended transfer, if attribute is not set or blank
      self.transfer_type = 0
    else:
      try:
        self.transfer_type = util.NonNegIntStringToInt(self.transfer_type)
      except (TypeError, ValueError):
        pass

    if hasattr(self, 'min_transfer_time'):
      try:
        self.min_transfer_time = util.NonNegIntStringToInt(self.min_transfer_time)
      except (TypeError, ValueError):
        pass
    else:
      self.min_transfer_time = None
    if schedule is not None:
      # Note from Tom, Nov 25, 2009: Maybe calling __init__ with a schedule
      # should output a DeprecationWarning. A schedule factory probably won't
      # use it and other GenericGTFSObject subclasses don't support it.
      schedule.AddTransferObject(self)

  def ValidateFromStopId(self, problems):
    if util.IsEmpty(self.from_stop_id):
      problems.MissingValue('from_stop_id')
    elif self._schedule:
      if self.from_stop_id not in self._schedule.stops.keys():
        problems.InvalidValue('from_stop_id', self.from_stop_id)

  def ValidateToStopId(self, problems):
    if util.IsEmpty(self.to_stop_id):
      problems.MissingValue('to_stop_id')
    elif self._schedule:
      if self.to_stop_id not in self._schedule.stops.keys():
        problems.InvalidValue('to_stop_id', self.to_stop_id)

  def ValidateTransferType(self, problems):
    if not util.IsEmpty(self.transfer_type):
      if (not isinstance(self.transfer_type, int)) or \
          (self.transfer_type not in range(0, 4)):
        problems.InvalidValue('transfer_type', self.transfer_type)

  def ValidateMinimumTransferTime(self, problems):
    if not util.IsEmpty(self.min_transfer_time):
      if (not isinstance(self.min_transfer_time, int)) or \
          self.min_transfer_time < 0:
        problems.InvalidValue('min_transfer_time', self.min_transfer_time)

  def ValidateBeforeAdd(self, problems):
    self.ValidateFromStopId(problems)
    self.ValidateToStopId(problems)
    self.ValidateTransferType(problems)
    self.ValidateMinimumTransferTime(problems)
    
    # None of these checks are blocking
    return True

  def ValidateAfterAdd(self, problems):
    return

  def Validate(self, problems=default_problem_reporter):
    self.ValidateBeforeAdd(problems)
    self.ValidateAfterAdd(problems)

  def _ID(self):
    return tuple(self[i] for i in self._ID_COLUMNS)

  def AddToSchedule(self, schedule, problems):
    schedule.AddTransferObject(self, problems)