#!/usr/bin/python3

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


from .gtfsfactoryuser import GtfsFactoryUser


class GtfsObjectBase(GtfsFactoryUser):
    """Object with arbitrary attributes which may be added to a schedule.

  This class should be used as the base class for GTFS objects which may
  be stored in a Schedule. It defines some methods for reading and writing
  attributes. If self._schedule is None than the object is not in a Schedule.

  Subclasses must:
  * define an __init__ method which sets the _schedule member to None or a
    weakref to a Schedule
  * set the _TABLE_NAME class variable to a name such as 'stops', 'agency', ...
  * override the _REQUIRED_FIELD_NAMES, _FIELD_NAMES and _DEPRECATED_FIELD_NAMES
    variables according to the needs of the GTFS object
  * define methods to validate objects of that type:
    * ValidateBeforeAdd, which is called before an object is added to a
      Schedule. With the default loader the object is added to the Schedule if
      this function returns True, and is not added if it returns False.
    * ValidateAfterAdd, which is called after an object is added to a Schedule.
      With the default Loader the return value, if any, is not used.

  """

    # list of all required field names for the GTFS object
    _REQUIRED_FIELD_NAMES = []
    # list of all valid field names including the required ones
    _FIELD_NAMES = _REQUIRED_FIELD_NAMES + []
    # list of tuples of all deprecated field names and, optionally, their new name
    # e.g. [('old_name', 'new_name')]
    # use None if there is no new name, e.g. [('old_name', None)]
    _DEPRECATED_FIELD_NAMES = []

    def __getitem__(self, name):
        """Return a unicode or str representation of name or "" if not set."""
        if name in self.__dict__ and self.__dict__[name] is not None:
            return "%s" % self.__dict__[name]
        else:
            return ""

    def __getattr__(self, name):
        """Return None or the default value if name is a known attribute.

    This method is only called when name is not found in __dict__.
    """

        if name in self.__class__._FIELD_NAMES:
            return None
        elif name in [
            dfn[0] for dfn in self.__class__._DEPRECATED_FIELD_NAMES
        ]:
            return None
        else:
            raise AttributeError(name)

    def iteritems(self):
        """Return a iterable for (name, value) pairs of public attributes."""
        for name, value in self.__dict__.items():
            if (not name) or name[0] == "_":
                continue
            yield name, value

    def __setattr__(self, name, value):
        """Set an attribute, adding name to the list of columns as needed."""
        object.__setattr__(self, name, value)
        if name[0] != "_" and self._schedule:
            self._schedule.AddTableColumn(self.__class__._TABLE_NAME, name)

    def __eq__(self, other):
        """Return true iff self and other are equivalent"""
        if not other:
            return False

        if id(self) == id(other):
            return True

        for k in self.keys().union(other.keys()):
            # use __getitem__ which returns "" for missing columns values
            if self[k] != other[k]:
                return False
        return True

    def __ne__(self, other):
        return not self.__eq__(other)

    # TODO(Tom): According to
    # http://docs.python.org/reference/datamodel.html#object.__hash__
    # this class should set '__hash__ = None' because it defines __eq__. This
    # can't be fixed until the merger is changed to not use a/b_merge_map.

    def __repr__(self):
        return "<%s %s>" % (
            self.__class__.__name__,
            sorted(self.__dict__.items()),
        )

    def keys(self):
        """Return iterable of columns used by this object."""
        columns = set()
        for name in vars(self):
            if (not name) or name[0] == "_":
                continue
            columns.add(name)
        return columns

    def _ColumnNames(self):
        return list(self.keys())

    def AddToSchedule(self, schedule, problems):
        self._schedule = schedule
