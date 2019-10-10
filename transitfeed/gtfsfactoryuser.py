#!/usr/bin/python3

# Copyright (C) 2010 Google Inc.
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


class GtfsFactoryUser(object):
    """Base class for objects that must store a GtfsFactory in order to
     be able to instantiate Gtfs classes.

     If a non-default GtfsFactory is to be used, it must be set explicitly."""

    _gtfs_factory = None

    def GetGtfsFactory(self):
        """Return the object's GTFS Factory.

    Returns:
        The GTFS Factory that was set for this object. If none was explicitly
        set, it first sets the object's factory to transitfeed's GtfsFactory
        and returns it"""

        if self._gtfs_factory is None:
            # TODO(anog): We really need to create a dependency graph and clean things
            #            up, as the comment in __init__.py says.
            #            Not having GenericGTFSObject as a leaf (with no other
            #            imports) creates all sorts of circular import problems.
            #            This is why the import is here and not at the top level.
            #            When this runs, gtfsfactory should have already been loaded
            #            by other modules, avoiding the circular imports.
            from . import gtfsfactory

            self._gtfs_factory = gtfsfactory.GetGtfsFactory()
        return self._gtfs_factory

    def SetGtfsFactory(self, factory):
        self._gtfs_factory = factory
