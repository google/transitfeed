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


from .loader import Loader


class ShapeLoader(Loader):
    """A subclass of Loader that only loads the shapes from a GTFS file."""

    def __init__(self, *args, **kwargs):
        """Initialize a new ShapeLoader object.

    See Loader.__init__ for argument documentation.
    """
        Loader.__init__(self, *args, **kwargs)

    def Load(self):
        self._LoadShapes()
        return self._schedule
