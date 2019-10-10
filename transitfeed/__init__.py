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

"""This module is a library to help you create, read and write Google
Transit Feed files. Refer to the feed specification, available at
https://developers.google.com/transit/gtfs/, for a
complete description how the transit feed represents a transit schedule. This
library supports all required parts of the specification but does not yet
support all optional parts. Patches welcome!

Before transitfeed version 1.2.4 all our library code was distributed in a
one file module, transitfeed.py, and could be used as

  import transitfeed
  schedule = transitfeed.Schedule()

At that time the module (one file, transitfeed.py) was converted into a
package (a directory named transitfeed containing __init__.py and multiple .py
files). Classes and attributes exposed by the old module may still be imported
in the same way. Indeed, code that depends on the library <em>should</em>
continue to use import commands such as the above and ignore _transitfeed.

To import the transitfeed module you should do something like:

  import transitfeed
  schedule = transitfeed.Schedule()
  ...

The specification describes several tables such as stops, routes and trips.
In a feed file these are stored as comma separeted value files. This library
represents each row of these tables with a single Python object. This object has
attributes for each value on the row. For example, schedule.AddStop returns a
Stop object which has attributes such as stop_lat and stop_name.

  Schedule: Central object of the parser
  GenericGTFSObject: A base class for each of the objects below
  Route: Represents a single route
  Trip: Represents a single trip
  Stop: Represents a single stop
  ServicePeriod: Represents a single service, a set of dates
  Agency: Represents the agency in this feed
  Transfer: Represents a single transfer rule
  TimeToSecondsSinceMidnight(): Convert HH:MM:SS into seconds since midnight.
  FormatSecondsSinceMidnight(s): Formats number of seconds past midnight into a string
"""

from transitfeed.version import __version__
from .agency import *
from .fareattribute import *
from .farerule import *
from .frequency import *
from .gtfsfactory import *
from .gtfsfactoryuser import *
from .gtfsobjectbase import *
from .loader import *
from .problems import *
from .route import *
from .schedule import *
from .serviceperiod import *
from .shape import *
from .shapelib import *
from .shapeloader import *
from .shapepoint import *
from .stop import *
from .stoptime import *
from .transfer import *
from .trip import *

# util needs to be imported before problems because otherwise the loading order
# of this module is Agency -> Problems -> Util -> Trip and trip tries to
# use problems.default_problem_reporter as a default argument (which fails
# because problems.py isn't fully loaded yet). Loading util first solves this as
# problems.py gets fully loaded right away.
# TODO: Solve this problem cleanly
from .util import *
