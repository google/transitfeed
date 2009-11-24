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

"""Expose some modules in this package.

Before transitfeed version 1.2.4 all our library code was distributed in a
one file module, transitfeed.py, and could be used as

import transitfeed
schedule = transitfeed.Schedule()

At that time the module (one file, transitfeed.py) was converted into a
package (a directory named transitfeed containing __init__.py and multiple .py
files). Classes and attributes exposed by the old module may still be imported
in the same way. Indeed, code that depends on the library <em>should</em>
continue to use import commands such as the above and ignore _transitfeed.
"""

from _transitfeed import *

__version__ = _transitfeed.__version__
