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

"""
This script can be used to create a source distribution, binary distribution
or Windows executable files. The output is put in dist/

See
http://code.google.com/p/googletransitdatafeed/wiki/BuildingPythonWindowsExecutables
for help on creating Windows executables.
"""

from distutils.core import setup
import glob
import os.path
from transitfeed import __version__ as VERSION

try:
  import py2exe
  has_py2exe = True
except ImportError, e:
  # Won't be able to generate win32 exe
  has_py2exe = False


# py2exe doesn't automatically include pytz dependency because it is optional
options = {'py2exe': {'packages': ['pytz']}}
scripts = ['feedvalidator.py', 'schedule_viewer.py',
           'kmlparser.py', 'kmlwriter.py']
kwargs = {}

if has_py2exe:
  kwargs['console'] = scripts
  # py2exe seems to ignore package_data and not add marey_graph. This makes it
  # work.
  kwargs['data_files'] = \
      [('schedule_viewer_files',
          glob.glob(os.path.join('gtfsscheduleviewer', 'files', '*')))]
  
setup(
    version=VERSION,
    name='transitfeed',
    url='http://code.google.com/p/googletransitdatafeed/',
    download_url='http://googletransitdatafeed.googlecode.com/'
        'files/transitfeed-%s.tar.gz' % VERSION,
    maintainer='Tom Brown',
    maintainer_email='tom.brown.code@gmail.com',
    description='Google Transit Feed Specification library and tools',
    long_description='This module provides a library for reading, writing and '
        'validating Google Transit Feed Specification files. It includes some '
        'scripts that validate a feed, display it using the Google Maps API and '
        'the start of a KML importer and exporter.',
    platforms='OS Independent',
    license='Apache License, Version 2.0',
    packages=['gtfsscheduleviewer'],
    py_modules=['transitfeed'],
    # Also need to list package_data contents in MANIFEST.in for it to be      
    # included in sdist. See "[Distutils] package_data not used by sdist       
    # command" Feb 2, 2007                                                     
    package_data={'gtfsscheduleviewer': ['files/*']},
    scripts=scripts,
    zip_safe=False,
    classifiers=[
        'Development Status :: 4 - Beta',
        'Intended Audience :: Developers',
        'Intended Audience :: Information Technology',
        'Intended Audience :: Other Audience',
        'License :: OSI Approved :: Apache Software License',
        'Operating System :: OS Independent',
        'Programming Language :: Python',
        'Topic :: Scientific/Engineering :: GIS',
        'Topic :: Software Development :: Libraries :: Python Modules'
        ],
    options=options,
    **kwargs
    )
