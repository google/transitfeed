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
options = {'py2exe': {'packages': ['pytz', 'pybcp47']}}
scripts_for_py2exe = ['feedvalidator.py', 'schedule_viewer.py', 'kmlparser.py',
                      'kmlwriter.py', 'merge.py', 'unusual_trip_filter.py',
                      'location_editor.py', 'feedvalidator_googletransit.py']
# On Nov 23, 2009 Tom Brown said: I'm not confident that we can include a
# working copy of this script in the py2exe distribution because it depends on
# ogr. I do want it included in the source tar.gz.
scripts_for_source_only = ['shape_importer.py']
kwargs = {}

if has_py2exe:
  kwargs['console'] = scripts_for_py2exe
  # py2exe seems to ignore package_data and not add marey_graph. This makes it
  # work.
  kwargs['data_files'] = \
      [('schedule_viewer_files',
          glob.glob(os.path.join('gtfsscheduleviewer', 'files', '*')))]
  options['py2exe'] = {'dist_dir': 'transitfeed-windows-binary-%s' % VERSION}

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
    packages=['gtfsscheduleviewer', 'transitfeed'],
    # Also need to list package_data contents in MANIFEST.in for it to be
    # included in sdist. See "[Distutils] package_data not used by sdist
    # command" Feb 2, 2007
    package_data={'gtfsscheduleviewer': ['files/*']},
    scripts=scripts_for_py2exe + scripts_for_source_only,
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

if has_py2exe:
  # Some data files are not copied automatically by py2exe into the
  # library.zip file. This concerns mainly files which are loaded by modules
  # using pkg_resources.
  import zipfile
  # Open the library.zip file for appending additional files.
  zipfile_path = os.path.join(options['py2exe']['dist_dir'], 'library.zip')
  z = zipfile.ZipFile(zipfile_path, 'a')

  # Sometime between pytz-2008a and pytz-2008i common_timezones started to
  # include only names of zones with a corresponding data file in zoneinfo.
  # pytz installs the zoneinfo directory tree in the same directory
  # as the pytz/__init__.py file. These data files are loaded using
  # pkg_resources.resource_stream. py2exe does not copy this to library.zip so
  # resource_stream can't find the files and common_timezones is empty when
  # read in the py2exe executable.
  # This manually copies zoneinfo into the zip. See also
  # http://code.google.com/p/googletransitdatafeed/issues/detail?id=121
  import pytz
  # Make sure the layout of pytz hasn't changed
  assert (pytz.__file__.endswith('__init__.pyc') or
          pytz.__file__.endswith('__init__.py')), pytz.__file__
  zoneinfo_dir = os.path.join(os.path.dirname(pytz.__file__), 'zoneinfo')
  # '..\\Lib\\pytz\\__init__.py' -> '..\\Lib'
  disk_basedir = os.path.dirname(os.path.dirname(pytz.__file__))
  for absdir, directories, filenames in os.walk(zoneinfo_dir):
    assert absdir.startswith(disk_basedir), (absdir, disk_basedir)
    zip_dir = absdir[len(disk_basedir):]
    for f in filenames:
      z.write(os.path.join(absdir, f), os.path.join(zip_dir, f))

  # The custom pybcp47 module included int the googletransit extension reads
  # from a registry file in the resource path. This manually copies the file
  # language-subtag-registry.txt to the library.zip file.
  import extensions.googletransit.pybcp47 as pybcp47_module
  pybcp47_dir = os.path.join(os.path.dirname(pybcp47_module.__file__))
  disk_basedir = os.path.dirname(os.path.dirname(os.path.dirname(pybcp47_dir)))
  zip_dir = pybcp47_dir[len(disk_basedir):]
  z.write(os.path.join(pybcp47_dir, 'language-subtag-registry.txt'),
          os.path.join(zip_dir, 'language-subtag-registry.txt'))

  # Finally close the library.zip file.
  z.close()
