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
This module provides functionality for writing GTFS feeds out in Google Earth
KML format.  Command-line usage:

python kmlwriter.py <input GTFS filename> [<output KML filename>]

If no output filename is specified, the output file will be given the same
name as the feed file (with ".kml" appended) and will be placed in the same
directory as the input feed.
"""

try:
  import xml.etree.ElementTree as ET  # python 2.5
except ImportError, e:
  import elementtree.ElementTree as ET  # older pythons
import os.path
import sys
import transitfeed

KML_NAMESPACE = 'http://earth.google.com/kml/2.1'

class KMLWriter(object):
  """
  This class knows how to write out a transit feed as KML.
  Sample usage:
    KMLWriter().Write(<transitfeed.Schedule object>, <output filename>)
  """

  def _SetIndentation(self, elem, level=0):
    """
    This is the recommended way to cause an ElementTree DOM to be
    prettyprinted on output, as per: http://effbot.org/zone/element-lib.htm

    Run this on the root element before outputting the tree.

    Args:
      elem - the element to start indenting from, usually the document root
      level - current indentation level for recursion
    """

    i = "\n" + level*"  "
    if len(elem):
      if not elem.text or not elem.text.strip():
        elem.text = i + "  "
      for elem in elem:
        self._SetIndentation(elem, level+1)
      if not elem.tail or not elem.tail.strip():
        elem.tail = i
    else:
      if level and (not elem.tail or not elem.tail.strip()):
        elem.tail = i

  def Write(self, schedule, output_file):
    """
    Writes out a feed as KML.
    
    Args:
      schedule - a transitfeed.Schedule object containing the feed to write
      output_file - name of the output KML file, or file object to use
    """

    # Generate the DOM to write
    root = ET.Element('kml')
    root.attrib['xmlns'] = 'http://earth.google.com/kml/2.1'
    doc = ET.SubElement(root, 'Document')
    stops = list(schedule.GetStopList())
    stops.sort(key=lambda x: x.stop_name)
    for stop in stops:
      placemark = ET.SubElement(doc, 'Placemark')
      name = ET.SubElement(placemark, 'name')
      name.text = stop.stop_name
      if stop.stop_desc or stop.stop_url:
        desc = ET.SubElement(placemark, 'description')
        desc_elements = []
        if stop.stop_desc:
          desc_elements.append(stop.stop_desc)
        if stop.stop_url:
          desc_elements.append('<a href="%s">Stop Info Page</a>' %
                               stop.stop_url)
        desc.text = '\n\n'.join(desc_elements)
      point = ET.SubElement(placemark, 'Point')
      coordinates = ET.SubElement(point, 'coordinates')
      coordinates.text = '%.6f,%.6f' % (stop.stop_lon, stop.stop_lat)

    # Make sure we pretty-print
    self._SetIndentation(root)

    # Now write the output
    if isinstance(output_file, file):
      output = output_file
    else:
      output = open(output_file, 'w')
    output.write("""<?xml version="1.0" encoding="UTF-8"?>\n""")
    ET.ElementTree(root).write(output, 'utf-8')

def main():
  if len(sys.argv) < 2:
    print "Usage: python kmlwriter.py <input GTFS filename> [<output KML filename>]"
    sys.exit(1)

  input_path = sys.argv[1]
  if len(sys.argv) >= 3:
    output_path = sys.argv[2]
  else:
    path = os.path.normpath(input_path)
    (feed_dir, feed) = os.path.split(path)
    if '.' in feed:
      feed = feed.rsplit('.', 1)[0]  # strip extension
    output_filename = '%s.kml' % feed
    output_path = os.path.join(feed_dir, output_filename)

  loader = transitfeed.Loader(input_path,
                              problems=transitfeed.ProblemReporter())
  feed = loader.Load()
  print "Writing %s" % output_path
  KMLWriter().Write(feed, output_path)

if __name__ == '__main__':
  main()
