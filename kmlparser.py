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
This package provides implementation of a converter from a kml
file format into Google transit feed format.

The KmlParser class is the main class implementing the parser.

Currently only information about stops is extracted from a kml file.
The extractor expects the stops to be represented as placemarks with
a single point.
"""

import re
import string
import sys
import transitfeed
import util
import xml.dom.minidom as minidom
import zipfile


class Placemark(object):
  def __init__(self):
    self.name = ""
    self.coordinates = []

  def IsPoint(self):
    return len(self.coordinates) == 1

  def IsLine(self):
    return len(self.coordinates) > 1

class KmlParser(object):
  def __init__(self, stopNameRe = '(.*)'):
    """
    Args:
      stopNameRe - a regular expression to extract a stop name from a
                   placemaker name
    """
    self.stopNameRe = re.compile(stopNameRe)

  def Parse(self, filename, feed):
    """
    Reads the kml file, parses it and updated the Google transit feed
    object with the extracted information.

    Args:
      filename - kml file name
      feed - an instance of Schedule class to be updated
    """
    dom = minidom.parse(filename)
    self.ParseDom(dom, feed)

  def ParseDom(self, dom, feed):
    """
    Parses the given kml dom tree and updates the Google transit feed object.

    Args:
      dom - kml dom tree
      feed - an instance of Schedule class to be updated
    """
    shape_num = 0
    for node in dom.getElementsByTagName('Placemark'):
      p = self.ParsePlacemark(node)
      if p.IsPoint():
        (lon, lat) = p.coordinates[0]
        m = self.stopNameRe.search(p.name)
        feed.AddStop(lat, lon, m.group(1))
      elif p.IsLine():
        shape_num = shape_num + 1
        shape = transitfeed.Shape("kml_shape_" + str(shape_num))
        for (lon, lat) in p.coordinates:
          shape.AddPoint(lat, lon)
        feed.AddShapeObject(shape)

  def ParsePlacemark(self, node):
    ret = Placemark()
    for child in node.childNodes:
      if child.nodeName == 'name':
        ret.name = self.ExtractText(child)
      if child.nodeName == 'Point' or child.nodeName == 'LineString':
        ret.coordinates = self.ExtractCoordinates(child)
    return ret

  def ExtractText(self, node):
    for child in node.childNodes:
      if child.nodeType == child.TEXT_NODE:
        return child.wholeText  # is a unicode string
    return ""

  def ExtractCoordinates(self, node):
    coordinatesText = ""
    for child in node.childNodes:
      if child.nodeName == 'coordinates':
        coordinatesText = self.ExtractText(child)
        break
    ret = []
    for point in coordinatesText.split():
      coords = point.split(',')
      ret.append((float(coords[0]), float(coords[1])))
    return ret


def main():
  usage = \
"""%prog <input.kml> <output GTFS.zip>

Reads KML file <input.kml> and creates GTFS file <output GTFS.zip> with
placemarks in the KML represented as stops.
"""

  parser = util.OptionParserLongError(
      usage=usage, version='%prog '+transitfeed.__version__)
  (options, args) = parser.parse_args()
  if len(args) != 2:
    parser.error('You did not provide all required command line arguments.')

  if args[0] == 'IWantMyCrash':
    raise Exception('For testCrashHandler')

  parser = KmlParser()
  feed = transitfeed.Schedule()
  feed.save_all_stops = True
  parser.Parse(args[0], feed)
  feed.WriteGoogleTransitFeed(args[1])

  print "Done."


if __name__ == '__main__':
  try:
    exit_code = main()
    sys.exit(exit_code)
  except (SystemExit, KeyboardInterrupt):
    raise
  except:
    import inspect
    import sys
    import traceback

    # Save trace and exception now. These calls look at the most recently
    # raised exception. The code that makes the report might trigger other
    # exceptions.
    original_trace = inspect.trace(3)[1:]
    formatted_exception = traceback.format_exception_only(*(sys.exc_info()[:2]))

    apology = """Yikes, the program threw an unexpected exception!

Hopefully a complete report has been saved to transitfeedcrash.txt,
though if you are seeing this message we've already disappointed you once
today. Please include the report in a new issue at
http://code.google.com/p/googletransitdatafeed/issues/entry
or an email to googletransitdatafeed@googlegroups.com. Sorry!

"""
    dashes = '%s\n' % ('-' * 60)
    dump = []
    dump.append(apology)
    dump.append(dashes)
    try:
      dump.append("transitfeed version %s\n\n" % transitfeed.__version__)
    except NameError:
      # Oh well, guess we won't put the version in the report
      pass

    for (frame_obj, filename, line_num, fun_name, context_lines,
         context_index) in original_trace:
      dump.append('File "%s", line %d, in %s\n' % (filename, line_num,
                                                   fun_name))
      if context_lines:
        for (i, line) in enumerate(context_lines):
          if i == context_index:
            dump.append(' --> %s' % line)
          else:
            dump.append('     %s' % line)
      for local_name, local_val in frame_obj.f_locals.items():
        try:
          truncated_val = str(local_val)[0:500]
        except Exception, e:
          dump.append('    Exception in str(%s): %s' % (local_name, e))
        else:
          if len(truncated_val) >= 500:
            truncated_val = '%s...' % truncated_val[0:499]
          dump.append('    %s = %s\n' % (local_name, truncated_val))
      dump.append('\n')

    dump.append(''.join(formatted_exception))

    open('transitfeedcrash.txt', 'w').write(''.join(dump))

    print ''.join(dump)
    print
    print dashes
    print apology

    if '-n' not in sys.argv and '--noprompt' not in sys.argv:
      try:
        raw_input('Press enter to continue')
      except EOFError:
        # Ignore stdin being closed. This happens during some tests.
        pass
    sys.exit(127)
