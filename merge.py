#!/usr/bin/python2.5
#
# Copyright 2007 Google Inc. All Rights Reserved.
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

"""A tool for merging two Google Transit feeds.

Given two Google Transit feeds intending to cover two disjoint calendar
intervals, this tool will attempt to produce a single feed by merging as much
of the two feeds together as possible.

For example, most stops remain the same throughout the year. Therefore, many
of the stops given in stops.txt for the first feed represent the same stops
given in the second feed. This tool will try to merge these stops so they
only appear once in the resultant feed.

A note on terminology: The first schedule is referred to as the "old" schedule;
the second as the "new" schedule. The resultant schedule is referred to as
the "merged" schedule. Names of things in the old schedule are variations of
the letter "a" while names of things from the new schedule are variations of
"b". The objects that represents routes, agencies and so on are called
"entities".

usage: merge.py [options] old_feed_path new_feed_path merged_feed_path

Run merge.py --help for a list of the possible options.
"""


__author__ = 'timothy.stranex@gmail.com (Timothy Stranex)'


import datetime
import optparse
import os
import re
import sys
import time
import transitfeed
from transitfeed import util
import webbrowser


# TODO:
# 1. write unit tests that use actual data
# 2. write a proper trip and stop_times merger
# 3. add a serialised access method for stop_times and shapes to transitfeed
# 4. add support for merging schedules which have some service period overlap


def ApproximateDistanceBetweenPoints(pa, pb):
  """Finds the distance between two points on the Earth's surface.

  This is an approximate distance based on assuming that the Earth is a sphere.
  The points are specified by their lattitude and longitude.

  Args:
    pa: the first (lat, lon) point tuple
    pb: the second (lat, lon) point tuple

  Returns:
    The distance as a float in metres.
  """
  alat, alon = pa
  blat, blon = pb
  sa = transitfeed.Stop(lat=alat, lng=alon)
  sb = transitfeed.Stop(lat=blat, lng=blon)
  return transitfeed.ApproximateDistanceBetweenStops(sa, sb)


class Error(Exception):
  """The base exception class for this module."""


class MergeError(Error):
  """An error produced when two entities could not be merged."""


class MergeProblemWithContext(transitfeed.ExceptionWithContext):
  """The base exception class for problem reporting in the merge module.

  Attributes:
    dataset_merger: The DataSetMerger that generated this problem.
    entity_type_name: The entity type of the dataset_merger. This is just
                      dataset_merger.ENTITY_TYPE_NAME.
    ERROR_TEXT: The text used for generating the problem message.
  """

  def __init__(self, dataset_merger, problem_type=transitfeed.TYPE_WARNING,
               **kwargs):
    """Initialise the exception object.

    Args:
      dataset_merger: The DataSetMerger instance that generated this problem.
      problem_type: The problem severity. This should be set to one of the
                    corresponding constants in transitfeed.
      kwargs: Keyword arguments to be saved as instance attributes.
    """
    kwargs['type'] = problem_type
    kwargs['entity_type_name'] = dataset_merger.ENTITY_TYPE_NAME
    transitfeed.ExceptionWithContext.__init__(self, None, None, **kwargs)
    self.dataset_merger = dataset_merger

  def FormatContext(self):
    return "In files '%s'" % self.dataset_merger.FILE_NAME


class SameIdButNotMerged(MergeProblemWithContext):
  ERROR_TEXT = ("There is a %(entity_type_name)s in the old feed with id "
                "'%(id)s' and one from the new feed with the same id but "
                "they could not be merged:")


class CalendarsNotDisjoint(MergeProblemWithContext):
  ERROR_TEXT = ("The service periods could not be merged since they are not "
                "disjoint.")


class MergeNotImplemented(MergeProblemWithContext):
  ERROR_TEXT = ("The feed merger does not currently support merging in this "
                "file. The entries have been duplicated instead.")


class FareRulesBroken(MergeProblemWithContext):
  ERROR_TEXT = ("The feed merger is currently unable to handle fare rules "
                "properly.")


class MergeProblemReporterBase(transitfeed.ProblemReporterBase):
  """The base problem reporter class for the merge module."""

  def SameIdButNotMerged(self, dataset, entity_id, reason):
    self._Report(SameIdButNotMerged(dataset, id=entity_id, reason=reason))

  def CalendarsNotDisjoint(self, dataset):
    self._Report(CalendarsNotDisjoint(dataset,
                                      problem_type=transitfeed.TYPE_ERROR))

  def MergeNotImplemented(self, dataset):
    self._Report(MergeNotImplemented(dataset))

  def FareRulesBroken(self, dataset):
    self._Report(FareRulesBroken(dataset))


class ExceptionProblemReporter(MergeProblemReporterBase):
  """A problem reporter that reports errors by raising exceptions."""

  def __init__(self, raise_warnings=False):
    """Initialise.

    Args:
      raise_warnings: If this is True then warnings are also raised as
                      exceptions.
    """
    MergeProblemReporterBase.__init__(self)
    self._raise_warnings = raise_warnings

  def _Report(self, merge_problem):
    if self._raise_warnings or merge_problem.IsError():
      raise merge_problem


class HTMLProblemReporter(MergeProblemReporterBase):
  """A problem reporter which generates HTML output."""

  def __init__(self):
    """Initialise."""
    MergeProblemReporterBase.__init__(self)
    self._dataset_warnings = {}  # a map from DataSetMergers to their warnings
    self._dataset_errors = {}
    self._warning_count = 0
    self._error_count = 0

  def _Report(self, merge_problem):
    if merge_problem.IsWarning():
      dataset_problems = self._dataset_warnings
      self._warning_count += 1
    else:
      dataset_problems = self._dataset_errors
      self._error_count += 1

    problem_html = '<li>%s</li>' % (
        merge_problem.FormatProblem().replace('\n', '<br>'))
    dataset_problems.setdefault(merge_problem.dataset_merger, []).append(
        problem_html)

  def _GenerateStatsTable(self, feed_merger):
    """Generate an HTML table of merge statistics.

    Args:
      feed_merger: The FeedMerger instance.

    Returns:
      The generated HTML as a string.
    """
    rows = []
    rows.append('<tr><th class="header"/><th class="header">Merged</th>'
                '<th class="header">Copied from old feed</th>'
                '<th class="header">Copied from new feed</th></tr>')
    for merger in feed_merger.GetMergerList():
      stats = merger.GetMergeStats()
      if stats is None:
        continue
      merged, not_merged_a, not_merged_b = stats
      rows.append('<tr><th class="header">%s</th>'
                  '<td class="header">%d</td>'
                  '<td class="header">%d</td>'
                  '<td class="header">%d</td></tr>' %
                  (merger.DATASET_NAME, merged, not_merged_a, not_merged_b))
    return '<table>%s</table>' % '\n'.join(rows)

  def _GenerateSection(self, problem_type):
    """Generate a listing of the given type of problems.

    Args:
      problem_type: The type of problem. This is one of the problem type
                    constants from transitfeed.

    Returns:
      The generated HTML as a string.
    """
    if problem_type == transitfeed.TYPE_WARNING:
      dataset_problems = self._dataset_warnings
      heading = 'Warnings'
    else:
      dataset_problems = self._dataset_errors
      heading = 'Errors'

    if not dataset_problems:
      return ''

    prefix = '<h2 class="issueHeader">%s:</h2>' % heading
    dataset_sections = []
    for dataset_merger, problems in dataset_problems.items():
      dataset_sections.append('<h3>%s</h3><ol>%s</ol>' % (
          dataset_merger.FILE_NAME, '\n'.join(problems)))
    body = '\n'.join(dataset_sections)
    return prefix + body

  def _GenerateSummary(self):
    """Generate a summary of the warnings and errors.

    Returns:
      The generated HTML as a string.
    """
    items = []
    if self._dataset_errors:
      items.append('errors: %d' % self._error_count)
    if self._dataset_warnings:
      items.append('warnings: %d' % self._warning_count)

    if items:
      return '<p><span class="fail">%s</span></p>' % '<br>'.join(items)
    else:
      return '<p><span class="pass">feeds merged successfully</span></p>'

  def WriteOutput(self, output_file, feed_merger,
                  old_feed_path, new_feed_path, merged_feed_path):
    """Write the HTML output to a file.

    Args:
      output_file: The file object that the HTML output will be written to.
      feed_merger: The FeedMerger instance.
      old_feed_path: The path to the old feed file as a string.
      new_feed_path: The path to the new feed file as a string
      merged_feed_path: The path to the merged feed file as a string. This
                        may be None if no merged feed was written.
    """
    if merged_feed_path is None:
      html_merged_feed_path = ''
    else:
      html_merged_feed_path = '<p>Merged feed created: <code>%s</code></p>' % (
          merged_feed_path)

    html_header = """<html>
<head>
<meta http-equiv="Content-Type" content="text/html; charset=UTF-8"/>
<title>Feed Merger Results</title>
<style>
  body {font-family: Georgia, serif; background-color: white}
  .path {color: gray}
  div.problem {max-width: 500px}
  td,th {background-color: khaki; padding: 2px; font-family:monospace}
  td.problem,th.problem {background-color: dc143c; color: white; padding: 2px;
                         font-family:monospace}
  table {border-spacing: 5px 0px; margin-top: 3px}
  h3.issueHeader {padding-left: 1em}
  span.pass {background-color: lightgreen}
  span.fail {background-color: yellow}
  .pass, .fail {font-size: 16pt; padding: 3px}
  ol,.unused {padding-left: 40pt}
  .header {background-color: white; font-family: Georgia, serif; padding: 0px}
  th.header {text-align: right; font-weight: normal; color: gray}
  .footer {font-size: 10pt}
</style>
</head>
<body>
<h1>Feed merger results</h1>
<p>Old feed: <code>%(old_feed_path)s</code></p>
<p>New feed: <code>%(new_feed_path)s</code></p>
%(html_merged_feed_path)s""" % locals()

    html_stats = self._GenerateStatsTable(feed_merger)
    html_summary = self._GenerateSummary()
    html_errors = self._GenerateSection(transitfeed.TYPE_ERROR)
    html_warnings = self._GenerateSection(transitfeed.TYPE_WARNING)

    html_footer = """
<div class="footer">
Generated using transitfeed version %s on %s.
</div>
</body>
</html>""" % (transitfeed.__version__,
              time.strftime('%B %d, %Y at %I:%M %p %Z'))

    output_file.write(transitfeed.EncodeUnicode(html_header))
    output_file.write(transitfeed.EncodeUnicode(html_stats))
    output_file.write(transitfeed.EncodeUnicode(html_summary))
    output_file.write(transitfeed.EncodeUnicode(html_errors))
    output_file.write(transitfeed.EncodeUnicode(html_warnings))
    output_file.write(transitfeed.EncodeUnicode(html_footer))


class ConsoleWarningRaiseErrorProblemReporter(transitfeed.ProblemReporterBase):
  """Problem reporter to use when loading feeds for merge."""

  def _Report(self, e):
    if e.IsError():
      raise e
    else:
      print transitfeed.EncodeUnicode(e.FormatProblem())
      context = e.FormatContext()
      if context:
        print context


def LoadWithoutErrors(path, memory_db):
  """"Return a Schedule object loaded from path; sys.exit for any error."""
  loading_problem_handler = ConsoleWarningRaiseErrorProblemReporter()
  try:
    schedule = transitfeed.Loader(path,
                                  memory_db=memory_db,
                                  problems=loading_problem_handler).Load()
  except transitfeed.ExceptionWithContext, e:
    print >>sys.stderr, (
        "\n\nFeeds to merge must load without any errors.\n"
        "While loading %s the following error was found:\n%s\n%s\n" %
        (path, e.FormatContext(), transitfeed.EncodeUnicode(e.FormatProblem())))
    sys.exit(1)
  return schedule


class DataSetMerger(object):
  """A DataSetMerger is in charge of merging a set of entities.

  This is an abstract class and should be subclassed for each different entity
  type.

  Attributes:
    ENTITY_TYPE_NAME: The name of the entity type like 'agency' or 'stop'.
    FILE_NAME: The name of the file containing this data set like 'agency.txt'.
    DATASET_NAME: A name for the dataset like 'Agencies' or 'Stops'.
  """

  def __init__(self, feed_merger):
    """Initialise.

    Args:
      feed_merger: The FeedMerger.
    """
    self.feed_merger = feed_merger
    self._num_merged = 0
    self._num_not_merged_a = 0
    self._num_not_merged_b = 0

  def _MergeIdentical(self, a, b):
    """Tries to merge two values. The values are required to be identical.

    Args:
      a: The first value.
      b: The second value.

    Returns:
      The trivially merged value.

    Raises:
      MergeError: The values were not identical.
    """
    if a != b:
      raise MergeError("values must be identical ('%s' vs '%s')" %
                       (transitfeed.EncodeUnicode(a),
                        transitfeed.EncodeUnicode(b)))
    return b

  def _MergeIdenticalCaseInsensitive(self, a, b):
    """Tries to merge two strings.

    The string are required to be the same ignoring case. The second string is
    always used as the merged value.

    Args:
      a: The first string.
      b: The second string.

    Returns:
      The merged string. This is equal to the second string.

    Raises:
      MergeError: The strings were not the same ignoring case.
    """
    if a.lower() != b.lower():
      raise MergeError("values must be the same (case insensitive) "
                       "('%s' vs '%s')" % (transitfeed.EncodeUnicode(a),
                                           transitfeed.EncodeUnicode(b)))
    return b

  def _MergeOptional(self, a, b):
    """Tries to merge two values which may be None.

    If both values are not None, they are required to be the same and the
    merge is trivial. If one of the values is None and the other is not None,
    the merge results in the one which is not None. If both are None, the merge
    results in None.

    Args:
      a: The first value.
      b: The second value.

    Returns:
      The merged value.

    Raises:
      MergeError: If both values are not None and are not the same.
    """
    if a and b:
      if a != b:
        raise MergeError("values must be identical if both specified "
                         "('%s' vs '%s')" % (transitfeed.EncodeUnicode(a),
                                             transitfeed.EncodeUnicode(b)))
    return a or b

  def _MergeSameAgency(self, a_agency_id, b_agency_id):
    """Merge agency ids to the corresponding agency id in the merged schedule.

    Args:
      a_agency_id: an agency id from the old schedule
      b_agency_id: an agency id from the new schedule

    Returns:
      The agency id of the corresponding merged agency.

    Raises:
      MergeError: If a_agency_id and b_agency_id do not correspond to the same
                  merged agency.
      KeyError: Either aaid or baid is not a valid agency id.
    """
    a_agency_id = (a_agency_id or
                   self.feed_merger.a_schedule.GetDefaultAgency().agency_id)
    b_agency_id = (b_agency_id or
                   self.feed_merger.b_schedule.GetDefaultAgency().agency_id)
    a_agency = self.feed_merger.a_merge_map[
        self.feed_merger.a_schedule.GetAgency(a_agency_id)]
    b_agency = self.feed_merger.b_merge_map[
        self.feed_merger.b_schedule.GetAgency(b_agency_id)]
    if a_agency != b_agency:
      raise MergeError('agency must be the same')
    return a_agency.agency_id

  def _SchemedMerge(self, scheme, a, b):
    """Tries to merge two entities according to a merge scheme.

    A scheme is specified by a map where the keys are entity attributes and the
    values are merge functions like Merger._MergeIdentical or
    Merger._MergeOptional. The entity is first migrated to the merged schedule.
    Then the attributes are individually merged as specified by the scheme.

    Args:
      scheme: The merge scheme, a map from entity attributes to merge
              functions.
      a: The entity from the old schedule.
      b: The entity from the new schedule.

    Returns:
      The migrated and merged entity.

    Raises:
      MergeError: One of the attributes was not able to be merged.
    """
    migrated = self._Migrate(b, self.feed_merger.b_schedule, False)
    for attr, merger in scheme.items():
      a_attr = getattr(a, attr, None)
      b_attr = getattr(b, attr, None)
      try:
        merged_attr = merger(a_attr, b_attr)
      except MergeError, merge_error:
        raise MergeError("Attribute '%s' could not be merged: %s." % (
            attr, merge_error))
      if migrated is not None:
        setattr(migrated, attr, merged_attr)
    return migrated

  def _MergeSameId(self):
    """Tries to merge entities based on their ids.

    This tries to merge only the entities from the old and new schedules which
    have the same id. These are added into the merged schedule. Entities which
    do not merge or do not have the same id as another entity in the other
    schedule are simply migrated into the merged schedule.

    This method is less flexible than _MergeDifferentId since it only tries
    to merge entities which have the same id while _MergeDifferentId tries to
    merge everything. However, it is faster and so should be used whenever
    possible.

    This method makes use of various methods like _Merge and _Migrate which
    are not implemented in the abstract DataSetMerger class. These method
    should be overwritten in a subclass to allow _MergeSameId to work with
    different entity types.

    Returns:
      The number of merged entities.
    """
    a_not_merged = []
    b_not_merged = []

    for a in self._GetIter(self.feed_merger.a_schedule):
      try:
        b = self._GetById(self.feed_merger.b_schedule, self._GetId(a))
      except KeyError:
        # there was no entity in B with the same id as a
        a_not_merged.append(a)
        continue
      try:
        self._Add(a, b, self._MergeEntities(a, b))
        self._num_merged += 1
      except MergeError, merge_error:
        a_not_merged.append(a)
        b_not_merged.append(b)
        self._ReportSameIdButNotMerged(self._GetId(a), merge_error)

    for b in self._GetIter(self.feed_merger.b_schedule):
      try:
        a = self._GetById(self.feed_merger.a_schedule, self._GetId(b))
      except KeyError:
        # there was no entity in A with the same id as b
        b_not_merged.append(b)

    # migrate the remaining entities
    for a in a_not_merged:
      newid = self._HasId(self.feed_merger.b_schedule, self._GetId(a))
      self._Add(a, None, self._Migrate(a, self.feed_merger.a_schedule, newid))
    for b in b_not_merged:
      newid = self._HasId(self.feed_merger.a_schedule, self._GetId(b))
      self._Add(None, b, self._Migrate(b, self.feed_merger.b_schedule, newid))

    self._num_not_merged_a = len(a_not_merged)
    self._num_not_merged_b = len(b_not_merged)
    return self._num_merged

  def _MergeDifferentId(self):
    """Tries to merge all possible combinations of entities.

    This tries to merge every entity in the old schedule with every entity in
    the new schedule. Unlike _MergeSameId, the ids do not need to match.
    However, _MergeDifferentId is much slower than _MergeSameId.

    This method makes use of various methods like _Merge and _Migrate which
    are not implemented in the abstract DataSetMerger class. These method
    should be overwritten in a subclass to allow _MergeSameId to work with
    different entity types.

    Returns:
      The number of merged entities.
    """
    # TODO: The same entity from A could merge with multiple from B.
    # This should either generate an error or should be prevented from
    # happening.
    for a in self._GetIter(self.feed_merger.a_schedule):
      for b in self._GetIter(self.feed_merger.b_schedule):
        try:
          self._Add(a, b, self._MergeEntities(a, b))
          self._num_merged += 1
        except MergeError:
          continue

    for a in self._GetIter(self.feed_merger.a_schedule):
      if a not in self.feed_merger.a_merge_map:
        self._num_not_merged_a += 1
        newid = self._HasId(self.feed_merger.b_schedule, self._GetId(a))
        self._Add(a, None,
                  self._Migrate(a, self.feed_merger.a_schedule, newid))
    for b in self._GetIter(self.feed_merger.b_schedule):
      if b not in self.feed_merger.b_merge_map:
        self._num_not_merged_b += 1
        newid = self._HasId(self.feed_merger.a_schedule, self._GetId(b))
        self._Add(None, b,
                  self._Migrate(b, self.feed_merger.b_schedule, newid))

    return self._num_merged

  def _ReportSameIdButNotMerged(self, entity_id, reason):
    """Report that two entities have the same id but could not be merged.

    Args:
      entity_id: The id of the entities.
      reason: A string giving a reason why they could not be merged.
    """
    self.feed_merger.problem_reporter.SameIdButNotMerged(self,
                                                         entity_id,
                                                         reason)

  def _GetIter(self, schedule):
    """Returns an iterator of entities for this data set in the given schedule.

    This method usually corresponds to one of the methods from
    transitfeed.Schedule like GetAgencyList() or GetRouteList().

    Note: This method must be overwritten in a subclass if _MergeSameId or
    _MergeDifferentId are to be used.

    Args:
      schedule: Either the old or new schedule from the FeedMerger.

    Returns:
      An iterator of entities.
    """
    raise NotImplementedError()

  def _GetById(self, schedule, entity_id):
    """Returns an entity given its id.

    This method usually corresponds to one of the methods from
    transitfeed.Schedule like GetAgency() or GetRoute().

    Note: This method must be overwritten in a subclass if _MergeSameId or
    _MergeDifferentId are to be used.

    Args:
      schedule: Either the old or new schedule from the FeedMerger.
      entity_id: The id string of the entity.

    Returns:
      The entity with the given id.

    Raises:
      KeyError: There is not entity with the given id.
    """
    raise NotImplementedError()

  def _HasId(self, schedule, entity_id):
    """Check if the schedule has an entity with the given id.

    Args:
      schedule: The transitfeed.Schedule instance to look in.
      entity_id: The id of the entity.

    Returns:
      True if the schedule has an entity with the id or False if not.
    """
    try:
      self._GetById(schedule, entity_id)
      has = True
    except KeyError:
      has = False
    return has

  def _MergeEntities(self, a, b):
    """Tries to merge the two entities.

    Note: This method must be overwritten in a subclass if _MergeSameId or
    _MergeDifferentId are to be used.

    Args:
      a: The entity from the old schedule.
      b: The entity from the new schedule.

    Returns:
      The merged migrated entity.

    Raises:
      MergeError: The entities were not able to be merged.
    """
    raise NotImplementedError()

  def _Migrate(self, entity, schedule, newid):
    """Migrates the entity to the merge schedule.

    This involves copying the entity and updating any ids to point to the
    corresponding entities in the merged schedule. If newid is True then
    a unique id is generated for the migrated entity using the original id
    as a prefix.

    Note: This method must be overwritten in a subclass if _MergeSameId or
    _MergeDifferentId are to be used.

    Args:
      entity: The entity to migrate.
      schedule: The schedule from the FeedMerger that contains ent.
      newid: Whether to generate a new id (True) or keep the original (False).

    Returns:
      The migrated entity.
    """
    raise NotImplementedError()

  def _Add(self, a, b, migrated):
    """Adds the migrated entity to the merged schedule.

    If a and b are both not None, it means that a and b were merged to create
    migrated. If one of a or b is None, it means that the other was not merged
    but has been migrated. This mapping is registered with the FeedMerger.

    Note: This method must be overwritten in a subclass if _MergeSameId or
    _MergeDifferentId are to be used.

    Args:
      a: The original entity from the old schedule.
      b: The original entity from the new schedule.
      migrated: The migrated entity for the merged schedule.
    """
    raise NotImplementedError()

  def _GetId(self, entity):
    """Returns the id of the given entity.

    Note: This method must be overwritten in a subclass if _MergeSameId or
    _MergeDifferentId are to be used.

    Args:
      entity: The entity.

    Returns:
      The id of the entity as a string or None.
    """
    raise NotImplementedError()

  def MergeDataSets(self):
    """Merge the data sets.

    This method is called in FeedMerger.MergeSchedule().

    Note: This method must be overwritten in a subclass.

    Returns:
      A boolean which is False if the dataset was unable to be merged and
      as a result the entire merge should be aborted. In this case, the problem
      will have been reported using the FeedMerger's problem reporter.
    """
    raise NotImplementedError()

  def GetMergeStats(self):
    """Returns some merge statistics.

    These are given as a tuple (merged, not_merged_a, not_merged_b) where
    "merged" is the number of merged entities, "not_merged_a" is the number of
    entities from the old schedule that were not merged and "not_merged_b" is
    the number of entities from the new schedule that were not merged.

    The return value can also be None. This means that there are no statistics
    for this entity type.

    The statistics are only available after MergeDataSets() has been called.

    Returns:
      Either the statistics tuple or None.
    """
    return (self._num_merged, self._num_not_merged_a, self._num_not_merged_b)


class AgencyMerger(DataSetMerger):
  """A DataSetMerger for agencies."""

  ENTITY_TYPE_NAME = 'agency'
  FILE_NAME = 'agency.txt'
  DATASET_NAME = 'Agencies'

  def _GetIter(self, schedule):
    return schedule.GetAgencyList()

  def _GetById(self, schedule, agency_id):
    return schedule.GetAgency(agency_id)

  def _MergeEntities(self, a, b):
    """Merges two agencies.

    To be merged, they are required to have the same id, name, url and
    timezone. The remaining language attribute is taken from the new agency.

    Args:
      a: The first agency.
      b: The second agency.

    Returns:
      The merged agency.

    Raises:
      MergeError: The agencies could not be merged.
    """

    def _MergeAgencyId(a_agency_id, b_agency_id):
      """Merge two agency ids.

      The only difference between this and _MergeIdentical() is that the values
      None and '' are regarded as being the same.

      Args:
        a_agency_id: The first agency id.
        b_agency_id: The second agency id.

      Returns:
        The merged agency id.

      Raises:
        MergeError: The agency ids could not be merged.
      """
      a_agency_id = a_agency_id or None
      b_agency_id = b_agency_id or None
      return self._MergeIdentical(a_agency_id, b_agency_id)

    scheme = {'agency_id': _MergeAgencyId,
              'agency_name': self._MergeIdentical,
              'agency_url': self._MergeIdentical,
              'agency_timezone': self._MergeIdentical}
    return self._SchemedMerge(scheme, a, b)

  def _Migrate(self, entity, schedule, newid):
    a = transitfeed.Agency(field_dict=entity)
    if newid:
      a.agency_id = self.feed_merger.GenerateId(entity.agency_id)
    return a

  def _Add(self, a, b, migrated):
    self.feed_merger.Register(a, b, migrated)
    self.feed_merger.merged_schedule.AddAgencyObject(migrated)

  def _GetId(self, entity):
    return entity.agency_id

  def MergeDataSets(self):
    self._MergeSameId()
    return True


class StopMerger(DataSetMerger):
  """A DataSetMerger for stops.

  Attributes:
    largest_stop_distance: The largest distance allowed between stops that
      will be merged in metres.
  """

  ENTITY_TYPE_NAME = 'stop'
  FILE_NAME = 'stops.txt'
  DATASET_NAME = 'Stops'

  largest_stop_distance = 10.0

  def __init__(self, feed_merger):
    DataSetMerger.__init__(self, feed_merger)
    self._merged = []
    self._a_not_merged = []
    self._b_not_merged = []

  def SetLargestStopDistance(self, distance):
    """Sets largest_stop_distance."""
    self.largest_stop_distance = distance

  def _GetIter(self, schedule):
    return schedule.GetStopList()

  def _GetById(self, schedule, stop_id):
    return schedule.GetStop(stop_id)

  def _MergeEntities(self, a, b):
    """Merges two stops.

    For the stops to be merged, they must have:
      - the same stop_id
      - the same stop_name (case insensitive)
      - the same zone_id
      - locations less than largest_stop_distance apart
    The other attributes can have arbitary changes. The merged attributes are
    taken from the new stop.

    Args:
      a: The first stop.
      b: The second stop.

    Returns:
      The merged stop.

    Raises:
      MergeError: The stops could not be merged.
    """
    distance = transitfeed.ApproximateDistanceBetweenStops(a, b)
    if distance > self.largest_stop_distance:
      raise MergeError("Stops are too far apart: %.1fm "
                       "(largest_stop_distance is %.1fm)." %
                       (distance, self.largest_stop_distance))
    scheme = {'stop_id': self._MergeIdentical,
              'stop_name': self._MergeIdenticalCaseInsensitive,
              'zone_id': self._MergeIdentical,
              'location_type': self._MergeIdentical}
    return self._SchemedMerge(scheme, a, b)

  def _Migrate(self, entity, schedule, newid):
    migrated_stop = transitfeed.Stop(field_dict=entity)
    if newid:
      migrated_stop.stop_id = self.feed_merger.GenerateId(entity.stop_id)
    return migrated_stop

  def _Add(self, a, b, migrated_stop):
    self.feed_merger.Register(a, b, migrated_stop)

    # The migrated_stop will be added to feed_merger.merged_schedule later
    # since adding must be done after the zone_ids have been finalized.
    if a and b:
      self._merged.append((a, b, migrated_stop))
    elif a:
      self._a_not_merged.append((a, migrated_stop))
    elif b:
      self._b_not_merged.append((b, migrated_stop))

  def _GetId(self, entity):
    return entity.stop_id

  def MergeDataSets(self):
    num_merged = self._MergeSameId()
    fm = self.feed_merger

    # now we do all the zone_id and parent_station mapping

    # the zone_ids for merged stops can be preserved
    for (a, b, merged_stop) in self._merged:
      assert a.zone_id == b.zone_id
      fm.a_zone_map[a.zone_id] = a.zone_id
      fm.b_zone_map[b.zone_id] = b.zone_id
      merged_stop.zone_id = a.zone_id
      if merged_stop.parent_station:
        # Merged stop has a parent. Update it to be the parent it had in b.
        parent_in_b = fm.b_schedule.GetStop(b.parent_station)
        merged_stop.parent_station = fm.b_merge_map[parent_in_b].stop_id
      fm.merged_schedule.AddStopObject(merged_stop)

    self._UpdateAndMigrateUnmerged(self._a_not_merged, fm.a_zone_map,
                                   fm.a_merge_map, fm.a_schedule)
    self._UpdateAndMigrateUnmerged(self._b_not_merged, fm.b_zone_map,
                                   fm.b_merge_map, fm.b_schedule)

    print 'Stops merged: %d of %d, %d' % (
        num_merged,
        len(fm.a_schedule.GetStopList()),
        len(fm.b_schedule.GetStopList()))
    return True

  def _UpdateAndMigrateUnmerged(self, not_merged_stops, zone_map, merge_map,
                                schedule):
    """Correct references in migrated unmerged stops and add to merged_schedule.

    For stops migrated from one of the input feeds to the output feed update the
    parent_station and zone_id references to point to objects in the output
    feed. Then add the migrated stop to the new schedule.

    Args:
      not_merged_stops: list of stops from one input feed that have not been
        merged
      zone_map: map from zone_id in the input feed to zone_id in the output feed
      merge_map: map from Stop objects in the input feed to Stop objects in
        the output feed
      schedule: the input Schedule object
    """
    # for the unmerged stops, we use an already mapped zone_id if possible
    # if not, we generate a new one and add it to the map
    for stop, migrated_stop in not_merged_stops:
      if stop.zone_id in zone_map:
        migrated_stop.zone_id = zone_map[stop.zone_id]
      else:
        migrated_stop.zone_id = self.feed_merger.GenerateId(stop.zone_id)
        zone_map[stop.zone_id] = migrated_stop.zone_id
      if stop.parent_station:
        parent_original = schedule.GetStop(stop.parent_station)
        migrated_stop.parent_station = merge_map[parent_original].stop_id
      self.feed_merger.merged_schedule.AddStopObject(migrated_stop)


class RouteMerger(DataSetMerger):
  """A DataSetMerger for routes."""

  ENTITY_TYPE_NAME = 'route'
  FILE_NAME = 'routes.txt'
  DATASET_NAME = 'Routes'

  def _GetIter(self, schedule):
    return schedule.GetRouteList()

  def _GetById(self, schedule, route_id):
    return schedule.GetRoute(route_id)

  def _MergeEntities(self, a, b):
    scheme = {'route_short_name': self._MergeIdentical,
              'route_long_name': self._MergeIdentical,
              'agency_id': self._MergeSameAgency,
              'route_type': self._MergeIdentical,
              'route_id': self._MergeIdentical,
              'route_url': self._MergeOptional,
              'route_color': self._MergeOptional,
              'route_text_color': self._MergeOptional}
    return self._SchemedMerge(scheme, a, b)

  def _Migrate(self, entity, schedule, newid):
    migrated_route = transitfeed.Route(field_dict=entity)
    if newid:
      migrated_route.route_id = self.feed_merger.GenerateId(entity.route_id)
    if entity.agency_id:
      original_agency = schedule.GetAgency(entity.agency_id)
    else:
      original_agency = schedule.GetDefaultAgency()

    migrated_agency = self.feed_merger.GetMergedObject(original_agency)
    migrated_route.agency_id = migrated_agency.agency_id
    return migrated_route

  def _Add(self, a, b, migrated_route):
    self.feed_merger.Register(a, b, migrated_route)
    self.feed_merger.merged_schedule.AddRouteObject(migrated_route)

  def _GetId(self, entity):
    return entity.route_id

  def MergeDataSets(self):
    self._MergeSameId()
    return True


class ServicePeriodMerger(DataSetMerger):
  """A DataSetMerger for service periods.

  Attributes:
    require_disjoint_calendars: A boolean specifying whether to require
      disjoint calendars when merging (True) or not (False).
  """

  ENTITY_TYPE_NAME = 'service period'
  FILE_NAME = 'calendar.txt/calendar_dates.txt'
  DATASET_NAME = 'Service Periods'

  def __init__(self, feed_merger):
    DataSetMerger.__init__(self, feed_merger)
    self.require_disjoint_calendars = True

  def _ReportSameIdButNotMerged(self, entity_id, reason):
    pass

  def _GetIter(self, schedule):
    return schedule.GetServicePeriodList()

  def _GetById(self, schedule, service_id):
    return schedule.GetServicePeriod(service_id)

  def _MergeEntities(self, a, b):
    """Tries to merge two service periods.

    Note: Currently this just raises a MergeError since service periods cannot
    be merged.

    Args:
      a: The first service period.
      b: The second service period.

    Returns:
      The merged service period.

    Raises:
      MergeError: When the service periods could not be merged.
    """
    raise MergeError('Cannot merge service periods')

  def _Migrate(self, original_service_period, schedule, newid):
    migrated_service_period = transitfeed.ServicePeriod()
    migrated_service_period.day_of_week = list(
        original_service_period.day_of_week)
    migrated_service_period.start_date = original_service_period.start_date
    migrated_service_period.end_date = original_service_period.end_date
    migrated_service_period.date_exceptions = dict(
        original_service_period.date_exceptions)
    if newid:
      migrated_service_period.service_id = self.feed_merger.GenerateId(
          original_service_period.service_id)
    else:
      migrated_service_period.service_id = original_service_period.service_id
    return migrated_service_period

  def _Add(self, a, b, migrated_service_period):
    self.feed_merger.Register(a, b, migrated_service_period)
    self.feed_merger.merged_schedule.AddServicePeriodObject(
        migrated_service_period)

  def _GetId(self, entity):
    return entity.service_id

  def MergeDataSets(self):
    if self.require_disjoint_calendars and not self.CheckDisjointCalendars():
      self.feed_merger.problem_reporter.CalendarsNotDisjoint(self)
      return False
    self._MergeSameId()
    self.feed_merger.problem_reporter.MergeNotImplemented(self)
    return True

  def DisjoinCalendars(self, cutoff):
    """Forces the old and new calendars to be disjoint about a cutoff date.

    This truncates the service periods of the old schedule so that service
    stops one day before the given cutoff date and truncates the new schedule
    so that service only begins on the cutoff date.

    Args:
      cutoff: The cutoff date as a string in YYYYMMDD format. The timezone
              is the same as used in the calendar.txt file.
    """

    def TruncatePeriod(service_period, start, end):
      """Truncate the service period to into the range [start, end].

      Args:
        service_period: The service period to truncate.
        start: The start date as a string in YYYYMMDD format.
        end: The end date as a string in YYYYMMDD format.
      """
      service_period.start_date = max(service_period.start_date, start)
      service_period.end_date = min(service_period.end_date, end)
      dates_to_delete = []
      for k in service_period.date_exceptions:
        if (k < start) or (k > end):
          dates_to_delete.append(k)
      for k in dates_to_delete:
        del service_period.date_exceptions[k]

    # find the date one day before cutoff
    year = int(cutoff[:4])
    month = int(cutoff[4:6])
    day = int(cutoff[6:8])
    cutoff_date = datetime.date(year, month, day)
    one_day_delta = datetime.timedelta(days=1)
    before = (cutoff_date - one_day_delta).strftime('%Y%m%d')

    for a in self.feed_merger.a_schedule.GetServicePeriodList():
      TruncatePeriod(a, 0, before)
    for b in self.feed_merger.b_schedule.GetServicePeriodList():
      TruncatePeriod(b, cutoff, '9'*8)

  def CheckDisjointCalendars(self):
    """Check whether any old service periods intersect with any new ones.

    This is a rather coarse check based on
    transitfeed.SevicePeriod.GetDateRange.

    Returns:
      True if the calendars are disjoint or False if not.
    """
    # TODO: Do an exact check here.

    a_service_periods = self.feed_merger.a_schedule.GetServicePeriodList()
    b_service_periods = self.feed_merger.b_schedule.GetServicePeriodList()

    for a_service_period in a_service_periods:
      a_start, a_end = a_service_period.GetDateRange()
      for b_service_period in b_service_periods:
        b_start, b_end = b_service_period.GetDateRange()
        overlap_start = max(a_start, b_start)
        overlap_end = min(a_end, b_end)
        if overlap_end >= overlap_start:
          return False
    return True

  def GetMergeStats(self):
    return None


class FareMerger(DataSetMerger):
  """A DataSetMerger for fares."""

  ENTITY_TYPE_NAME = 'fare'
  FILE_NAME = 'fare_attributes.txt'
  DATASET_NAME = 'Fares'

  def _GetIter(self, schedule):
    return schedule.GetFareList()

  def _GetById(self, schedule, fare_id):
    return schedule.GetFare(fare_id)

  def _MergeEntities(self, a, b):
    """Merges the fares if all the attributes are the same."""
    scheme = {'price': self._MergeIdentical,
              'currency_type': self._MergeIdentical,
              'payment_method': self._MergeIdentical,
              'transfers': self._MergeIdentical,
              'transfer_duration': self._MergeIdentical}
    return self._SchemedMerge(scheme, a, b)

  def _Migrate(self, original_fare, schedule, newid):
    migrated_fare = transitfeed.Fare(
        field_list=original_fare.GetFieldValuesTuple())
    if newid:
      migrated_fare.fare_id = self.feed_merger.GenerateId(
          original_fare.fare_id)
    return migrated_fare

  def _Add(self, a, b, migrated_fare):
    self.feed_merger.Register(a, b, migrated_fare)
    self.feed_merger.merged_schedule.AddFareObject(migrated_fare)

  def _GetId(self, fare):
    return fare.fare_id

  def MergeDataSets(self):
    num_merged = self._MergeSameId()
    print 'Fares merged: %d of %d, %d' % (
        num_merged,
        len(self.feed_merger.a_schedule.GetFareList()),
        len(self.feed_merger.b_schedule.GetFareList()))
    return True


class ShapeMerger(DataSetMerger):
  """A DataSetMerger for shapes.

  In this implementation, merging shapes means just taking the new shape.
  The only conditions for a merge are that the shape_ids are the same and
  the endpoints of the old and new shapes are no further than
  largest_shape_distance apart.

  Attributes:
    largest_shape_distance: The largest distance between the endpoints of two
      shapes allowed for them to be merged in metres.
  """

  ENTITY_TYPE_NAME = 'shape'
  FILE_NAME = 'shapes.txt'
  DATASET_NAME = 'Shapes'

  largest_shape_distance = 10.0

  def SetLargestShapeDistance(self, distance):
    """Sets largest_shape_distance."""
    self.largest_shape_distance = distance

  def _GetIter(self, schedule):
    return schedule.GetShapeList()

  def _GetById(self, schedule, shape_id):
    return schedule.GetShape(shape_id)

  def _MergeEntities(self, a, b):
    """Merges the shapes by taking the new shape.

    Args:
      a: The first transitfeed.Shape instance.
      b: The second transitfeed.Shape instance.

    Returns:
      The merged shape.

    Raises:
      MergeError: If the ids are different or if the endpoints are further
                  than largest_shape_distance apart.
    """
    if a.shape_id != b.shape_id:
      raise MergeError('shape_id must be the same')

    distance = max(ApproximateDistanceBetweenPoints(a.points[0][:2],
                                                    b.points[0][:2]),
                   ApproximateDistanceBetweenPoints(a.points[-1][:2],
                                                    b.points[-1][:2]))
    if distance > self.largest_shape_distance:
      raise MergeError('The shape endpoints are too far away: %.1fm '
                       '(largest_shape_distance is %.1fm)' %
                       (distance, self.largest_shape_distance))

    return self._Migrate(b, self.feed_merger.b_schedule, False)

  def _Migrate(self, original_shape, schedule, newid):
    migrated_shape = transitfeed.Shape(original_shape.shape_id)
    if newid:
      migrated_shape.shape_id = self.feed_merger.GenerateId(
          original_shape.shape_id)
    for (lat, lon, dist) in original_shape.points:
      migrated_shape.AddPoint(lat=lat, lon=lon, distance=dist)
    return migrated_shape

  def _Add(self, a, b, migrated_shape):
    self.feed_merger.Register(a, b, migrated_shape)
    self.feed_merger.merged_schedule.AddShapeObject(migrated_shape)

  def _GetId(self, shape):
    return shape.shape_id

  def MergeDataSets(self):
    self._MergeSameId()
    return True


class TripMerger(DataSetMerger):
  """A DataSetMerger for trips.

  This implementation makes no attempt to merge trips, it simply migrates
  them all to the merged feed.
  """

  ENTITY_TYPE_NAME = 'trip'
  FILE_NAME = 'trips.txt'
  DATASET_NAME = 'Trips'

  def _ReportSameIdButNotMerged(self, trip_id, reason):
    pass

  def _GetIter(self, schedule):
    return schedule.GetTripList()

  def _GetById(self, schedule, trip_id):
    return schedule.GetTrip(trip_id)

  def _MergeEntities(self, a, b):
    """Raises a MergeError because currently trips cannot be merged."""
    raise MergeError('Cannot merge trips')

  def _Migrate(self, original_trip, schedule, newid):
    migrated_trip = transitfeed.Trip(field_dict=original_trip)
    # Make new trip_id first. AddTripObject reports a problem if it conflicts
    # with an existing id.
    if newid:
      migrated_trip.trip_id = self.feed_merger.GenerateId(
          original_trip.trip_id)
    # Need to add trip to schedule before copying stoptimes
    self.feed_merger.merged_schedule.AddTripObject(migrated_trip,
                                                   validate=False)

    if schedule == self.feed_merger.a_schedule:
      merge_map = self.feed_merger.a_merge_map
    else:
      merge_map = self.feed_merger.b_merge_map

    original_route = schedule.GetRoute(original_trip.route_id)
    migrated_trip.route_id = merge_map[original_route].route_id

    original_service_period = schedule.GetServicePeriod(
        original_trip.service_id)
    migrated_trip.service_id = merge_map[original_service_period].service_id

    if original_trip.block_id:
      migrated_trip.block_id = '%s_%s' % (
          self.feed_merger.GetScheduleName(schedule),
          original_trip.block_id)

    if original_trip.shape_id:
      original_shape = schedule.GetShape(original_trip.shape_id)
      migrated_trip.shape_id = merge_map[original_shape].shape_id

    for original_stop_time in original_trip.GetStopTimes():
      migrated_stop_time = transitfeed.StopTime(
          None,
          merge_map[original_stop_time.stop],
          original_stop_time.arrival_time,
          original_stop_time.departure_time,
          original_stop_time.stop_headsign,
          original_stop_time.pickup_type,
          original_stop_time.drop_off_type,
          original_stop_time.shape_dist_traveled,
          original_stop_time.arrival_secs,
          original_stop_time.departure_secs)
      migrated_trip.AddStopTimeObject(migrated_stop_time)

    for headway_period in original_trip.GetHeadwayPeriodTuples():
      migrated_trip.AddHeadwayPeriod(*headway_period)

    return migrated_trip

  def _Add(self, a, b, migrated_trip):
    # Validate now, since it wasn't done in _Migrate
    migrated_trip.Validate(self.feed_merger.merged_schedule.problem_reporter)
    self.feed_merger.Register(a, b, migrated_trip)

  def _GetId(self, trip):
    return trip.trip_id

  def MergeDataSets(self):
    self._MergeSameId()
    self.feed_merger.problem_reporter.MergeNotImplemented(self)
    return True

  def GetMergeStats(self):
    return None


class FareRuleMerger(DataSetMerger):
  """A DataSetMerger for fare rules."""

  ENTITY_TYPE_NAME = 'fare rule'
  FILE_NAME = 'fare_rules.txt'
  DATASET_NAME = 'Fare Rules'

  def MergeDataSets(self):
    """Merge the fare rule datasets.

    The fare rules are first migrated. Merging is done by removing any
    duplicate rules.

    Returns:
      True since fare rules can always be merged.
    """
    rules = set()
    for (schedule, merge_map, zone_map) in ([self.feed_merger.a_schedule,
                                             self.feed_merger.a_merge_map,
                                             self.feed_merger.a_zone_map],
                                            [self.feed_merger.b_schedule,
                                             self.feed_merger.b_merge_map,
                                             self.feed_merger.b_zone_map]):
      for fare in schedule.GetFareList():
        for fare_rule in fare.GetFareRuleList():
          fare_id = merge_map[schedule.GetFare(fare_rule.fare_id)].fare_id
          route_id = (fare_rule.route_id and
                      merge_map[schedule.GetRoute(fare_rule.route_id)].route_id)
          origin_id = (fare_rule.origin_id and
                       zone_map[fare_rule.origin_id])
          destination_id = (fare_rule.destination_id and
                            zone_map[fare_rule.destination_id])
          contains_id = (fare_rule.contains_id and
                         zone_map[fare_rule.contains_id])
          rules.add((fare_id, route_id, origin_id, destination_id,
                     contains_id))
    for fare_rule_tuple in rules:
      migrated_fare_rule = transitfeed.FareRule(*fare_rule_tuple)
      self.feed_merger.merged_schedule.AddFareRuleObject(migrated_fare_rule)

    if rules:
      self.feed_merger.problem_reporter.FareRulesBroken(self)
    print 'Fare Rules: union has %d fare rules' % len(rules)
    return True

  def GetMergeStats(self):
    return None


class FeedMerger(object):
  """A class for merging two whole feeds.

  This class takes two instances of transitfeed.Schedule and uses
  DataSetMerger instances to merge the feeds and produce the resultant
  merged feed.

  Attributes:
    a_schedule: The old transitfeed.Schedule instance.
    b_schedule: The new transitfeed.Schedule instance.
    problem_reporter: The merge problem reporter.
    merged_schedule: The merged transitfeed.Schedule instance.
    a_merge_map: A map from old entities to merged entities.
    b_merge_map: A map from new entities to merged entities.
    a_zone_map: A map from old zone ids to merged zone ids.
    b_zone_map: A map from new zone ids to merged zone ids.
  """

  def __init__(self, a_schedule, b_schedule, merged_schedule,
               problem_reporter=None):
    """Initialise the merger.

    Once this initialiser has been called, a_schedule and b_schedule should
    not be modified.

    Args:
      a_schedule: The old schedule, an instance of transitfeed.Schedule.
      b_schedule: The new schedule, an instance of transitfeed.Schedule.
      problem_reporter: The problem reporter, an instance of
                        transitfeed.ProblemReporterBase. This can be None in
                        which case the ExceptionProblemReporter is used.
    """
    self.a_schedule = a_schedule
    self.b_schedule = b_schedule
    self.merged_schedule = merged_schedule
    self.a_merge_map = {}
    self.b_merge_map = {}
    self.a_zone_map = {}
    self.b_zone_map = {}
    self._mergers = []
    self._idnum = max(self._FindLargestIdPostfixNumber(self.a_schedule),
                      self._FindLargestIdPostfixNumber(self.b_schedule))

    if problem_reporter is not None:
      self.problem_reporter = problem_reporter
    else:
      self.problem_reporter = ExceptionProblemReporter()

  def _FindLargestIdPostfixNumber(self, schedule):
    """Finds the largest integer used as the ending of an id in the schedule.

    Args:
      schedule: The schedule to check.

    Returns:
      The maximum integer used as an ending for an id.
    """
    postfix_number_re = re.compile('(\d+)$')

    def ExtractPostfixNumber(entity_id):
      """Try to extract an integer from the end of entity_id.

      If entity_id is None or if there is no integer ending the id, zero is
      returned.

      Args:
        entity_id: An id string or None.

      Returns:
        An integer ending the entity_id or zero.
      """
      if entity_id is None:
        return 0
      match = postfix_number_re.search(entity_id)
      if match is not None:
        return int(match.group(1))
      else:
        return 0

    id_data_sets = {'agency_id': schedule.GetAgencyList(),
                    'stop_id': schedule.GetStopList(),
                    'route_id': schedule.GetRouteList(),
                    'trip_id': schedule.GetTripList(),
                    'service_id': schedule.GetServicePeriodList(),
                    'fare_id': schedule.GetFareList(),
                    'shape_id': schedule.GetShapeList()}

    max_postfix_number = 0
    for id_name, entity_list in id_data_sets.items():
      for entity in entity_list:
        entity_id = getattr(entity, id_name)
        postfix_number = ExtractPostfixNumber(entity_id)
        max_postfix_number = max(max_postfix_number, postfix_number)
    return max_postfix_number

  def GetScheduleName(self, schedule):
    """Returns a single letter identifier for the schedule.

    This only works for the old and new schedules which return 'a' and 'b'
    respectively. The purpose of such identifiers is for generating ids.

    Args:
      schedule: The transitfeed.Schedule instance.

    Returns:
      The schedule identifier.

    Raises:
      KeyError: schedule is not the old or new schedule.
    """
    return {self.a_schedule: 'a', self.b_schedule: 'b'}[schedule]

  def GenerateId(self, entity_id=None):
    """Generate a unique id based on the given id.

    This is done by appending a counter which is then incremented. The
    counter is initialised at the maximum number used as an ending for
    any id in the old and new schedules.

    Args:
      entity_id: The base id string. This is allowed to be None.

    Returns:
      The generated id.
    """
    self._idnum += 1
    if entity_id:
      return '%s_merged_%d' % (entity_id, self._idnum)
    else:
      return 'merged_%d' % self._idnum

  def Register(self, a, b, migrated_entity):
    """Registers a merge mapping.

    If a and b are both not None, this means that entities a and b were merged
    to produce migrated_entity. If one of a or b are not None, then it means
    it was not merged but simply migrated.

    The effect of a call to register is to update a_merge_map and b_merge_map
    according to the merge.

    Args:
      a: The entity from the old feed or None.
      b: The entity from the new feed or None.
      migrated_entity: The migrated entity.
    """
    if a is not None: self.a_merge_map[a] = migrated_entity
    if b is not None: self.b_merge_map[b] = migrated_entity

  def AddMerger(self, merger):
    """Add a DataSetMerger to be run by Merge().

    Args:
      merger: The DataSetMerger instance.
    """
    self._mergers.append(merger)

  def AddDefaultMergers(self):
    """Adds the default DataSetMergers defined in this module."""
    self.AddMerger(AgencyMerger(self))
    self.AddMerger(StopMerger(self))
    self.AddMerger(RouteMerger(self))
    self.AddMerger(ServicePeriodMerger(self))
    self.AddMerger(FareMerger(self))
    self.AddMerger(ShapeMerger(self))
    self.AddMerger(TripMerger(self))
    self.AddMerger(FareRuleMerger(self))

  def GetMerger(self, cls):
    """Looks for an added DataSetMerger derived from the given class.

    Args:
      cls: A class derived from DataSetMerger.

    Returns:
      The matching DataSetMerger instance.

    Raises:
      LookupError: No matching DataSetMerger has been added.
    """
    for merger in self._mergers:
      if isinstance(merger, cls):
        return merger
    raise LookupError('No matching DataSetMerger found')

  def GetMergerList(self):
    """Returns the list of DataSetMerger instances that have been added."""
    return self._mergers

  def MergeSchedules(self):
    """Merge the schedules.

    This is done by running the DataSetMergers that have been added with
    AddMerger() in the order that they were added.

    Returns:
      True if the merge was successful.
    """
    for merger in self._mergers:
      if not merger.MergeDataSets():
        return False
    return True

  def GetMergedSchedule(self):
    """Returns the merged schedule.

    This will be empty before MergeSchedules() is called.

    Returns:
      The merged schedule.
    """
    return self.merged_schedule

  def GetMergedObject(self, original):
    """Returns an object that represents original in the merged schedule."""
    # TODO: I think this would be better implemented by adding a private
    # attribute to the objects in the original feeds
    merged = (self.a_merge_map.get(original) or
              self.b_merge_map.get(original))
    if merged:
      return merged
    else:
      raise KeyError()


def main():
  """Run the merge driver program."""
  usage = \
"""%prog [options] <input GTFS a.zip> <input GTFS b.zip> <output GTFS.zip>

Merges <input GTFS a.zip> and <input GTFS b.zip> into a new GTFS file
<output GTFS.zip>.
"""

  parser = util.OptionParserLongError(
      usage=usage, version='%prog '+transitfeed.__version__)
  parser.add_option('--cutoff_date',
                    dest='cutoff_date',
                    default=None,
                    help='a transition date from the old feed to the new '
                    'feed in the format YYYYMMDD')
  parser.add_option('--largest_stop_distance',
                    dest='largest_stop_distance',
                    default=StopMerger.largest_stop_distance,
                    help='the furthest distance two stops can be apart and '
                    'still be merged, in metres')
  parser.add_option('--largest_shape_distance',
                    dest='largest_shape_distance',
                    default=ShapeMerger.largest_shape_distance,
                    help='the furthest distance the endpoints of two shapes '
                    'can be apart and the shape still be merged, in metres')
  parser.add_option('--html_output_path',
                    dest='html_output_path',
                    default='merge-results.html',
                    help='write the html output to this file')
  parser.add_option('--no_browser',
                    dest='no_browser',
                    action='store_true',
                    help='prevents the merge results from being opened in a '
                    'browser')
  parser.add_option('-m', '--memory_db', dest='memory_db',  action='store_true',
                    help='Use in-memory sqlite db instead of a temporary file. '
                         'It is faster but uses more RAM.')
  parser.set_defaults(memory_db=False)
  (options, args) = parser.parse_args()

  if len(args) != 3:
    parser.error('You did not provide all required command line arguments.')

  old_feed_path = os.path.abspath(args[0])
  new_feed_path = os.path.abspath(args[1])
  merged_feed_path = os.path.abspath(args[2])

  if old_feed_path.find("IWantMyCrash") != -1:
    # See test/testmerge.py
    raise Exception('For testing the merge crash handler.')

  a_schedule = LoadWithoutErrors(old_feed_path, options.memory_db)
  b_schedule = LoadWithoutErrors(new_feed_path, options.memory_db)
  merged_schedule = transitfeed.Schedule(memory_db=options.memory_db)
  problem_reporter = HTMLProblemReporter()
  feed_merger = FeedMerger(a_schedule, b_schedule, merged_schedule,
                           problem_reporter)
  feed_merger.AddDefaultMergers()

  feed_merger.GetMerger(StopMerger).SetLargestStopDistance(float(
      options.largest_stop_distance))
  feed_merger.GetMerger(ShapeMerger).SetLargestShapeDistance(float(
      options.largest_shape_distance))

  if options.cutoff_date is not None:
    service_period_merger = feed_merger.GetMerger(ServicePeriodMerger)
    service_period_merger.DisjoinCalendars(options.cutoff_date)

  if feed_merger.MergeSchedules():
    feed_merger.GetMergedSchedule().WriteGoogleTransitFeed(merged_feed_path)
  else:
    merged_feed_path = None

  output_file = file(options.html_output_path, 'w')
  problem_reporter.WriteOutput(output_file, feed_merger,
                               old_feed_path, new_feed_path, merged_feed_path)
  output_file.close()

  if not options.no_browser:
    webbrowser.open('file://%s' % os.path.abspath(options.html_output_path))


if __name__ == '__main__':
  util.RunWithCrashHandler(main)
