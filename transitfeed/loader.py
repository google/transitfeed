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


import codecs
import csv
import os
import re
import zipfile

from . import gtfsfactoryuser
from . import problems
from . import util
from io import BytesIO


class Loader:
    def __init__(
        self,
        feed_path=None,
        schedule=None,
        problems=problems.default_problem_reporter,
        extra_validation=False,
        load_stop_times=True,
        memory_db=True,
        zip=None,
        check_duplicate_trips=False,
        gtfs_factory=None,
    ):
        """Initialize a new Loader object.

        Args:
          feed_path: string path to a zip file or directory
          schedule: a Schedule object or None to have one created
          problems: a ProblemReporter object, the default reporter raises an
            exception for each problem
          extra_validation: True if you would like extra validation
          load_stop_times: load the stop_times table, used to speed load time when
            times are not needed. The default is True.
          memory_db: if creating a new Schedule object use an in-memory sqlite
            database instead of creating one in a temporary file
          zip: a zipfile.ZipFile object, optionally used instead of path
        """
        if gtfs_factory is None:
            gtfs_factory = gtfsfactoryuser.GtfsFactoryUser().GetGtfsFactory()

        if not schedule:
            schedule = gtfs_factory.Schedule(
                problem_reporter=problems,
                memory_db=memory_db,
                check_duplicate_trips=check_duplicate_trips,
            )

        self._extra_validation = extra_validation
        self._schedule = schedule
        self._problems = problems
        self._path = feed_path
        self._zip = zip
        self._load_stop_times = load_stop_times
        self._gtfs_factory = gtfs_factory

    def _DetermineFormat(self):
        """Determines whether the feed is in a form that we understand, and
           if so, returns True."""
        if self._zip:
            # If zip was passed to __init__ then path isn't used
            assert not self._path
            return True

        if not isinstance(self._path, str) and hasattr(self._path, "read"):
            # A file-like object, used for testing with a StringIO file
            self._zip = zipfile.ZipFile(self._path, mode="r")
            return True

        if not os.path.exists(self._path):
            self._problems.FeedNotFound(self._path)
            return False

        if self._path.endswith(".zip"):
            try:
                self._zip = zipfile.ZipFile(self._path, mode="r")
            except IOError:  # self._path is a directory
                pass
            except zipfile.BadZipfile:
                self._problems.UnknownFormat(self._path)
                return False

        if not self._zip and not os.path.isdir(self._path):
            self._problems.UnknownFormat(self._path)
            return False

        return True

    def _GetFileNames(self):
        """Returns a list of file names in the feed."""
        if self._zip:
            return self._zip.namelist()
        else:
            return os.listdir(self._path)

    def _CheckFileNames(self):
        filenames = self._GetFileNames()
        known_filenames = self._gtfs_factory.GetKnownFilenames()
        for feed_file in filenames:
            if feed_file not in known_filenames:
                if not feed_file.startswith("."):
                    # Don't worry about .svn files and other hidden files
                    # as this will break the tests.
                    self._problems.UnknownFile(feed_file)

    def _GetUtf8Contents(self, file_name):
        """Check for errors in file_name and return a string for csv reader."""
        contents = self._FileContents(file_name)
        if not contents:  # Missing file
            return

        # Check for errors that will prevent csv.reader from working
        if len(contents) >= 2 and contents[0:2] in (
            codecs.BOM_UTF16_BE,
            codecs.BOM_UTF16_LE,
        ):
            self._problems.FileFormat(
                "appears to be encoded in utf-16", (file_name,)
            )
            # Convert and continue, so we can find more errors
            contents = codecs.getdecoder("utf-16")(contents)[0].encode("utf-8")

        null_index = contents.find(b"\0")
        if null_index != -1:
            # It is easier to get some surrounding text than calculate the exact
            # row_num
            m = re.search(b".{,20}\0.{,20}", contents, re.DOTALL)
            self._problems.FileFormat(
                'contains a null in text "%s" at byte %d'
                % (m.group(), null_index + 1),
                (file_name,),
            )
            return

        # strip out any UTF-8 Byte Order Marker (otherwise it'll be
        # treated as part of the first column name, causing a mis-parse)
        contents = contents.lstrip(codecs.BOM_UTF8)
        return contents

    def _ReadCsvDict(self, file_name, cols, required, deprecated):
        """Reads lines from file_name, yielding a dict of unicode values."""
        assert file_name.endswith(".txt")
        table_name = file_name[0:-4]
        contents = self._GetUtf8Contents(file_name)
        if not contents:
            return

        eol_checker = util.EndOfLineChecker(
            BytesIO(contents), file_name, self._problems
        )
        # The csv module doesn't provide a way to skip trailing space, but when I
        # checked 15/675 feeds had trailing space in a header row and 120 had spaces
        # after fields. Space after header fields can cause a serious parsing
        # problem, so warn. Space after body fields can cause a problem time,
        # integer and id fields; they will be validated at higher levels.
        reader = csv.reader(eol_checker, skipinitialspace=True)

        raw_header = next(reader)
        header_occurrences = util.defaultdict(lambda: 0)
        header = []
        valid_columns = []  # Index into raw_header and raw_row
        for i, h in enumerate(raw_header):
            h_stripped = h.strip()
            if not h_stripped:
                self._problems.CsvSyntax(
                    description="The header row should not contain any blank values. "
                    "The corresponding column will be skipped for the "
                    "entire file.",
                    context=(file_name, 1, [""] * len(raw_header), raw_header),
                    type=problems.TYPE_ERROR,
                )
                continue
            elif h != h_stripped:
                self._problems.CsvSyntax(
                    description="The header row should not contain any "
                    "space characters.",
                    context=(file_name, 1, [""] * len(raw_header), raw_header),
                    type=problems.TYPE_WARNING,
                )
            header.append(h_stripped)
            valid_columns.append(i)
            header_occurrences[h_stripped] += 1

        for name, count in list(header_occurrences.items()):
            if count > 1:
                self._problems.DuplicateColumn(
                    header=name, file_name=file_name, count=count
                )

        self._schedule._table_columns[table_name] = header

        # check for unrecognized columns, which are often misspellings
        header_context = (file_name, 1, [""] * len(header), header)
        valid_cols = cols + [
            deprecated_name for (deprecated_name, _) in deprecated
        ]
        unknown_cols = set(header) - set(valid_cols)
        if len(unknown_cols) == len(header):
            self._problems.CsvSyntax(
                description="The header row did not contain any known column "
                "names. The file is most likely missing the header row "
                "or not in the expected CSV format.",
                context=(file_name, 1, [""] * len(raw_header), raw_header),
                type=problems.TYPE_ERROR,
            )
        else:
            for col in unknown_cols:
                # this is provided in order to create a nice colored list of
                # columns in the validator output
                self._problems.UnrecognizedColumn(
                    file_name, col, header_context
                )

        # check for missing required columns
        missing_cols = set(required) - set(header)
        for col in missing_cols:
            # this is provided in order to create a nice colored list of
            # columns in the validator output
            self._problems.MissingColumn(file_name, col, header_context)

        # check for deprecated columns
        for (deprecated_name, new_name) in deprecated:
            if deprecated_name in header:
                self._problems.DeprecatedColumn(
                    file_name, deprecated_name, new_name, header_context
                )

        line_num = 1  # First line read by reader.next() above
        for raw_row in reader:
            line_num += 1
            if len(raw_row) == 0:  # skip extra empty lines in file
                continue

            if len(raw_row) > len(raw_header):
                self._problems.OtherProblem(
                    "Found too many cells (commas) in line "
                    '%d of file "%s".  Every row in the file '
                    "should have the same number of cells as "
                    "the header (first line) does." % (line_num, file_name),
                    (file_name, line_num),
                    type=problems.TYPE_WARNING,
                )

            if len(raw_row) < len(raw_header):
                self._problems.OtherProblem(
                    "Found missing cells (commas) in line "
                    '%d of file "%s".  Every row in the file '
                    "should have the same number of cells as "
                    "the header (first line) does." % (line_num, file_name),
                    (file_name, line_num),
                    type=problems.TYPE_WARNING,
                )

            # raw_row is a list of raw bytes which should be valid utf-8. Convert each
            # valid_columns of raw_row into Unicode.
            valid_values = []
            unicode_error_columns = (
                []
            )  # index of valid_values elements with an error
            for i in valid_columns:
                try:
                    valid_values.append(raw_row[i])
                    # check bad characters
                    bad_characters = (
                        raw_row[i].encode("utf-8").find(b"\xef\xbf\xbd")
                    )
                    if bad_characters >= 0:
                        raise UnicodeDecodeError(
                            "utf-8",
                            raw_row[i].encode("utf-8"),
                            bad_characters,
                            bad_characters + 1,
                            "",
                        )
                except UnicodeDecodeError:
                    unicode_error_columns.append(len(valid_values) - 1)
                except IndexError:
                    break

            # The error report may contain a dump of all values in valid_values so
            # problems can not be reported until after converting all of raw_row to
            # Unicode.
            for i in unicode_error_columns:
                self._problems.InvalidValue(
                    header[i],
                    valid_values[i],
                    "Unicode error",
                    (file_name, line_num, valid_values, header),
                )

            # We strip ALL whitespace from around values.  This matches the behavior
            # of both the Google and OneBusAway GTFS parser.
            valid_values = [value.strip() for value in valid_values]

            d = dict(list(zip(header, valid_values)))
            yield (d, line_num, header, valid_values)

    # TODO: Add testing for this specific function
    def _ReadCSV(self, file_name, cols, required, deprecated):
        """Reads lines from file_name, yielding a list of unicode values
        corresponding to the column names in cols."""
        contents = self._GetUtf8Contents(file_name)
        if not contents:
            return

        eol_checker = util.EndOfLineChecker(
            BytesIO(contents), file_name, self._problems
        )
        reader = csv.reader(eol_checker)  # Use excel dialect

        header = next(reader)
        header = [x.strip() for x in header]  # trim any whitespace
        header_occurrences = util.defaultdict(lambda: 0)
        for column_header in header:
            header_occurrences[column_header] += 1

        for name, count in list(header_occurrences.items()):
            if count > 1:
                self._problems.DuplicateColumn(
                    header=name, file_name=file_name, count=count
                )

        # check for unrecognized columns, which are often misspellings
        header_context = (file_name, 1, [""] * len(header), header)
        valid_cols = cols + [
            deprecated_name for (deprecated_name, _) in deprecated
        ]
        unknown_cols = set(header).difference(set(valid_cols))
        for col in unknown_cols:
            # this is provided in order to create a nice colored list of
            # columns in the validator output
            self._problems.UnrecognizedColumn(file_name, col, header_context)

        # check for missing required columns
        col_index = [-1] * len(cols)
        for i in range(len(cols)):
            if cols[i] in header:
                col_index[i] = header.index(cols[i])
            elif cols[i] in required:
                self._problems.MissingColumn(
                    file_name, cols[i], header_context
                )

        # check for deprecated columns
        for (deprecated_name, new_name) in deprecated:
            if deprecated_name in header:
                self._problems.DeprecatedColumn(
                    file_name, deprecated_name, new_name, header_context
                )

        row_num = 1
        for row in reader:
            row_num += 1
            if len(row) == 0:  # skip extra empty lines in file
                continue

            if len(row) > len(header):
                self._problems.OtherProblem(
                    "Found too many cells (commas) in line "
                    '%d of file "%s".  Every row in the file '
                    "should have the same number of cells as "
                    "the header (first line) does." % (row_num, file_name),
                    (file_name, row_num),
                    type=problems.TYPE_WARNING,
                )

            if len(row) < len(header):
                self._problems.OtherProblem(
                    "Found missing cells (commas) in line "
                    '%d of file "%s".  Every row in the file '
                    "should have the same number of cells as "
                    "the header (first line) does." % (row_num, file_name),
                    (file_name, row_num),
                    type=problems.TYPE_WARNING,
                )

            result = [None] * len(cols)
            unicode_error_columns = (
                []
            )  # A list of column numbers with an error
            for i in range(len(cols)):
                ci = col_index[i]
                if ci >= 0:
                    if len(row) <= ci:  # handle short CSV rows
                        result[i] = ""
                    else:
                        try:
                            test_string = row[ci].strip()
                            bad_characters = test_string.encode("utf-8").find(
                                b"\xef\xbf\xbd"
                            )
                            result[i] = test_string
                            if bad_characters >= 0:
                                raise UnicodeDecodeError(
                                    "utf-8",
                                    test_string.encode("utf-8"),
                                    bad_characters,
                                    bad_characters + 1,
                                    "",
                                )
                        except UnicodeDecodeError:
                            unicode_error_columns.append(i)

            for i in unicode_error_columns:
                self._problems.InvalidValue(
                    cols[i],
                    result[i],
                    "Unicode error",
                    (file_name, row_num, result, cols),
                )
            yield (result, row_num, cols)

    def _HasFile(self, file_name):
        """Returns True if there's a file in the current feed with the
           given file_name in the current feed."""
        if self._zip:
            return file_name in self._zip.namelist()
        else:
            file_path = os.path.join(self._path, file_name)
            return os.path.exists(file_path) and os.path.isfile(file_path)

    def _FileContents(self, file_name):
        results = None
        if self._zip:
            try:
                results = self._zip.read(file_name)
            except KeyError:  # file not found in archve
                self._problems.MissingFile(file_name)
                return None
        else:
            try:
                data_file = open(os.path.join(self._path, file_name), "rb")
                results = data_file.read()
            except IOError:  # file not found
                self._problems.MissingFile(file_name)
                return None

        if not results:
            self._problems.EmptyFile(file_name)
        return results

    def _LoadFeed(self):
        loading_order = self._gtfs_factory.GetLoadingOrder()
        for filename in loading_order:
            if not self._gtfs_factory.IsFileRequired(
                filename
            ) and not self._HasFile(filename):
                pass  # File is not required, and feed does not have it.
            else:
                object_class = self._gtfs_factory.GetGtfsClassByFileName(
                    filename
                )
                for (d, row_num, header, row) in self._ReadCsvDict(
                    filename,
                    object_class._FIELD_NAMES,
                    object_class._REQUIRED_FIELD_NAMES,
                    object_class._DEPRECATED_FIELD_NAMES,
                ):
                    self._problems.SetFileContext(
                        filename, row_num, row, header
                    )
                    instance = object_class(field_dict=d)
                    instance.SetGtfsFactory(self._gtfs_factory)
                    if not instance.ValidateBeforeAdd(self._problems):
                        continue
                    instance.AddToSchedule(self._schedule, self._problems)
                    instance.ValidateAfterAdd(self._problems)
                    self._problems.ClearContext()

    def _LoadCalendar(self):
        file_name = "calendar.txt"
        file_name_dates = "calendar_dates.txt"
        if not self._HasFile(file_name) and not self._HasFile(file_name_dates):
            self._problems.MissingFile(file_name)
            return

        # map period IDs to (period object, (file_name, row_num, row, cols))
        periods = {}

        service_period_class = self._gtfs_factory.ServicePeriod

        # process calendar.txt
        if self._HasFile(file_name):
            has_useful_contents = False
            for (row, row_num, cols) in self._ReadCSV(
                file_name,
                service_period_class._FIELD_NAMES,
                service_period_class._REQUIRED_FIELD_NAMES,
                service_period_class._DEPRECATED_FIELD_NAMES,
            ):
                context = (file_name, row_num, row, cols)
                self._problems.SetFileContext(*context)

                period = service_period_class(field_list=row)

                if period.service_id in periods:
                    self._problems.DuplicateID("service_id", period.service_id)
                else:
                    periods[period.service_id] = (period, context)
                self._problems.ClearContext()

        # process calendar_dates.txt
        if self._HasFile(file_name_dates):
            # ['service_id', 'date', 'exception_type']
            for (row, row_num, cols) in self._ReadCSV(
                file_name_dates,
                service_period_class._FIELD_NAMES_CALENDAR_DATES,
                service_period_class._REQUIRED_FIELD_NAMES_CALENDAR_DATES,
                service_period_class._DEPRECATED_FIELD_NAMES_CALENDAR_DATES,
            ):
                context = (file_name_dates, row_num, row, cols)
                self._problems.SetFileContext(*context)

                service_id = row[0]

                period = None
                if service_id in periods:
                    period = periods[service_id][0]
                else:
                    period = service_period_class(service_id)
                    periods[period.service_id] = (period, context)

                exception_type = row[2]
                if exception_type == "1":
                    period.SetDateHasService(row[1], True, self._problems)
                elif exception_type == "2":
                    period.SetDateHasService(row[1], False, self._problems)
                else:
                    self._problems.InvalidValue(
                        "exception_type", exception_type
                    )
                self._problems.ClearContext()

        # Now insert the periods into the schedule object, so that they're
        # validated with both calendar and calendar_dates info present
        for period, context in list(periods.values()):
            self._problems.SetFileContext(*context)
            self._schedule.AddServicePeriodObject(period, self._problems)
            self._problems.ClearContext()

    def _LoadShapes(self):
        file_name = "shapes.txt"
        if not self._HasFile(file_name):
            return
        shapes = {}  # shape_id to shape object

        shape_class = self._gtfs_factory.Shape

        for (d, row_num, header, row) in self._ReadCsvDict(
            file_name,
            shape_class._FIELD_NAMES,
            shape_class._REQUIRED_FIELD_NAMES,
            shape_class._DEPRECATED_FIELD_NAMES,
        ):
            file_context = (file_name, row_num, row, header)
            self._problems.SetFileContext(*file_context)

            shapepoint = self._gtfs_factory.ShapePoint(field_dict=d)
            if not shapepoint.ParseAttributes(self._problems):
                continue

            if shapepoint.shape_id in shapes:
                shape = shapes[shapepoint.shape_id]
            else:
                shape = shape_class(shapepoint.shape_id)
                shape.SetGtfsFactory(self._gtfs_factory)
                shapes[shapepoint.shape_id] = shape

            shape.AddShapePointObjectUnsorted(shapepoint, self._problems)
            self._problems.ClearContext()

        for shape_id, shape in list(shapes.items()):
            self._schedule.AddShapeObject(shape, self._problems)
            del shapes[shape_id]

    def _LoadStopTimes(self):
        stop_time_class = self._gtfs_factory.StopTime

        for (row, row_num, cols) in self._ReadCSV(
            "stop_times.txt",
            stop_time_class._FIELD_NAMES,
            stop_time_class._REQUIRED_FIELD_NAMES,
            stop_time_class._DEPRECATED_FIELD_NAMES,
        ):
            file_context = ("stop_times.txt", row_num, row, cols)
            self._problems.SetFileContext(*file_context)

            (
                trip_id,
                arrival_time,
                departure_time,
                stop_id,
                stop_sequence,
                stop_headsign,
                pickup_type,
                drop_off_type,
                shape_dist_traveled,
                timepoint,
            ) = row

            try:
                sequence = int(stop_sequence)
            except (TypeError, ValueError):
                self._problems.InvalidValue(
                    "stop_sequence", stop_sequence, "This should be a number."
                )
                continue
            if sequence < 0:
                self._problems.InvalidValue(
                    "stop_sequence",
                    sequence,
                    "Sequence numbers should be 0 or higher.",
                )

            if stop_id not in self._schedule.stops:
                self._problems.InvalidValue(
                    "stop_id",
                    stop_id,
                    "This value wasn't defined in stops.txt",
                )
                continue
            stop = self._schedule.stops[stop_id]
            if trip_id not in self._schedule.trips:
                self._problems.InvalidValue(
                    "trip_id",
                    trip_id,
                    "This value wasn't defined in trips.txt",
                )
                continue
            trip = self._schedule.trips[trip_id]

            # If self._problems.Report returns then StopTime.__init__ will return
            # even if the StopTime object has an error. Thus this code may add a
            # StopTime that didn't validate to the database.
            # Trip.GetStopTimes then tries to make a StopTime from the invalid data
            # and calls the problem reporter for errors. An ugly solution is to
            # wrap problems and a better solution is to move all validation out of
            # __init__. For now make sure Trip.GetStopTimes gets a problem reporter
            # when called from Trip.Validate.
            stop_time = stop_time_class(
                self._problems,
                stop,
                arrival_time,
                departure_time,
                stop_headsign,
                pickup_type,
                drop_off_type,
                shape_dist_traveled,
                stop_sequence=sequence,
                timepoint=timepoint,
            )
            trip._AddStopTimeObjectUnordered(stop_time, self._schedule)
            self._problems.ClearContext()

        # stop_times are validated in Trip.ValidateChildren, called by
        # Schedule.Validate

    def Load(self):
        self._problems.ClearContext()
        if not self._DetermineFormat():
            return self._schedule

        self._CheckFileNames()
        self._LoadCalendar()
        self._LoadShapes()
        self._LoadFeed()

        if self._load_stop_times:
            self._LoadStopTimes()

        if self._zip:
            self._zip.close()
            self._zip = None

        if self._extra_validation:
            self._schedule.Validate(self._problems, validate_children=False)

        return self._schedule
