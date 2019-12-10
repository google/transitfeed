#!/usr/bin/python2.5

# Copyright (C) 2018 Google Inc.
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

# Includes locals in the stacktrace when a failure occurs.
#
# Example use:
#
# if __name__ == '__main__':
#   try:
#     import traceplusunittest
#   except ImportError:
#     unittest.main()
#   else:
#     traceplusunittest.main()

import unittest
import traceplus
import traceback
import inspect


class TextBigStackTestRunner(unittest.TextTestRunner):
    def _makeResult(self):
        return TextBigStackTestResult(self.stream, self.descriptions, self.verbosity)


class TextBigStackTestResult(unittest._TextTestResult):
    def _exc_info_to_string(self, err, test):
        """Converts a sys.exc_info()-style tuple of values into a string."""
        exctype, value, tb = err
        # Skip test runner traceback levels
        while tb and self._is_relevant_tb_level(tb):
            tb = tb.tb_next
        if exctype is test.failureException:
            # Skip assert*() traceback levels
            length = self._count_relevant_tb_levels(tb)
            return "".join(FormatException(exctype, value, tb, length))
        return "".join(FormatException(exctype, value, tb))


def FormatException(exctype, value, tb, length=None):
    frame_records = inspect.getinnerframes(tb, 3)

    dump = []
    if length is None:
        dump.extend(traceplus.MakeExpandedTrace(frame_records))
    else:
        dump.extend(traceplus.MakeExpandedTrace(frame_records[:length]))
    dump.extend(traceback.format_exception_only(exctype, value))
    return "".join(dump)


def main():
    unittest.main(testRunner=TextBigStackTestRunner())
