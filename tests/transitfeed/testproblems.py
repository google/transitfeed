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

# Unit tests for the problem module.


import re

import transitfeed
from tests import util


class ProblemReporterTestCase(util.RedirectStdOutTestCaseBase):
    # Unittest for problem reporter
    def testContextWithBadUnicodeProblem(self):
        pr = transitfeed.ProblemReporter()
        # Context has valid unicode values
        pr.SetFileContext(
            "filename.foo",
            23,
            ["Andr\202", "Person \uc720 foo", None],
            ["1\202", "2\202", "3\202"],
        )
        pr.OtherProblem("test string")
        pr.OtherProblem("\xff\xfe\x80\x88")
        # Invalid ascii and utf-8. encode('utf-8') and decode('utf-8') will fail
        # for this value
        pr.OtherProblem("\xff\xfe\x80\x88")
        self.assertTrue(re.search(r"test string", self.this_stdout.getvalue()))
        self.assertTrue(
            re.search(r"filename.foo:23", self.this_stdout.getvalue())
        )

    def testNoContextWithBadUnicode(self):
        pr = transitfeed.ProblemReporter()
        pr.OtherProblem("test string")
        pr.OtherProblem("\xff\xfe\x80\x88")
        # Invalid ascii and utf-8. encode('utf-8') and decode('utf-8') will fail
        # for this value
        pr.OtherProblem("\xff\xfe\x80\x88")
        self.assertTrue(re.search(r"test string", self.this_stdout.getvalue()))

    def testBadUnicodeContext(self):
        pr = transitfeed.ProblemReporter()
        pr.SetFileContext(
            "filename.foo",
            23,
            ["Andr\202", "Person \xff\xfe\x80\x88 foo", None],
            ["1\202", "2\202", "3\202"],
        )
        pr.OtherProblem("help, my context isn't utf-8!")
        self.assertTrue(
            re.search(r"help, my context", self.this_stdout.getvalue())
        )
        self.assertTrue(
            re.search(r"filename.foo:23", self.this_stdout.getvalue())
        )

    def testLongWord(self):
        # Make sure LineWrap doesn't puke
        pr = transitfeed.ProblemReporter()
        pr.OtherProblem(
            "1111untheontuhoenuthoentuhntoehuontehuntoehuntoehunto"
            "2222oheuntheounthoeunthoeunthoeuntheontuheontuhoue"
        )
        self.assertTrue(re.search(r"1111.+2222", self.this_stdout.getvalue()))


class BadProblemReporterTestCase(util.RedirectStdOutTestCaseBase):
    """Make sure ProblemReporter doesn't crash when given bad unicode data and
    does find some error"""

    # tom.brown.code-utf8_weaknesses fixed a bug with problem reporter and bad
    # utf-8 strings
    def runTest(self):
        loader = transitfeed.Loader(
            util.DataPath("bad_utf8"),
            problems=transitfeed.ProblemReporter(),
            extra_validation=True,
        )
        loader.Load()
        # raises exception if not found
        self.this_stdout.getvalue().index("Invalid value")
