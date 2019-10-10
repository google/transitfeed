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

# Unit tests for the agency module.


import transitfeed
from tests import util


class AgencyValidationTestCase(util.ValidationTestCase):
    def runTest(self):
        # success case
        agency = transitfeed.Agency(
            name="Test Agency",
            url="http://example.com",
            timezone="America/Los_Angeles",
            id="TA",
            lang="xh",
        )
        self.ExpectNoProblems(agency)

        # bad agency
        agency = transitfeed.Agency(
            name="   ",
            url="http://example.com",
            timezone="America/Los_Angeles",
            id="TA",
        )
        self.ValidateAndExpectMissingValue(agency, "agency_name")

        # missing url
        agency = transitfeed.Agency(
            name="Test Agency", timezone="America/Los_Angeles", id="TA"
        )
        self.ValidateAndExpectMissingValue(agency, "agency_url")

        # bad url
        agency = transitfeed.Agency(
            name="Test Agency",
            url="www.example.com",
            timezone="America/Los_Angeles",
            id="TA",
        )
        self.ValidateAndExpectInvalidValue(agency, "agency_url")

        # bad time zone
        agency = transitfeed.Agency(
            name="Test Agency",
            url="http://example.com",
            timezone="America/Alviso",
            id="TA",
        )
        agency.Validate(self.problems)
        e = self.accumulator.PopInvalidValue("agency_timezone")
        self.assertMatchesRegex(
            '"America/Alviso" is not a common timezone', e.FormatProblem()
        )
        self.accumulator.AssertNoMoreExceptions()

        # bad language code
        agency = transitfeed.Agency(
            name="Test Agency",
            url="http://example.com",
            timezone="America/Los_Angeles",
            id="TA",
            lang="English",
        )
        self.ValidateAndExpectInvalidValue(agency, "agency_lang")

        # bad 2-letter language code
        agency = transitfeed.Agency(
            name="Test Agency",
            url="http://example.com",
            timezone="America/Los_Angeles",
            id="TA",
            lang="xx",
        )
        self.ValidateAndExpectInvalidValue(agency, "agency_lang")

        # capitalized language code is OK
        agency = transitfeed.Agency(
            name="Test Agency",
            url="http://example.com",
            timezone="America/Los_Angeles",
            id="TA",
            lang="EN",
        )
        self.ExpectNoProblems(agency)

        # extra attribute in constructor is fine, only checked when loading a file
        agency = transitfeed.Agency(
            name="Test Agency",
            url="http://example.com",
            timezone="America/Los_Angeles",
            agency_mission="monorail you there",
        )
        self.ExpectNoProblems(agency)

        # extra attribute in assigned later is also fine
        agency = transitfeed.Agency(
            name="Test Agency",
            url="http://example.com",
            timezone="America/Los_Angeles",
        )
        agency.agency_mission = "monorail you there"
        self.ExpectNoProblems(agency)

        # good agency_fare_url url
        agency = transitfeed.Agency(
            name="Test Agency",
            url="http://www.example.com",
            timezone="America/Los_Angeles",
            agency_fare_url="http://www.example.com/fares",
        )
        self.ExpectNoProblems(agency)

        # bad agency_fare_url url
        agency = transitfeed.Agency(
            name="Test Agency",
            url="http://www.example.com",
            timezone="America/Los_Angeles",
            agency_fare_url="www.example.com/fares",
        )
        self.ValidateAndExpectInvalidValue(agency, "agency_fare_url")

        # Multiple problems
        agency = transitfeed.Agency(
            name="Test Agency",
            url="www.example.com",
            timezone="America/West Coast",
            id="TA",
        )
        self.assertEqual(False, agency.Validate(self.problems))
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual(e.column_name, "agency_url")
        e = self.accumulator.PopException("InvalidValue")
        self.assertEqual(e.column_name, "agency_timezone")
        self.accumulator.AssertNoMoreExceptions()


class AgencyAttributesTestCase(util.ValidationTestCase):
    def testCopy(self):
        agency = transitfeed.Agency(
            field_dict={
                "agency_name": "Test Agency",
                "agency_url": "http://example.com",
                "timezone": "America/Los_Angeles",
                "agency_mission": "get you there",
            }
        )
        self.assertEqual(agency.agency_mission, "get you there")
        agency_copy = transitfeed.Agency(field_dict=agency)
        self.assertEqual(agency_copy.agency_mission, "get you there")
        self.assertEqual(agency_copy["agency_mission"], "get you there")

    def testEq(self):
        agency1 = transitfeed.Agency(
            "Test Agency", "http://example.com", "America/Los_Angeles"
        )
        agency2 = transitfeed.Agency(
            "Test Agency", "http://example.com", "America/Los_Angeles"
        )
        # Unknown columns, such as agency_mission, do affect equality
        self.assertEqual(agency1, agency2)
        agency1.agency_mission = "Get you there"
        self.assertNotEqual(agency1, agency2)
        agency2.agency_mission = "Move you"
        self.assertNotEqual(agency1, agency2)
        agency1.agency_mission = "Move you"
        self.assertEqual(agency1, agency2)
        # Private attributes don't affect equality
        agency1._private_attr = "My private message"
        self.assertEqual(agency1, agency2)
        agency2._private_attr = "Another private thing"
        self.assertEqual(agency1, agency2)

    def testDict(self):
        agency = transitfeed.Agency(
            "Test Agency", "http://example.com", "America/Los_Angeles"
        )
        agency._private_attribute = "blah"
        # Private attributes don't appear when iterating through an agency as a
        # dict but can be directly accessed.
        self.assertEqual("blah", agency._private_attribute)
        self.assertEqual("blah", agency["_private_attribute"])
        self.assertEqual(
            set("agency_name agency_url agency_timezone".split()),
            set(agency.keys()),
        )
        self.assertEqual(
            {
                "agency_name": "Test Agency",
                "agency_url": "http://example.com",
                "agency_timezone": "America/Los_Angeles",
            },
            dict(agency),
        )


class DeprecatedAgencyFieldsTestCase(util.MemoryZipTestCase):
    def testDeprectatedFieldNames(self):
        self.SetArchiveContents(
            "agency.txt",
            "agency_id,agency_name,agency_timezone,agency_url,agency_ticket_url\n"
            "DTA,Demo Agency,America/Los_Angeles,http://google.com,"
            "http://google.com/tickets\n",
        )
        self.MakeLoaderAndLoad(self.problems)
        e = self.accumulator.PopException("DeprecatedColumn")
        self.assertEqual("agency_ticket_url", e.column_name)
        self.accumulator.AssertNoMoreExceptions()


class MultiAgencyTimeZoneTestCase(util.MemoryZipTestCase):
    def testNoErrorsWithAgenciesHavingSameTimeZone(self):
        self.SetArchiveContents(
            "agency.txt",
            "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
            "DTA,Demo Agency,http://google.com,America/Los_Angeles,en\n"
            "DTA2,Demo Agency 2,http://google.com,America/Los_Angeles,en\n",
        )
        self.MakeLoaderAndLoad(self.problems)
        self.accumulator.AssertNoMoreExceptions()

    def testAgenciesWithDifferentTimeZone(self):
        self.SetArchiveContents(
            "agency.txt",
            "agency_id,agency_name,agency_url,agency_timezone,agency_lang\n"
            "DTA,Demo Agency,http://google.com,America/Los_Angeles,en\n"
            "DTA2,Demo Agency 2,http://google.com,America/New_York,en\n",
        )
        self.MakeLoaderAndLoad(self.problems)
        self.accumulator.PopInvalidValue("agency_timezone")
        self.accumulator.AssertNoMoreExceptions()
