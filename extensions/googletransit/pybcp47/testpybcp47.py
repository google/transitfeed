#!/usr/bin/python3

# Copyright (C) 2011 Google Inc.
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

# Unit tests for the bcp47languageparser module.


import codecs
import os
import unittest

from .bcp47languageparser import Bcp47LanguageParser


class PyBcp47TestCase(unittest.TestCase):
    bcp47parser = Bcp47LanguageParser()

    def testRegistryFileRecordsBeingWellformed(self):
        # Test whether the parsed entries from the registry file in this package are
        # valid. The registry file in this package is originally downloaded from
        # http://www.iana.org/assignments/language-subtag-registry. Formatting
        # rules of this file can be found at http://tools.ietf.org/html/rfc5646
        for tag in list(self.bcp47parser.grandfathereds.keys()):
            self.assertTrue(
                self.bcp47parser.IsWellformed(tag),
                "Grandfathered tag '%s' in language-subtag-registry.txt "
                "seems to be invalid!" % (tag),
            )
        for tag in list(self.bcp47parser.redundants.keys()):
            self.assertTrue(
                self.bcp47parser.IsWellformed(tag),
                "Redundant tag '%s' in language-subtag-registry.txt "
                "seems to be invalid!" % (tag),
            )
        for tag in list(self.bcp47parser.languages.keys()):
            self.assertTrue(
                self.bcp47parser.IsWellformedSubtag(tag, "lang"),
                "Language subtag '%s' in language-subtag-registry.txt "
                "seems to be invalid!" % (tag),
            )
        for tag in list(self.bcp47parser.extlangs.keys()):
            # extlangs contains each for each extlang just the tag and the tag
            # combined with its prefix. E.g. 'aao' and 'ar-aao'.
            extlang_parts = tag.split("-")
            extlang = extlang_parts[len(extlang_parts) - 1]
            self.assertTrue(
                self.bcp47parser.IsWellformedSubtag(extlang, "extlang"),
                "Extlang subtag '%s' in language-subtag-registry.txt "
                "seems to be invalid!" % (tag),
            )
        for tag in list(self.bcp47parser.scripts.keys()):
            self.assertTrue(
                self.bcp47parser.IsWellformedSubtag(tag, "script"),
                "Script subtag '%s' in language-subtag-registry.txt "
                "seems to be invalid!" % (tag),
            )
        for tag in list(self.bcp47parser.regions.keys()):
            self.assertTrue(
                self.bcp47parser.IsWellformedSubtag(tag, "region"),
                "Region subtag '%s' in language-subtag-registry.txt "
                "seems to be invalid!" % (tag),
            )
        for tag in list(self.bcp47parser.variants.keys()):
            self.assertTrue(
                self.bcp47parser.IsWellformedSubtag(tag, "variant"),
                "Variant subtag '%s' in language-subtag-registry.txt "
                "seems to be invalid!" % (tag),
            )

    def testValidationWithSamples(self):
        # Test whether samples are all well-formed but not valid.
        self._CheckTagsInFile("well-formed-not-valid-tags.txt", True, False)

        # Test whether samples are all not well-formed.
        self._CheckTagsInFile("not-well-formed-tags.txt", False, False)

        # Test whether samples are all valid.
        self._CheckTagsInFile("valid-tags.txt", True, True)

    def _CheckTagsInFile(
        self, filename, should_be_wellformed, should_be_valid
    ):
        full_filename = os.path.join(
            os.path.dirname(__file__), "testdata", filename
        )
        fileObj = codecs.open(full_filename, "r", "utf-8")
        for line in fileObj:
            line_parts = line.split("#")
            tag = line_parts[0].strip()
            if tag:
                lang_obj = self.bcp47parser.ParseLanguage(tag)
                self.assertEqual(
                    lang_obj.wellformed,
                    should_be_wellformed,
                    "the language code '%s' (%s) should%s be well-formed"
                    % (
                        tag,
                        lang_obj,
                        str((not should_be_wellformed and " not") or ""),
                    ),
                )
                self.assertEqual(
                    lang_obj.valid,
                    should_be_valid,
                    "the language code '%s' (%s) should%s be valid"
                    % (
                        tag,
                        lang_obj,
                        str((not should_be_valid and " not") or ""),
                    ),
                )
