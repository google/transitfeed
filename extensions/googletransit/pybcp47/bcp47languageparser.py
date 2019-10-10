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

import re
import string
from functools import reduce

from pkg_resources import resource_string


class FileParseError(Exception):
    """Exception raised for errors in the subtag registry file. """

    def __init__(self, line_number, msg):
        self.msg = "Error at line %s in the subtag registry file: %s" % (
            line_number,
            msg,
        )

    def __str__(self):
        return repr(self.msg)


class Bcp47LanguageParser(object):
    """Validates language tags to be well-formed and registered BCP-47 codes. """

    def __init__(self):
        # Dictionaries for mapping tags and subtags to their descriptions.
        self.languages = {}  # language tags, e.g. 'de'
        self.extlangs = {}  # extlang subtags, e.g. 'aao'
        self.scripts = {}  # script subtagss, e.g. 'Latn'
        self.regions = {}  # region subtags, e.g. 'CA'
        self.variants = {}  # variant subtags, e.g. '1901'
        self.grandfathereds = {}  # grandfathered tags, e.g. 'sgn-CH-DE'
        self.redundants = {}  # redundant subtags, e.g. 'zh-Hant-CN'
        self._ReadLanguageSubtagRegistryFile()

    def _GetLinesFromLanguageSubtagRegistryFile(self):
        # Read and yield the registry file from this package. This should be a most
        # recent copy of http://www.iana.org/assignments/language-subtag-registry
        # Formatting rules of this file can be found at page 20 of
        # http://tools.ietf.org/html/rfc5646
        file_name = "language-subtag-registry.txt"
        # Read Unicode string from the UTF-8 bytes in the file.
        file_string_utf8 = resource_string(__name__, file_name).decode("utf-8")
        # Yield the lines from the file. Handle "folding" indicated by two leading
        # whitespaces.
        accumulated_line_parts = None
        line_number = 0
        for line in file_string_utf8.splitlines():
            line_number += 1
            if line[:2] == "  ":
                accumulated_line_parts.append(line.strip())
            else:
                if accumulated_line_parts:
                    yield (" ".join(accumulated_line_parts), line_number)
                    accumulated_line_parts = [line.strip()]
                else:
                    accumulated_line_parts = [line.strip()]
        if accumulated_line_parts:
            yield (" ".join(accumulated_line_parts), line_number)

    def _ReadLanguageSubtagRegistryFile(self):
        # Load the entries from the registry file in this package.
        line_iterator = self._GetLinesFromLanguageSubtagRegistryFile()
        # Read the header lines with the File-Date record.
        first_line, line_number = next(line_iterator)
        if not first_line[:11] == "File-Date: ":
            raise FileParseError(
                line_number,
                "Invalid first line '%s'! Must be a File-Date record."
                % (first_line),
            )
        second_line, line_number = next(line_iterator)
        if not second_line == "%%":
            raise FileParseError(
                line_number,
                "Invalid first record '%s'! Must start with '%%%%'."
                % (second_line),
            )
        # Read the (Sub)tag records.
        current_type = None
        current_tag = None
        current_descriptions = []
        current_prefixes = []
        for line, line_number in line_iterator:
            if line == "%%":
                self._AddSubtagFromRegistryFile(
                    current_type,
                    current_tag,
                    current_descriptions,
                    current_prefixes,
                    line_number,
                )
                current_type = None
                current_tag = None
                current_descriptions = []
                current_prefixes = []
                continue

            line_parts = line.split(": ")
            if len(line_parts) > 2 and line_parts[0] == "Comments":
                # Silently ignore comments. They may contain multiple ':'.
                continue
            if len(line_parts) != 2:
                raise FileParseError(
                    line_number, "Invalid line %s in registry file!" % (line)
                )

            line_key, line_value = line_parts
            if line_key == "Type":
                if current_type:
                    raise FileParseError(
                        line_number,
                        "Duplicate Type for (Sub)tag %s" % (current_tag),
                    )
                current_type = line_value.lower()
            elif line_key == "Subtag" or line_key == "Tag":
                if current_tag:
                    raise FileParseError(
                        line_number, "Duplicate (Sub)tag %s" % (current_tag)
                    )
                current_tag = line_value.lower()
            elif line_key == "Description":
                current_descriptions.append(line_value)
            elif line_key == "Prefix":
                current_prefixes.append(line_value)
            elif line_key not in [
                "Added",
                "Deprecated",
                "Preferred-Value",
                "Suppress-Script",
                "Macrolanguage",
                "Scope",
                "Comments",
            ]:
                raise FileParseError(
                    line_number,
                    "Invalid registry field %s with value %s"
                    % (line_key, line_value),
                )

        # The last record does not get terminated by the '%%' preceding the next
        # record. So we have to add it after the 'for' loop.
        self._AddSubtagFromRegistryFile(
            current_type,
            current_tag,
            current_descriptions,
            current_prefixes,
            line_number,
        )

    def IntStr26ToInt(self, int_str):
        return reduce(
            lambda x, y: 26 * x + y,
            list(map(string.ascii_lowercase.index, int_str)),
        )

    def IntToIntStr26(self, int_value, int_str=""):
        if int_value == 0:
            return int_str
        return self.IntToIntStr26(
            int(int_value / 26),
            string.ascii_lowercase[int(int_value % 26)] + int_str,
        )

    def _AddSubtagFromRegistryFile(
        self,
        current_type,
        current_tag,
        current_descriptions,
        current_prefixes,
        line_number,
    ):
        if not current_descriptions:
            raise FileParseError(
                line_number,
                "Missing Description(s) for (Sub)tag %s" % (current_tag),
            )
        current_description = ", ".join(current_descriptions)

        if not current_tag:
            raise FileParseError(
                line_number,
                "Missing (Sub)tag for Type %s and Description(s) %s"
                % (current_type, current_description),
            )

        if ".." in current_tag:
            # Decompose ranges (private use range) and add them recursively.
            range_parts = current_tag.split("..")
            if len(range_parts) != 2:
                raise FileParseError(
                    line_number,
                    "(Sub)tag ranges must consist of two tags being separated by '..'! "
                    "The range '%s' of Type '%s' is invalid."
                    % (current_tag, current_type),
                )
            start_str, end_str = range_parts
            if len(start_str) != len(end_str):
                raise FileParseError(
                    line_number,
                    "The start and end tags in ranges must have the same length! "
                    "The tags '%s' and '%s' are different!"
                    % (start_str, end_str),
                )
            for i in range(
                self.IntStr26ToInt(start_str), self.IntStr26ToInt(end_str) + 1
            ):
                range_tag = self.IntToIntStr26(i)
                self._AddSubtagFromRegistryFile(
                    current_type,
                    range_tag,
                    current_descriptions,
                    current_prefixes,
                    line_number,
                )
            # Range tags are added as recursion so we have to return afterwards.
            return

        if current_type == "language":
            self.languages[current_tag] = current_description
        elif current_type == "extlang":
            if current_prefixes:
                for current_prefix in current_prefixes:
                    extlang = current_prefix + "-" + current_tag
                    self.extlangs[extlang] = current_description
            self.extlangs[current_tag] = current_description
        elif current_type == "region":
            self.regions[current_tag] = current_description
        elif current_type == "variant":
            self.variants[current_tag] = current_description
        elif current_type == "grandfathered":
            self.grandfathereds[current_tag] = current_description
        elif current_type == "redundant":
            self.redundants[current_tag] = current_description
        elif current_type == "script":
            self.scripts[current_tag] = current_description
        else:
            raise FileParseError(
                line_number,
                "Invalid Type %s for Subtag %s" % (current_type, current_tag),
            )

    # A dictionary for the regular expression strings to test wellformedness.
    _wellformed_dict = {}

    # Language subtag of 4 to 8 characters registered in the IANA subtag registry.
    _wellformed_dict["iana_lang"] = "[a-zA-Z]{4,8}"
    # Language subtag of 2 or 3 characters according to ISO 639-1/-2/-3/-5,
    # optionally followed by up to 3 extended language subtags of 3 characters
    # each as in ISO 639-3.
    _wellformed_dict["extlang"] = "[a-zA-Z]{3}"
    _wellformed_dict["iso_lang_extlang"] = (
        "[a-zA-Z]{2,3}(-%(extlang)s){0,3}" % _wellformed_dict
    )
    _wellformed_dict["lang"] = (
        "(%(iana_lang)s|%(iso_lang_extlang)s|x)" % _wellformed_dict
    )

    # Script subtag of 4 characters as in ISO 15924.
    _wellformed_dict["script"] = "[a-zA-Z]{4}"

    # Region subtag of 2 characters as in ISO 3166-1 or of 3 digits as in the
    # "UN Standard Country or Area Codes for Statistical Use".
    _wellformed_dict["region"] = r"([a-zA-Z]{2}|\d{3})"

    # Variant subtag of 4 to 8 characters (must begin with a digit if length 4).
    _wellformed_dict["variant"] = r"([a-zA-Z0-9]{5,8}|\d[a-zA-Z0-9]{3})"

    # Extension subtag consisting of a singleton subtag (1 character, not "x")
    # followed by at least one subtag of 2 to 8 characters.
    _wellformed_dict["extension"] = "([a-wyzA-WYZ0-9](-[a-zA-Z0-9]{2,8})+)"

    # Private subtag consisting of the subtag "x" followed by 1..n subtags of
    # 1 to 8 characters.
    _wellformed_dict["private"] = "x(-([a-zA-Z0-9]{1,8}))+"

    # Full BCP-47 wellformed regular expression.
    _wellformed_bcp47 = re.compile(
        "^("
        "((%(iana_lang)s)"  # 1 language subtag as registered at IANA
        "|(%(iso_lang_extlang)s))"  # or 1 language subtag as in ISO 639x
        "(-%(script)s)?"  # 0..1 script subtags
        "(-%(region)s)?"  # 0..1 region subtags
        "(-%(variant)s)*"  # 0..n variant subtags
        "(-%(extension)s)*"  # 0..n extension subtags
        ")?"
        "((^|-)%(private)s)?$"  # 0..1 private subtags (can be standalone)
        % _wellformed_dict
    )

    def IsWellformedSubtag(self, subtag, subtag_type):
        if subtag_type in self._wellformed_dict:
            subtag_regexp = "^%s$" % (self._wellformed_dict[subtag_type])
            return re.search(subtag_regexp, subtag) is not None
        return False

    def IsWellformed(self, lang_code):
        if lang_code.lower() in self.grandfathereds:
            return True
        match_obj = self._wellformed_bcp47.match(lang_code)
        if not match_obj:
            return False
        elif match_obj.group(0) != lang_code:
            return False
        else:
            return True

    def ParseLanguage(self, lang_code):
        lang_obj = Bcp47LanguageObject(lang_code)

        if not self.IsWellformed(lang_code):
            return lang_obj
        lang_obj.wellformed = True

        lang_code = lang_code.lower()
        if lang_code in self.grandfathereds:
            return lang_obj.Update(self.grandfathereds[lang_code], True, True)
        if lang_code in self.redundants:
            return lang_obj.Update(self.redundants[lang_code], True, True)

        lang_code_parts = lang_code.split("-")
        lang_code_part_len = len(lang_code_parts)
        lang_code_part_idx = 0
        lang_tag = lang_code_parts[lang_code_part_idx]
        if not self.IsWellformedSubtag(lang_tag, "lang"):
            return lang_obj.Update(None, False, False)
        elif lang_tag != "x":
            if lang_tag in self.languages:
                lang_obj.descriptions.append(self.languages[lang_tag])
            else:
                return lang_obj.Update(
                    "unknown language '" + lang_tag + "'", True, False
                )
            lang_code_part_idx = lang_code_part_idx + 1

        if lang_code_part_idx == lang_code_part_len:
            return lang_obj.Update(None, True, True)

        extlang_tag = lang_code_parts[lang_code_part_idx]
        if self.IsWellformedSubtag(extlang_tag, "extlang"):
            if extlang_tag in self.extlangs:
                lang_obj.descriptions.append(self.extlangs[extlang_tag])
            else:
                return lang_obj.Update(
                    "unknown extlang '" + extlang_tag + "'", True, False
                )
            lang_code_part_idx = lang_code_part_idx + 1

        if lang_code_part_idx == lang_code_part_len:
            return lang_obj.Update(None, True, True)

        script_tag = lang_code_parts[lang_code_part_idx]
        if self.IsWellformedSubtag(script_tag, "script"):
            if script_tag in self.scripts:
                lang_obj.descriptions.append(
                    self.scripts[script_tag] + " script"
                )
            else:
                return lang_obj.Update(
                    "unknown script '" + script_tag + "'", True, False
                )
            lang_code_part_idx = lang_code_part_idx + 1

        if lang_code_part_idx == lang_code_part_len:
            return lang_obj.Update(None, True, True)

        region_tag = lang_code_parts[lang_code_part_idx]
        if self.IsWellformedSubtag(region_tag, "region"):
            if region_tag in self.regions:
                lang_obj.descriptions.append(self.regions[region_tag])
            else:
                return lang_obj.Update(
                    "unknown region '" + region_tag + "'", True, False
                )
            lang_code_part_idx = lang_code_part_idx + 1

        if lang_code_part_idx == lang_code_part_len:
            return lang_obj.Update(None, True, True)

        variant_tag = lang_code_parts[lang_code_part_idx]
        if self.IsWellformedSubtag(variant_tag, "variant"):
            if variant_tag in self.variants:
                lang_obj.descriptions.append(self.variants[variant_tag])
            else:
                return lang_obj.Update(
                    "unknown variant '" + variant_tag + "'", True, False
                )
            lang_code_part_idx = lang_code_part_idx + 1

        if lang_code_part_len > lang_code_part_idx:
            remainder = "-".join(lang_code_parts[lang_code_part_idx:])
            if len(remainder) > 0:
                return lang_obj.Update(
                    "subtag '" + remainder + "' was ignored", True, True
                )

        return lang_obj.Update(None, True, True)

    def Parse_ISO639_1_Language(self, lang_code):
        lang_obj = Bcp47LanguageObject(lang_code)

        lang_code = lang_code.lower()
        if len(lang_code) == 2:
            if lang_code in self.languages:
                lang_obj.Update(self.languages[lang_code], True, True)
            else:
                match_obj = re.match("^([a-z]{2})", lang_code)
                if match_obj:
                    lang_obj.wellformed = True

        return lang_obj


class Bcp47LanguageObject(object):
    def __init__(self, lang_code):
        self.lang_code = lang_code
        self.descriptions = []
        self.wellformed = False
        self.valid = False

    def Update(self, description, wellformed, valid):
        if description:
            self.descriptions.append(description)
        self.wellformed = wellformed
        self.valid = valid
        return self

    def __str__(self):
        return ", ".join(self.descriptions)
