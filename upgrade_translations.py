#!/usr/bin/python2.5
# -*- coding: utf-8 -*-

# Copyright (C) 2019 Google Inc.
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
Upgrades GTFS from Google translations extension [1] to GTFS-Translations [2].

[1] http://developers.google.com/transit/gtfs/reference/gtfs-extensions#translationstxt
[2] http://bit.ly/gtfs-translations

Usage.

Upgrade translations of a feed unpacked to `my-feed` directory and store them
to `my-feed_updated`:

  $ upgrade_translations.py my-feed

Specify output directory name explicitly:

  $ upgrade_translations.py my-feed-old my-feed-new

Sample feed.

  feed_info.txt:
  feed_publisher_name,feed_publisher_url,feed_lang
  Narnia,http://en.wikipedia.org/wiki/Narnia,en

  stops.txt:
  stop_id,stop_name,stop_lat,stop_lon
  stop1,Palace,10,11

  trips.txt:
  route_id,service_id,trip_id,trip_headsign
  sledge,service1,trip1,To Palace

Translations in Google extension format.

  translations.txt:
  trans_id,lang,translation
  http://en.wikipedia.org/wiki/Narnia,en,http://en.wikipedia.org/wiki/Narnia
  http://en.wikipedia.org/wiki/Narnia,es,http://es.wikipedia.org/wiki/Narnia
  Palace,en,Palace
  Palace,es,Palacio
  To Palace,en,To Palace
  To Palace,es,Palacio

Translations in GTFS-Translations format.

  translations.txt:
  table_name,field_name,language,translation,record_id,record_sub_id,field_value
  feed_info,feed_publisher_url,es,http://es.wikipedia.org/wiki/Narnia,,,
  stops,stop_name,es,Palacio,stop1,,
  trips,trip_headsign,es,Palacio,,,To Palace
"""

from __future__ import print_function

import csv
import os
import os.path
import shutil
import sys

# GTFS-Translations defines record_id and record_sub_id used for referencing a
# row in a GTFS table that requires translation.
RECORD_ID_MAP = {
    'agency': ('agency_id', None),
    'stops': ('stop_id', None),
    'routes': ('route_id', None),
    'trips': ('trip_id', None),
    'stop_times': ('trip_id', 'stop_sequence'),
    'feed_info': (None, None),
    'calendar': ('service_id', None),
    'calendar_dates': ('service_id', 'date'),
    'fare_attributes': ('fare_id', None),
    'fare_rules': ('fare_id', 'route_id'),
    'shapes': ('shape_id', 'shape_pt_sequence'),
    'frequencies': ('trip_id', 'start_time'),
    'transfers': ('from_stop_id', 'to_stop_id'),
    'pathways': ('pathway_id', None),
    'levels': ('level_id', None),
}

# File translations.txt in GTFS-Translations has the following fields.
NEW_TRANSLATIONS_FIELDS = [
    'table_name',
    'field_name',
    'language',
    'translation',
    'record_id',
    'record_sub_id',
    'field_value',
]

# Fields whose names end with the following suffixes are translated according
# to Google translations extension.
TRANSLATABLE_FIELD_NAME_SUFFIXES = [
    '_name',
    '_desc',
    '_headsign',
    '_url',
    '_text',
    '_abbreviation',
    # Handle pathway fields "signposted_as", "reversed_signposted_as"
    # and "instructions".
    'signposted_as',
    'instructions',
]


class RecordIdHelper(object):
    """Helper object to find record_id and record_sub_id based on GTFS table
    name and its fields.
    """
    def __init__(self, table_name, field_names):
        id_and_sub_id = RECORD_ID_MAP.get(table_name)
        if id_and_sub_id is None:
            # Use the first field name that ends with _id as record_id.
            first_id = self._find_first_id(field_names)
            if first_id:
                id_and_sub_id = (first_id, None)
            else:
                id_and_sub_id = (None, None)
        self.id_and_sub_id = id_and_sub_id

    def get_record_id(self, row):
        if self.id_and_sub_id[0]:
            return row.get(self.id_and_sub_id[0])
        return None

    def get_record_sub_id(self, row):
        if self.id_and_sub_id[1]:
            return row.get(self.id_and_sub_id[1])
        return None

    def describe_ids(self):
        return 'record_id = "%s", record_sub_id = "%s"' % (
            self.id_and_sub_id[0] or '',
            self.id_and_sub_id[1] or '')

    @staticmethod
    def _find_first_id(field_names):
        for field_name in field_names:
            if field_name.endswith('_id'):
                return field_name
        return None


def read_first_available_value(filename, field_name):
    """Reads the first assigned value of the given field in the CSV table.
    """
    if not os.path.exists(filename):
        return None
    with open(filename, 'rb') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            value = row.get(field_name)
            if value:
                return value
    return None


def is_translatable_field(field):
    for suffix in TRANSLATABLE_FIELD_NAME_SUFFIXES:
        if field.endswith(suffix):
            return True
    return False


def any_translatable_field(fields):
    return any(is_translatable_field(field) for field in fields)


class OldTranslations(object):
    """Reads all old translations and keeps them for further usage.
    """
    def __init__(self, src_dir):
        self.src_dir = src_dir
        self._find_feed_language()
        self._read_translations()
        self._find_context_dependent_names()

    def _find_feed_language(self):
        """Find feed language based specified feed_info.txt or agency.txt.
        """
        self.feed_language = (
            read_first_available_value(
                os.path.join(self.src_dir, 'feed_info.txt'), 'feed_lang') or
            read_first_available_value(
                os.path.join(self.src_dir, 'agency.txt'), 'agency_lang'))
        if not self.feed_language:
            raise Exception(
                'Cannot find feed language in feed_info.txt and agency.txt')
        print('\tfeed language: %s' % self.feed_language)

    def _read_translations(self):
        """Read from the old translations.txt.
        """
        print('Reading original translations')
        self.translations_map = {}
        n_translations = 0
        with open(os.path.join(self.src_dir, 'translations.txt'),
                  'rb') as csvfile:
            reader = csv.DictReader(csvfile)
            for row in reader:
                self.translations_map.setdefault(
                    row['trans_id'], {})[row['lang']] = row['translation']
                n_translations += 1
        print('\ttotal original translations: %s' % n_translations)

    def _find_context_dependent_names(self):
        """Finds texts whose translation depends on context.

        Example.
          Here the word "Palacio" is translated from Spanish to English in
          multiple ways. Feed language is es (Spanish).

          trans_id,lang,translation
          stop-name-1,es,Palacio
          stop-name-1,en,Palace
          headsign-1,es,Palacio
          headsign-1,en,To Palace
        """
        n_occurences_of_original = {}
        for trans_id, translations in self.translations_map.items():
            try:
                original_name = translations[self.feed_language]
            except KeyError:
                raise Exception(
                    'No translation in feed language for %s, available: %s' %
                    (trans_id, translations))
            n_occurences_of_original[original_name] = (
                n_occurences_of_original.get(original_name, 0) +
                1)

        self.context_dependent_names = set(
            name
            for name, occur in n_occurences_of_original.items()
            if occur > 1)
        print('Total context-dependent translations: %d' %
              len(self.context_dependent_names))


class TranslationsConverter(object):
    """Converts translations from the old to the new format.
    """
    def __init__(self, src_dir):
        self.src_dir = src_dir
        self.old_translations = OldTranslations(src_dir)

    def convert_translations(self, dest_dir):
        """
        Converts translations to the new format and stores at dest_dir.
        """
        if not os.path.isdir(dest_dir):
            os.makedirs(dest_dir)
        total_translation_rows = 0
        with open(os.path.join(dest_dir, 'translations.txt'),
                  'w+b') as out_file:
            writer = csv.DictWriter(
                out_file, fieldnames=NEW_TRANSLATIONS_FIELDS)
            writer.writeheader()
            for filename in sorted(os.listdir(self.src_dir)):
                if not (filename.endswith('.txt') and
                        os.path.isfile(os.path.join(self.src_dir, filename))):
                    print('Skipping %s' % filename)
                    continue
                table_name = filename[:-len('.txt')]
                if table_name == 'translations':
                    continue
                total_translation_rows += self._translate_table(
                    dest_dir, table_name, writer)
        print('Total translation rows: %s' % total_translation_rows)

    def _translate_table(self, dest_dir, table_name, translations_writer):
        """
        Converts translations to the new format for a single table.
        """
        in_filename = os.path.join(self.src_dir, '%s.txt' % table_name)
        if not os.path.exists(in_filename):
            raise Exception('No %s' % table_name)

        out_filename = os.path.join(dest_dir, '%s.txt' % table_name)
        with open(in_filename, 'rb') as in_file:
            reader = csv.DictReader(in_file)
            if not reader.fieldnames or not any_translatable_field(
                    reader.fieldnames):
                print('Copying %s with no translatable columns' % table_name)
                shutil.copy(in_filename, out_filename)
                return 0
            table_translator = TableTranslator(
                table_name, reader.fieldnames, self.old_translations,
                translations_writer)
            with open(out_filename, 'w+b') as out_file:
                writer = csv.DictWriter(out_file, fieldnames=reader.fieldnames)
                writer.writeheader()
                for row in reader:
                    writer.writerow(table_translator.translate_row(row))

            table_translator.write_for_field_values()
            print('\ttranslation rows: %s' %
                  table_translator.total_translation_rows)
            return table_translator.total_translation_rows


class TableTranslator(object):
    """Translates a given GTFS table.
    """
    def __init__(self, table_name, field_names, old_translations,
                 translations_writer):
        self.table_name = table_name
        self.old_translations = old_translations
        self.translations_writer = translations_writer
        self.record_id_helper = RecordIdHelper(table_name, field_names)
        self.total_translation_rows = 0
        # stop_times.txt and trips.txt usually have a lot of repeated
        # headsigns, so it is better to use field_value than record_id
        # and record_sub_id.  However, we will fallback to
        # record_id+sub_id if the translation is context-dependent,
        # e.g., the same trip_headsign is translated differently for
        # different trips.
        self.table_uses_record_id = table_name not in ('stop_times', 'trips')
        self.translations_for_values = {}

        print('Translating %s by %s' % (
            table_name,
            self.record_id_helper.describe_ids()
            if self.table_uses_record_id
            else 'field_name'))

    def translate_row(self, row):
        table_name = self.table_name
        feed_language = self.old_translations.feed_language
        translations_map = self.old_translations.translations_map
        context_dependent_names = self.old_translations.context_dependent_names
        out_row = row
        for field_name, field_value in row.items():
            if not is_translatable_field(field_name):
                continue
            field_translations = translations_map.get(field_value)
            if not field_translations:
                continue
            value_in_feed_lang = field_translations[feed_language]
            out_row[field_name] = value_in_feed_lang
            # If translation depends on the context, then always use record_id.
            use_record_id = (
                self.table_uses_record_id or
                value_in_feed_lang in context_dependent_names)
            record_id = self.record_id_helper.get_record_id(row)
            record_sub_id = self.record_id_helper.get_record_sub_id(row)
            for language, translation in field_translations.items():
                if language == feed_language:
                    continue
                if use_record_id:
                    self._write_translation_row({
                        'table_name': table_name,
                        'field_name': field_name,
                        'language': language,
                        'translation': translation,
                        'record_id': record_id,
                        'record_sub_id': record_sub_id,
                    })
                else:
                    self.translations_for_values[(
                        field_name,
                        language,
                        value_in_feed_lang)] = translation
        return out_row

    def write_for_field_values(self):
        for ((field_name, language, field_value),
             translation) in self.translations_for_values.items():
            self._write_translation_row({
                'table_name': self.table_name,
                'field_name': field_name,
                'language': language,
                'translation': translation,
                'field_value': field_value,
            })

    def _write_translation_row(self, row):
        self.translations_writer.writerow(row)
        self.total_translation_rows += 1


def main():
    if len(sys.argv) < 2:
        print('usage: upgrade_translations.py [SRC GTFS DIR] [DEST GTFS DIR]',
              file=sys.stderr)
        sys.exit(1)

    src_dir = os.path.normpath(sys.argv[1])
    if len(sys.argv) >= 3:
        dest_dir = sys.argv[2]
    else:
        dest_dir = '%s_upgraded' % src_dir

    print('Upgrading translations')
    print('\tsource directory: %s' % src_dir)
    print('\tdestination directory: %s' % dest_dir)

    TranslationsConverter(src_dir).convert_translations(dest_dir)
    print('Done!')


if __name__ == '__main__':
    main()
