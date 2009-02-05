#!/usr/bin/python2.4

# Copyright (C) 2009 Google Inc.
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

"""Command line interface to an sqlite database, can also load a db from csv."""


import cmd
import re
import csv
import os
import os.path
import resource
import pstats

try:
  import sqlite3 as sqlite
except ImportError:
  from pysqlite2 import dbapi2 as sqlite

class SqlLoop(cmd.Cmd):
  """Interactive sql shell"""
  def __init__(self, cursor):
    cmd.Cmd.__init__(self)
    self.prompt = '> '
    self.continue_line = ''
    self.cursor = cursor
    self.doc = r"""List stops by trip in order visited:
LECT trip_headsign,trip_id,stop_name from stop_times NATURAL JOIN trips \
TURAL JOIN stops WHERE trip_headsign LIKE '%intern%' ORDER BY trip_id,time"""

  def do_help(self, topic):
    print self.doc

  def do_EOF(self, line):
    print
    return True

  def default(self, line):
    if line[-1] == '\\':
      self.continue_line += line[0:-1]
      self.prompt = ''
      return
    else:
      line = self.continue_line + line
      self.continue_line = ''
      self.prompt = '> '

    try:
      self.cursor.execute(line);
      s = "%s" % self.cursor.fetchall()
      if len(s) > 2000:
        print s[0:2000]
      else:
        print s
    except sqlite.DatabaseError, e:
      print "error %s" % e


def LoadNamedFile(file_name, conn):
  basename = os.path.basename(file_name)
  last_dot = basename.rfind(".")
  if last_dot > 0:
    basename = basename[:last_dot]
  file_object = open(file_name, "rb")
  LoadFile(file_object, basename, conn)

def LoadFile(f, table_name, conn):
  """Import lines from f as new table in db with cursor c."""
  reader = csv.reader(f)
  header = reader.next()

  columns = []
  for n in header:
    n = n.replace(' ', '')
    n = n.replace('-', '_')
    columns.append(n)

  create_columns = []
  column_types = {}
  for n in columns:
    if n in column_types:
      create_columns.append("%s %s" % (n, column_types[n]))
    else:
      create_columns.append("%s INTEGER" % (n))

  c = conn.cursor()
  try:
    c.execute("CREATE TABLE %s (%s)" % (table_name, ",".join(create_columns)))
  except sqlite.OperationalError:
    # Likely table exists
    print "table %s already exists?" % (table_name)
    for create_column in create_columns:
      try:
        c.execute("ALTER TABLE %s ADD COLUMN %s" % (table_name, create_column))
      except sqlite.OperationalError:
        # Likely it already exists
        print "column %s already exists in %s?" % (create_column, table_name)

  placeholders = ",".join(["?"] * len(columns))
  insert_values = "INSERT INTO %s (%s) VALUES (%s)" % (table_name, ",".join(columns), placeholders)

  #c.execute("BEGIN TRANSACTION;")
  for row in reader:
    if row:
      if len(row) < len(columns):
        row.extend([None] * (len(columns) - len(row)))
      c.execute(insert_values, row)
      conn.commit()
  #c.execute("END TRANSACTION;")
  conn.commit()

def main():
  import optparse
  parser = optparse.OptionParser(usage='usage: %prog [options] [sql command]')
  parser.add_option('-f', '--load_file', dest='load_file', metavar='FILE',
                    help='Load FILE into the db')
  parser.add_option('-d', '--load_dir', dest='load_dir', metavar='FILE',
                    help='Load FILE/*.txt into the db')
  parser.add_option('-i', '--interactive', dest='interactive',
                    action='store_true',
                    help='Go into command prompt mode')
  parser.add_option('', '--db', dest='database', metavar='FILE',
                    help='sqlite db')
  parser.set_defaults(database=':memory:', interactive=False)
  (options, args) = parser.parse_args()
  conn = sqlite.connect(options.database)

  if options.load_dir:
    for entry in os.listdir(options.load_dir):
      if re.search(r"\.txt", entry):
        LoadNamedFile(os.path.join(options.load_dir, entry), conn)

  if options.load_file:
    LoadNamedFile(options.load_file, conn)

  cursor = conn.cursor()
  if args:
    import csv
    import sys
    writer = csv.writer(sys.stdout)
    cursor.execute(args[0])
    writer.writerow([desc[0] for desc in cursor.description])
    for row in cursor:
      writer.writerow([unicode(v).encode('utf8') for v in row])
  elif options.interactive:
    loop = SqlLoop(cursor)
    loop.cmdloop()

def ProfileMain():
  import cProfile
  cProfile.run('main()', 'load-stats')
  p = pstats.Stats('load-stats')
  p.strip_dirs()
  p.sort_stats('cumulative').print_callers(10)

if __name__ == '__main__':
  try:
    import traceplus
    traceplus.RunWithExpandedTrace(main)
  except ImportError:
    main()
