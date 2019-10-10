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

# Problem types:
# Error: A data issue not allowed by the GTFS spec.
TYPE_ERROR = 0
# Warning: A data issue not recommended by the GTFS spec.
TYPE_WARNING = 1
# Notice: an issue unrelated to data.
TYPE_NOTICE = 2

ALL_TYPES = [TYPE_ERROR, TYPE_WARNING, TYPE_NOTICE]


class Error(Exception):
    pass
