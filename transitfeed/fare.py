#!/usr/bin/python2.5

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

from problems import default_problem_reporter
import util

class Fare(object):
  """Represents a fare type."""
  _REQUIRED_FIELD_NAMES = ['fare_id', 'price', 'currency_type',
                           'payment_method', 'transfers']
  _FIELD_NAMES = _REQUIRED_FIELD_NAMES + ['transfer_duration']

  def __init__(self,
               fare_id=None, price=None, currency_type=None,
               payment_method=None, transfers=None, transfer_duration=None,
               field_list=None):
    self.rules = []
    (self.fare_id, self.price, self.currency_type, self.payment_method,
     self.transfers, self.transfer_duration) = \
     (fare_id, price, currency_type, payment_method,
      transfers, transfer_duration)
    if field_list:
      (self.fare_id, self.price, self.currency_type, self.payment_method,
       self.transfers, self.transfer_duration) = field_list

    try:
      self.price = float(self.price)
    except (TypeError, ValueError):
      pass
    try:
      self.payment_method = int(self.payment_method)
    except (TypeError, ValueError):
      pass
    if self.transfers == None or self.transfers == "":
      self.transfers = None
    else:
      try:
        self.transfers = int(self.transfers)
      except (TypeError, ValueError):
        pass
    if self.transfer_duration == None or self.transfer_duration == "":
      self.transfer_duration = None
    else:
      try:
        self.transfer_duration = int(self.transfer_duration)
      except (TypeError, ValueError):
        pass

  def GetFareRuleList(self):
    return self.rules

  def ClearFareRules(self):
    self.rules = []

  def GetFieldValuesTuple(self):
    return [getattr(self, fn) for fn in Fare._FIELD_NAMES]

  def __getitem__(self, name):
    return getattr(self, name)

  def __eq__(self, other):
    if not other:
      return False

    if id(self) == id(other):
      return True

    if self.GetFieldValuesTuple() != other.GetFieldValuesTuple():
      return False

    self_rules = [r.GetFieldValuesTuple() for r in self.GetFareRuleList()]
    self_rules.sort()
    other_rules = [r.GetFieldValuesTuple() for r in other.GetFareRuleList()]
    other_rules.sort()
    return self_rules == other_rules

  def __ne__(self, other):
    return not self.__eq__(other)

  def Validate(self, problems=default_problem_reporter):
    if util.IsEmpty(self.fare_id):
      problems.MissingValue("fare_id")

    if self.price == None:
      problems.MissingValue("price")
    elif not isinstance(self.price, float) and not isinstance(self.price, int):
      problems.InvalidValue("price", self.price)
    elif self.price < 0:
      problems.InvalidValue("price", self.price)

    if util.IsEmpty(self.currency_type):
      problems.MissingValue("currency_type")
    elif self.currency_type not in ISO4217.codes:
      problems.InvalidValue("currency_type", self.currency_type)

    if self.payment_method == "" or self.payment_method == None:
      problems.MissingValue("payment_method")
    elif (not isinstance(self.payment_method, int) or
          self.payment_method not in range(0, 2)):
      problems.InvalidValue("payment_method", self.payment_method)

    if not ((self.transfers == None) or
            (isinstance(self.transfers, int) and
             self.transfers in range(0, 3))):
      problems.InvalidValue("transfers", self.transfers)

    if ((self.transfer_duration != None) and
        not isinstance(self.transfer_duration, int)):
      problems.InvalidValue("transfer_duration", self.transfer_duration)
    if self.transfer_duration and (self.transfer_duration < 0):
      problems.InvalidValue("transfer_duration", self.transfer_duration)
    if (self.transfer_duration and (self.transfer_duration > 0) and
        self.transfers == 0):
      problems.InvalidValue("transfer_duration", self.transfer_duration,
                            "can't have a nonzero transfer_duration for "
                            "a fare that doesn't allow transfers!")

# TODO: move these into a separate file
class ISO4217(object):
  """Represents the set of currencies recognized by the ISO-4217 spec."""
  codes = {  # map of alpha code to numerical code
    'AED': 784, 'AFN': 971, 'ALL':   8, 'AMD':  51, 'ANG': 532, 'AOA': 973,
    'ARS':  32, 'AUD':  36, 'AWG': 533, 'AZN': 944, 'BAM': 977, 'BBD':  52,
    'BDT':  50, 'BGN': 975, 'BHD':  48, 'BIF': 108, 'BMD':  60, 'BND':  96,
    'BOB':  68, 'BOV': 984, 'BRL': 986, 'BSD':  44, 'BTN':  64, 'BWP':  72,
    'BYR': 974, 'BZD':  84, 'CAD': 124, 'CDF': 976, 'CHE': 947, 'CHF': 756,
    'CHW': 948, 'CLF': 990, 'CLP': 152, 'CNY': 156, 'COP': 170, 'COU': 970,
    'CRC': 188, 'CUP': 192, 'CVE': 132, 'CYP': 196, 'CZK': 203, 'DJF': 262,
    'DKK': 208, 'DOP': 214, 'DZD':  12, 'EEK': 233, 'EGP': 818, 'ERN': 232,
    'ETB': 230, 'EUR': 978, 'FJD': 242, 'FKP': 238, 'GBP': 826, 'GEL': 981,
    'GHC': 288, 'GIP': 292, 'GMD': 270, 'GNF': 324, 'GTQ': 320, 'GYD': 328,
    'HKD': 344, 'HNL': 340, 'HRK': 191, 'HTG': 332, 'HUF': 348, 'IDR': 360,
    'ILS': 376, 'INR': 356, 'IQD': 368, 'IRR': 364, 'ISK': 352, 'JMD': 388,
    'JOD': 400, 'JPY': 392, 'KES': 404, 'KGS': 417, 'KHR': 116, 'KMF': 174,
    'KPW': 408, 'KRW': 410, 'KWD': 414, 'KYD': 136, 'KZT': 398, 'LAK': 418,
    'LBP': 422, 'LKR': 144, 'LRD': 430, 'LSL': 426, 'LTL': 440, 'LVL': 428,
    'LYD': 434, 'MAD': 504, 'MDL': 498, 'MGA': 969, 'MKD': 807, 'MMK': 104,
    'MNT': 496, 'MOP': 446, 'MRO': 478, 'MTL': 470, 'MUR': 480, 'MVR': 462,
    'MWK': 454, 'MXN': 484, 'MXV': 979, 'MYR': 458, 'MZN': 943, 'NAD': 516,
    'NGN': 566, 'NIO': 558, 'NOK': 578, 'NPR': 524, 'NZD': 554, 'OMR': 512,
    'PAB': 590, 'PEN': 604, 'PGK': 598, 'PHP': 608, 'PKR': 586, 'PLN': 985,
    'PYG': 600, 'QAR': 634, 'ROL': 642, 'RON': 946, 'RSD': 941, 'RUB': 643,
    'RWF': 646, 'SAR': 682, 'SBD':  90, 'SCR': 690, 'SDD': 736, 'SDG': 938,
    'SEK': 752, 'SGD': 702, 'SHP': 654, 'SKK': 703, 'SLL': 694, 'SOS': 706,
    'SRD': 968, 'STD': 678, 'SYP': 760, 'SZL': 748, 'THB': 764, 'TJS': 972,
    'TMM': 795, 'TND': 788, 'TOP': 776, 'TRY': 949, 'TTD': 780, 'TWD': 901,
    'TZS': 834, 'UAH': 980, 'UGX': 800, 'USD': 840, 'USN': 997, 'USS': 998,
    'UYU': 858, 'UZS': 860, 'VEB': 862, 'VND': 704, 'VUV': 548, 'WST': 882,
    'XAF': 950, 'XAG': 961, 'XAU': 959, 'XBA': 955, 'XBB': 956, 'XBC': 957,
    'XBD': 958, 'XCD': 951, 'XDR': 960, 'XFO': None, 'XFU': None, 'XOF': 952,
    'XPD': 964, 'XPF': 953, 'XPT': 962, 'XTS': 963, 'XXX': 999, 'YER': 886,
    'ZAR': 710, 'ZMK': 894, 'ZWD': 716,
  }
