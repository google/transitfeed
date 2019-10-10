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

# Unit tests for the fareattribute module.


import transitfeed
from tests import util


class FareAttributeValidationTestCase(util.ValidationTestCase):
    def runTest(self):
        fare = transitfeed.FareAttribute()
        fare.fare_id = "normal"
        fare.price = 1.50
        fare.currency_type = "USD"
        fare.payment_method = 0
        fare.transfers = 1
        fare.transfer_duration = 7200
        fare.Validate(self.problems)

        fare.fare_id = None
        self.ValidateAndExpectMissingValue(fare, "fare_id")
        fare.fare_id = ""
        self.ValidateAndExpectMissingValue(fare, "fare_id")
        fare.fare_id = "normal"

        fare.price = "1.50"
        self.ValidateAndExpectInvalidValue(fare, "price")
        fare.price = 1
        fare.Validate(self.problems)
        fare.price = None
        self.ValidateAndExpectMissingValue(fare, "price")
        fare.price = 0.0
        fare.Validate(self.problems)
        fare.price = -1.50
        self.ValidateAndExpectInvalidValue(fare, "price")
        fare.price = 1.50

        fare.currency_type = ""
        self.ValidateAndExpectMissingValue(fare, "currency_type")
        fare.currency_type = None
        self.ValidateAndExpectMissingValue(fare, "currency_type")
        fare.currency_type = "usd"
        self.ValidateAndExpectInvalidValue(fare, "currency_type")
        fare.currency_type = "KML"
        self.ValidateAndExpectInvalidValue(fare, "currency_type")
        fare.currency_type = "USD"

        fare.payment_method = "0"
        self.ValidateAndExpectInvalidValue(fare, "payment_method")
        fare.payment_method = -1
        self.ValidateAndExpectInvalidValue(fare, "payment_method")
        fare.payment_method = 1
        fare.Validate(self.problems)
        fare.payment_method = 2
        self.ValidateAndExpectInvalidValue(fare, "payment_method")
        fare.payment_method = None
        self.ValidateAndExpectMissingValue(fare, "payment_method")
        fare.payment_method = ""
        self.ValidateAndExpectMissingValue(fare, "payment_method")
        fare.payment_method = 0

        fare.transfers = "1"
        self.ValidateAndExpectInvalidValue(fare, "transfers")
        fare.transfers = -1
        self.ValidateAndExpectInvalidValue(fare, "transfers")
        fare.transfers = 2
        fare.Validate(self.problems)
        fare.transfers = 3
        self.ValidateAndExpectInvalidValue(fare, "transfers")
        fare.transfers = None
        fare.Validate(self.problems)
        fare.transfers = 1

        fare.transfer_duration = 0
        fare.Validate(self.problems)
        fare.transfer_duration = None
        fare.Validate(self.problems)
        fare.transfer_duration = -3600
        self.ValidateAndExpectInvalidValue(fare, "transfer_duration")
        fare.transfers = 0  # no transfers allowed and duration specified!
        fare.transfer_duration = 3600
        fare.Validate(self.problems)
        fare.transfers = 1
        fare.transfer_duration = "3600"
        self.ValidateAndExpectInvalidValue(fare, "transfer_duration")
        fare.transfer_duration = 7200
        self.accumulator.AssertNoMoreExceptions()
