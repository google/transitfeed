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

# Unit tests for the gtfsfactory module.


import transitfeed
from tests import util


class TestGtfsFactory(util.TestCase):
    def setUp(self):
        self._factory = transitfeed.GetGtfsFactory()

    def testCanUpdateMapping(self):
        self._factory.UpdateMapping(
            "agency.txt", {"required": False, "classes": ["Foo"]}
        )
        self._factory.RemoveClass("Agency")
        self._factory.AddClass("Foo", transitfeed.Stop)
        self._factory.UpdateMapping(
            "calendar.txt", {"loading_order": -4, "classes": ["Bar"]}
        )
        self._factory.AddClass("Bar", transitfeed.ServicePeriod)
        self.assertFalse(self._factory.IsFileRequired("agency.txt"))
        self.assertFalse(self._factory.IsFileRequired("calendar.txt"))
        self.assertTrue(self._factory.GetLoadingOrder()[0] == "calendar.txt")
        self.assertEqual(self._factory.Foo, transitfeed.Stop)
        self.assertEqual(self._factory.Bar, transitfeed.ServicePeriod)
        self.assertEqual(
            self._factory.GetGtfsClassByFileName("agency.txt"),
            transitfeed.Stop,
        )
        self.assertFalse(self._factory.IsFileRequired("agency.txt"))
        known_filenames = self._factory.GetKnownFilenames()
        self.assertTrue("agency.txt" in known_filenames)
        self.assertTrue("calendar.txt" in known_filenames)

    def testCanAddMapping(self):
        self._factory.AddMapping(
            "newrequiredfile.txt",
            {
                "required": True,
                "classes": ["NewRequiredClass"],
                "loading_order": -20,
            },
        )
        self._factory.AddClass("NewRequiredClass", transitfeed.Stop)
        self._factory.AddMapping(
            "newfile.txt",
            {"required": False, "classes": ["NewClass"], "loading_order": -10},
        )
        self._factory.AddClass("NewClass", transitfeed.FareAttribute)
        self.assertEqual(self._factory.NewClass, transitfeed.FareAttribute)
        self.assertEqual(self._factory.NewRequiredClass, transitfeed.Stop)
        self.assertTrue(self._factory.IsFileRequired("newrequiredfile.txt"))
        self.assertFalse(self._factory.IsFileRequired("newfile.txt"))
        known_filenames = self._factory.GetKnownFilenames()
        self.assertTrue("newfile.txt" in known_filenames)
        self.assertTrue("newrequiredfile.txt" in known_filenames)
        loading_order = self._factory.GetLoadingOrder()
        self.assertTrue(loading_order[0] == "newrequiredfile.txt")
        self.assertTrue(loading_order[1] == "newfile.txt")

    def testThrowsExceptionWhenAddingDuplicateMapping(self):
        self.assertRaises(
            transitfeed.DuplicateMapping,
            self._factory.AddMapping,
            "agency.txt",
            {"required": True, "classes": ["Stop"], "loading_order": -20},
        )

    def testThrowsExceptionWhenAddingInvalidMapping(self):
        self.assertRaises(
            transitfeed.InvalidMapping,
            self._factory.AddMapping,
            "foo.txt",
            {"required": True, "loading_order": -20},
        )

    def testThrowsExceptionWhenUpdatingNonexistentMapping(self):
        self.assertRaises(
            transitfeed.NonexistentMapping,
            self._factory.UpdateMapping,
            "doesnotexist.txt",
            {"required": False},
        )

    def testCanRemoveFileFromLoadingOrder(self):
        self._factory.UpdateMapping("agency.txt", {"loading_order": None})
        self.assertTrue("agency.txt" not in self._factory.GetLoadingOrder())

    def testCanRemoveMapping(self):
        self._factory.RemoveMapping("agency.txt")
        self.assertFalse("agency.txt" in self._factory.GetKnownFilenames())
        self.assertFalse("agency.txt" in self._factory.GetLoadingOrder())
        self.assertEqual(
            self._factory.GetGtfsClassByFileName("agency.txt"), None
        )
        self.assertFalse(self._factory.IsFileRequired("agency.txt"))

    def testIsFileRequired(self):
        self.assertTrue(self._factory.IsFileRequired("agency.txt"))
        self.assertTrue(self._factory.IsFileRequired("stops.txt"))
        self.assertTrue(self._factory.IsFileRequired("routes.txt"))
        self.assertTrue(self._factory.IsFileRequired("trips.txt"))
        self.assertTrue(self._factory.IsFileRequired("stop_times.txt"))

        # We don't have yet a way to specify that one or the other (or both
        # simultaneously) might be provided, so we don't consider them as required
        # for now
        self.assertFalse(self._factory.IsFileRequired("calendar.txt"))
        self.assertFalse(self._factory.IsFileRequired("calendar_dates.txt"))

        self.assertFalse(self._factory.IsFileRequired("fare_attributes.txt"))
        self.assertFalse(self._factory.IsFileRequired("fare_rules.txt"))
        self.assertFalse(self._factory.IsFileRequired("shapes.txt"))
        self.assertFalse(self._factory.IsFileRequired("frequencies.txt"))
        self.assertFalse(self._factory.IsFileRequired("transfers.txt"))

    def testFactoryReturnsClassesAndNotInstances(self):
        for filename in (
            "agency.txt",
            "fare_attributes.txt",
            "fare_rules.txt",
            "frequencies.txt",
            "stops.txt",
            "stop_times.txt",
            "transfers.txt",
            "routes.txt",
            "trips.txt",
        ):
            class_object = self._factory.GetGtfsClassByFileName(filename)
            self.assertTrue(
                isinstance(class_object, type),
                "The mapping from filenames to classes must return "
                "classes and not instances. This is not the case for "
                + filename,
            )

    def testCanFindClassByClassName(self):
        self.assertEqual(transitfeed.Agency, self._factory.Agency)
        self.assertEqual(
            transitfeed.FareAttribute, self._factory.FareAttribute
        )
        self.assertEqual(transitfeed.FareRule, self._factory.FareRule)
        self.assertEqual(transitfeed.Frequency, self._factory.Frequency)
        self.assertEqual(transitfeed.Route, self._factory.Route)
        self.assertEqual(
            transitfeed.ServicePeriod, self._factory.ServicePeriod
        )
        self.assertEqual(transitfeed.Shape, self._factory.Shape)
        self.assertEqual(transitfeed.ShapePoint, self._factory.ShapePoint)
        self.assertEqual(transitfeed.Stop, self._factory.Stop)
        self.assertEqual(transitfeed.StopTime, self._factory.StopTime)
        self.assertEqual(transitfeed.Transfer, self._factory.Transfer)
        self.assertEqual(transitfeed.Trip, self._factory.Trip)

    def testCanFindClassByFileName(self):
        self.assertEqual(
            transitfeed.Agency,
            self._factory.GetGtfsClassByFileName("agency.txt"),
        )
        self.assertEqual(
            transitfeed.FareAttribute,
            self._factory.GetGtfsClassByFileName("fare_attributes.txt"),
        )
        self.assertEqual(
            transitfeed.FareRule,
            self._factory.GetGtfsClassByFileName("fare_rules.txt"),
        )
        self.assertEqual(
            transitfeed.Frequency,
            self._factory.GetGtfsClassByFileName("frequencies.txt"),
        )
        self.assertEqual(
            transitfeed.Route,
            self._factory.GetGtfsClassByFileName("routes.txt"),
        )
        self.assertEqual(
            transitfeed.ServicePeriod,
            self._factory.GetGtfsClassByFileName("calendar.txt"),
        )
        self.assertEqual(
            transitfeed.ServicePeriod,
            self._factory.GetGtfsClassByFileName("calendar_dates.txt"),
        )
        self.assertEqual(
            transitfeed.Stop, self._factory.GetGtfsClassByFileName("stops.txt")
        )
        self.assertEqual(
            transitfeed.StopTime,
            self._factory.GetGtfsClassByFileName("stop_times.txt"),
        )
        self.assertEqual(
            transitfeed.Transfer,
            self._factory.GetGtfsClassByFileName("transfers.txt"),
        )
        self.assertEqual(
            transitfeed.Trip, self._factory.GetGtfsClassByFileName("trips.txt")
        )

    def testClassFunctionsRaiseExceptions(self):
        self.assertRaises(
            transitfeed.NonexistentMapping, self._factory.RemoveClass, "Agenci"
        )
        self.assertRaises(
            transitfeed.DuplicateMapping,
            self._factory.AddClass,
            "Agency",
            transitfeed.Agency,
        )
        self.assertRaises(
            transitfeed.NonStandardMapping,
            self._factory.GetGtfsClassByFileName,
            "shapes.txt",
        )
        self.assertRaises(
            transitfeed.NonexistentMapping,
            self._factory.UpdateClass,
            "Agenci",
            transitfeed.Agency,
        )


class TestGtfsFactoryUser(util.TestCase):
    def AssertDefaultFactoryIsReturnedIfNoneIsSet(self, instance):
        self.assertTrue(
            isinstance(instance.GetGtfsFactory(), transitfeed.GtfsFactory)
        )

    def AssertFactoryIsSavedAndReturned(self, instance, factory):
        instance.SetGtfsFactory(factory)
        self.assertEqual(factory, instance.GetGtfsFactory())

    def testClasses(self):
        class FakeGtfsFactory(object):
            pass

        factory = transitfeed.GetGtfsFactory()
        gtfs_class_instances = [factory.Shape("id"), factory.ShapePoint()]
        gtfs_class_instances += [
            factory.GetGtfsClassByFileName(filename)()
            for filename in factory.GetLoadingOrder()
        ]

        for instance in gtfs_class_instances:
            self.AssertDefaultFactoryIsReturnedIfNoneIsSet(instance)
            self.AssertFactoryIsSavedAndReturned(instance, FakeGtfsFactory())
