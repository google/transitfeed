import os.path
import unittest

import visualize_pathways


def get_file_contents(filename):
    with open(filename, "rt", encoding="utf-8") as f:
        return f.read()


class TestVisualizePathways(unittest.TestCase):
    def test_gtfs_to_graphviz(self):
        testdata_dir = os.path.join(
            os.path.dirname(__file__), "data/au-sydney-entrances"
        )
        golden_data = get_file_contents(
            os.path.join(testdata_dir, "au-sydney-entrances.dot")
        )
        reader = visualize_pathways.GtfsReader(testdata_dir)
        self.assertEqual(
            str(visualize_pathways.gtfs_to_graphviz(reader)), golden_data
        )
