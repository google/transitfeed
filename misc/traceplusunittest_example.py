import unittest


class TestStringMethods(unittest.TestCase):
    def test_upper(self):
        self.assertEqual("foo".upper(), "FOO")

    def test_split(self):
        s = "hello world"
        self.assertEqual(s.split(), ["hello", "hello"])  # This will fail


if __name__ == "__main__":
    try:
        import traceplusunittest
    except ImportError:
        unittest.main()
    else:
        traceplusunittest.main()
