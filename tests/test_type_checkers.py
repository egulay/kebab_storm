import unittest

from util.type_checkers import is_not_blank, is_timestamp


class TypeCheckersTest(unittest.TestCase):
    def test_none_and_blank_values_are_not_nonblank(self):
        self.assertFalse(is_not_blank(None))
        self.assertFalse(is_not_blank('  '))
        self.assertTrue(is_not_blank('value'))

    def test_timestamp_requires_a_parseable_nonblank_value(self):
        self.assertFalse(is_timestamp(None))
        self.assertFalse(is_timestamp(''))
        self.assertFalse(is_timestamp('not-a-date'))
        self.assertTrue(is_timestamp('2026-07-21'))


if __name__ == '__main__':
    unittest.main()
