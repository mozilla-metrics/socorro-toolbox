"""Library for adding 'date' options to optparse."""

from optparse import Option, OptionValueError
from datetime import datetime, date
from copy import copy
import re

patterns = [
    (re.compile(r'\d{6}$'), '%y%m%d'),
    (re.compile(r'\d{8}$'), '%Y%m%d'),
    (re.compile(r'\d{4}-\d{2}-\d{2}$'), '%Y-%m-%d'),
]

def check_date(option, opt, value):
    for pattern, fmt in patterns:
        if pattern.match(value) is not None:
            try:
                return datetime.strptime(value, fmt).date()
            except ValueError:
                pass

    raise OptionValueError("option %s: could not interpret %r as a date" % (opt, value))

class OptionWithDate(Option):
    TYPES = Option.TYPES + ('date',)
    TYPE_CHECKER = copy(Option.TYPE_CHECKER)
    TYPE_CHECKER['date'] = check_date

if __name__ == '__main__':
    import unittest

    class TestDateOption(unittest.TestCase):
        tests = [
            ("130301", (2013, 3, 1)),
            ("20130301", (2013, 3, 1)),
            ("2013-03-01", (2013, 3, 1)),
            ]
        throwtests = [
            ("20130300"),
            ("201303011"),
            ("1303011"),
            ("2013030x"),
            ]

        def test_dates(self):
            for s, (y, m, d) in self.tests:
                self.assertEqual(check_date(None, 'test', s), date(y, m, d))

            for s in self.throwtests:
                self.assertRaises(OptionValueError, check_date, None, 'test', s)

    unittest.main()
