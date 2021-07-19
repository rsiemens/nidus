import os
from unittest import TestCase

from nidus.log import Log, LogEntry, Pager, append_entries


class AppendEntriesTestCases(TestCase):
    test_log_file = "test_log.db"

    def tearDown(self):
        if os.path.exists(self.test_log_file):
            os.remove(self.test_log_file)

    def test_linear_appends(self):
        log = Log(self.test_log_file)
        expected = [
            LogEntry(1, "one"),
            LogEntry(1, "two"),
            LogEntry(1, "three"),
            LogEntry(2, "four"),
            LogEntry(2, "five"),
        ]
        # initial
        append_entries(log, -1, 0, [LogEntry(1, "one")])
        # index increment, term remains
        append_entries(log, 0, 1, [LogEntry(1, "two")])
        append_entries(log, 1, 1, [LogEntry(1, "three")])
        # index increment and term changes
        append_entries(log, 2, 1, [LogEntry(2, "four")])
        append_entries(log, 3, 2, [LogEntry(2, "five")])

        self.assertEqual(log.as_list(), expected)

    def test_many_appends(self):
        # many appends
        log = Log(self.test_log_file)
        append_entries(
            log, -1, 0, [LogEntry(1, "one"), LogEntry(1, "two"), LogEntry(2, "three")]
        )
        self.assertEqual(
            log.as_list(),
            [LogEntry(1, "one"), LogEntry(1, "two"), LogEntry(2, "three")],
        )

        append_entries(log, 2, 2, [LogEntry(3, "four"), LogEntry(3, "five")])
        self.assertEqual(
            log.as_list(),
            [
                LogEntry(1, "one"),
                LogEntry(1, "two"),
                LogEntry(2, "three"),
                LogEntry(3, "four"),
                LogEntry(3, "five"),
            ],
        )

    def test_holes_fail(self):
        log = Log(self.test_log_file)
        result = append_entries(log, -1, 0, [LogEntry(1, "one")])
        self.assertTrue(result)

        # fails because prev_index doesn't exist
        result = append_entries(log, 10, 1, [LogEntry(1, "eleven")])
        self.assertFalse(result)

        self.assertEqual(log.as_list(), [LogEntry(1, "one")])

    def test_prev_term_mismatch_fails(self):
        log = Log(self.test_log_file)
        log.append(LogEntry(1, "one"))
        # matching prev_index, but mismatched term
        result = append_entries(log, 0, 2, [LogEntry(3, "three")])
        self.assertFalse(result)
        self.assertEqual(log.as_list(), [LogEntry(1, "one")])

    def test_idempotent_append(self):
        log = Log(self.test_log_file)
        # idempotent for the first item
        append_entries(log, -1, 0, [LogEntry(1, "one")])
        append_entries(log, -1, 0, [LogEntry(1, "one")])
        self.assertEqual(log.as_list(), [LogEntry(1, "one")])

        # idempotent for other indexes
        for i in range(10):
            append_entries(log, 0, 1, [LogEntry(1, "two")])
        self.assertEqual(log.as_list(), [LogEntry(1, "one"), LogEntry(1, "two")])

    def test_append_empty_entries(self):
        log = Log(self.test_log_file)

        # append at index 0 would succeed
        result = append_entries(log, -1, 0, [])
        self.assertTrue(result)
        self.assertEqual(log.as_list(), [])

        # append at index 1 would not succeed since index 0 doesn't exist
        result = append_entries(log, 0, 1, [])
        self.assertFalse(result)
        self.assertEqual(log.as_list(), [])

        os.remove(self.test_log_file)
        log = Log(self.test_log_file)
        log.append(LogEntry(1, "one"))
        # now it would succeed since index 0 exists
        result = append_entries(log, 0, 1, [])
        self.assertTrue(result)
        self.assertEqual(log.as_list(), [LogEntry(1, "one")])

        # append way off ahead fails
        result = append_entries(log, 10, 1, [])
        self.assertFalse(result)
        self.assertEqual(log.as_list(), [LogEntry(1, "one")])

    def test_existing_entry_term_mismatch(self):
        log = Log(self.test_log_file)
        for entry in [
            LogEntry(1, "one"),
            LogEntry(1, "two"),
            LogEntry(1, "three"),
            LogEntry(1, "four"),
        ]:
            log.append(entry)

        result = append_entries(log, 0, 1, [LogEntry(2, "two"), LogEntry(2, "three")])
        self.assertTrue(result)
        self.assertEqual(
            log.as_list(),
            [LogEntry(1, "one"), LogEntry(2, "two"), LogEntry(2, "three")],
        )

        result = append_entries(log, -1, 0, [LogEntry(3, "a"), LogEntry(3, "b")])
        self.assertTrue(result)
        self.assertEqual(log.as_list(), [LogEntry(3, "a"), LogEntry(3, "b")])

        os.remove(self.test_log_file)
        log = Log(self.test_log_file)
        for entry in [
            LogEntry(1, 1),
            LogEntry(4, 4),
            LogEntry(5, 5),
            LogEntry(6, 6),
            LogEntry(7, 7),
        ]:
            log.append(entry)
        result = append_entries(log, 3, 6, [])
        self.assertTrue(result)
        self.assertEqual(
            log.as_list(),
            [LogEntry(1, 1), LogEntry(4, 4), LogEntry(5, 5), LogEntry(6, 6)],
        )
