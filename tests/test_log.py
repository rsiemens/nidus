import os
from unittest import TestCase

from nidus.log import LogEntry, Pager, append_entries


class PagerTestCases(TestCase):
    test_log_file = "test_log.db"

    def tearDown(self):
        if os.path.exists(self.test_log_file):
            os.remove(self.test_log_file)

    def fill_pages(self, pager, n_pages):
        entry = LogEntry(1, "hello world")
        entry_bytes = entry.to_bytes()
        entries_per_page = (pager.page_size - 2) // len(entry_bytes)

        for _ in range(n_pages):
            for _ in range(entries_per_page):
                pager.append_log_entry(entry)

    def test_get_last_page(self):
        pager = Pager(self.test_log_file)

        self.assertIsNone(pager.cached_last_page)
        page = pager.get_last_page()
        expected_page_num = 0
        expected_remaining = pager.page_size - 2
        expected_data = b"\x00" * expected_remaining

        self.assertEqual(page, (expected_page_num, expected_remaining, expected_data))
        self.assertEqual(
            pager.cached_last_page,
            (expected_page_num, expected_remaining, expected_data),
        )

    def test_append_log_entry(self):
        pager = Pager(self.test_log_file)
        entry = LogEntry(1, ["SET", "hello", "world"])
        entry_bytes = entry.to_bytes()

        pager.append_log_entry(entry)
        expected_page_num = 0
        expected_remaining = pager.page_size - 2 - len(entry_bytes)
        expected_data = (
            b'\x00\x00\x00\x01\x00\x00\x00\x17["SET","hello","world"]'
            + b"\x00" * expected_remaining
        )

        self.assertEqual(
            pager.get_last_page(),
            (expected_page_num, expected_remaining, expected_data),
        )

        pager.cached_last_page = None
        self.assertEqual(
            pager.get_last_page(),
            (expected_page_num, expected_remaining, expected_data),
        )

    def test_append_log_entry_many_pages(self):
        pager = Pager(self.test_log_file)
        self.fill_pages(pager, 4)
        pages = [p for p in pager]
        self.assertEqual(len(pages), 4)


class AppendEntriesTestCases(TestCase):
    def log_from_list(self, term_list):
        return [LogEntry(i, "_") for i in term_list]

    def test_linear_appends(self):
        log = []
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

        self.assertEqual(log, expected)

    def test_many_appends(self):
        # many appends
        log = []
        append_entries(
            log, -1, 0, [LogEntry(1, "one"), LogEntry(1, "two"), LogEntry(2, "three")]
        )
        self.assertEqual(
            log, [LogEntry(1, "one"), LogEntry(1, "two"), LogEntry(2, "three")]
        )

        append_entries(log, 2, 2, [LogEntry(3, "four"), LogEntry(3, "five")])
        self.assertEqual(
            log,
            [
                LogEntry(1, "one"),
                LogEntry(1, "two"),
                LogEntry(2, "three"),
                LogEntry(3, "four"),
                LogEntry(3, "five"),
            ],
        )

    def test_holes_fail(self):
        log = []
        result = append_entries(log, -1, 0, [LogEntry(1, "one")])
        self.assertTrue(result)

        # fails because prev_index doesn't exist
        result = append_entries(log, 10, 1, [LogEntry(1, "eleven")])
        self.assertFalse(result)

        self.assertEqual(log, [LogEntry(1, "one")])

    def test_prev_term_mismatch_fails(self):
        log = [LogEntry(1, "one")]
        # matching prev_index, but mismatched term
        result = append_entries(log, 0, 2, [LogEntry(3, "three")])
        self.assertFalse(result)
        self.assertEqual(log, [LogEntry(1, "one")])

    def test_idempotent_append(self):
        log = []
        # idempotent for the first item
        append_entries(log, -1, 0, [LogEntry(1, "one")])
        append_entries(log, -1, 0, [LogEntry(1, "one")])
        self.assertEqual(log, [LogEntry(1, "one")])

        # idempotent for other indexes
        for i in range(10):
            append_entries(log, 0, 1, [LogEntry(1, "two")])
        self.assertEqual(log, [LogEntry(1, "one"), LogEntry(1, "two")])

    def test_append_empty_entries(self):
        log = []

        # append at index 0 would succeed
        result = append_entries(log, -1, 0, [])
        self.assertTrue(result)
        self.assertEqual(log, [])

        # append at index 1 would not succeed since index 0 doesn't exist
        result = append_entries(log, 0, 1, [])
        self.assertFalse(result)
        self.assertEqual(log, [])

        log = [LogEntry(1, "one")]
        # now it would succeed since index 0 exists
        result = append_entries(log, 0, 1, [])
        self.assertTrue(result)
        self.assertEqual(log, [LogEntry(1, "one")])

        # append way off ahead fails
        result = append_entries(log, 10, 1, [])
        self.assertFalse(result)
        self.assertEqual(log, [LogEntry(1, "one")])

    def test_existing_entry_term_mismatch(self):
        log = [
            LogEntry(1, "one"),
            LogEntry(1, "two"),
            LogEntry(1, "three"),
            LogEntry(1, "four"),
        ]
        result = append_entries(log, 0, 1, [LogEntry(2, "two"), LogEntry(2, "three")])
        self.assertTrue(result)
        self.assertEqual(
            log, [LogEntry(1, "one"), LogEntry(2, "two"), LogEntry(2, "three")]
        )

        result = append_entries(log, -1, 0, [LogEntry(3, "a"), LogEntry(3, "b")])
        self.assertTrue(result)
        self.assertEqual(log, [LogEntry(3, "a"), LogEntry(3, "b")])

        log = [
            LogEntry(1, 1),
            LogEntry(4, 4),
            LogEntry(5, 5),
            LogEntry(6, 6),
            LogEntry(7, 7),
        ]
        result = append_entries(log, 3, 6, [])
        self.assertTrue(result)
        self.assertEqual(
            log, [LogEntry(1, 1), LogEntry(4, 4), LogEntry(5, 5), LogEntry(6, 6)]
        )
