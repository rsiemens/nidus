import os
from unittest import TestCase

from nidus.log import (
    Log,
    LogEntry,
    Page,
    PageEmptyException,
    PageFullException,
    Pager,
    append_entries,
)


class PageTestCases(TestCase):
    def test_load(self):
        entry1 = LogEntry(term=1, item="hello").to_bytes()
        entry2 = LogEntry(term=1, item="world").to_bytes()
        size = len(entry1) + len(entry2)
        page_size = 64
        # -2 is because `remaining_space` takes 2 bytes
        page = Page(num=0, remaining_space=page_size - size - 2, data=entry1 + entry2)

        self.assertEqual(len(page.log), 2)
        self.assertEqual(
            page.log, [LogEntry(term=1, item="hello"), LogEntry(term=1, item="world")]
        )

    def test_append(self):
        page = Page(num=0, remaining_space=64 - 2, data=b"")
        entry = LogEntry(term=2, item="hello")

        # each entry is 15bytes so 4 is the most we can fit on the page
        for i in range(4):
            page.append(entry)
            self.assertEqual(len(page.log), i + 1)
            self.assertEqual(len(page.data), len(entry.to_bytes()) * (i + 1))
            self.assertEqual(
                page.remaining_space, 64 - 2 - len(entry.to_bytes()) * (i + 1)
            )

        with self.assertRaises(PageFullException):
            page.append(entry)
        self.assertEqual(len(page.log), 4)

    def test_pop(self):
        page = Page(num=0, remaining_space=64 - 2, data=b"")
        entry = LogEntry(term=2, item="hello")
        for i in range(4):
            page.append(entry)

        self.assertEqual(len(page.log), 4)
        for i in range(4):
            expected = 4 - (i + 1)
            self.assertEqual(page.pop(), entry)
            self.assertEqual(len(page.log), expected)
            self.assertEqual(len(page.data), len(entry.to_bytes()) * expected)
            self.assertEqual(
                page.remaining_space, 64 - 2 - len(entry.to_bytes()) * expected
            )

        with self.assertRaises(PageEmptyException):
            page.pop()

        self.assertEqual(len(page.log), 0)
        self.assertEqual(page.remaining_space, 62)
        self.assertEqual(page.data, b"")


class PagerTestCases(TestCase):
    test_log_file = "test_log.log"

    def tearDown(self):
        if os.path.exists(self.test_log_file):
            os.remove(self.test_log_file)

    def write_pages(self, pager, num_pages):
        pages = []

        for i in range(num_pages):
            page = Page(i, pager.page_size - 2, b"")
            page.append(LogEntry(term=1, item="a" * 50))  # serializes to 60 bytes
            pager.write(page)
            pages.append(page)

        return pages

    def test_write(self):
        pager = Pager(self.test_log_file, page_size=64)
        self.write_pages(pager, 3)

        with open(self.test_log_file, "rb") as f:
            data = f.read()
            self.assertEqual(len(data), 64 * 3)

    def test_read(self):
        pager = Pager(self.test_log_file, page_size=64)
        self.write_pages(pager, 3)

        read_p0 = pager.read(0)
        self.assertEqual(read_p0.num, 0)

        read_p1 = pager.read(1)
        self.assertEqual(read_p1.num, 1)

        read_p2 = pager.read(2)
        self.assertEqual(read_p2.num, 2)

        self.assertEqual(pager.read(3), None)

        read_p1 = pager.read(1)
        self.assertEqual(read_p1.num, 1)

    def test_truncate(self):
        pager = Pager(self.test_log_file, page_size=64)
        pages = self.write_pages(pager, 3)

        pager.truncate(pages[2])
        with open(self.test_log_file, "rb") as f:
            data = f.read()
            self.assertEqual(len(data), 64 * 2)

        pager.truncate(pages[0])
        with open(self.test_log_file, "rb") as f:
            data = f.read()
            self.assertEqual(len(data), 0)

    def test_iter(self):
        pager = Pager(self.test_log_file, page_size=64)
        self.write_pages(pager, 5)

        pages = [p for p in pager]
        self.assertEqual(len(pages), 5)

        for i, page in enumerate(pages):
            self.assertEqual(page.num, i)


class AppendEntriesTestCases(TestCase):
    test_log_file = "test_log.log"

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
