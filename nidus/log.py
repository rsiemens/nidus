import io
import json
import os
from struct import pack, unpack
from typing import NamedTuple


class PageFullException(Exception):
    pass


class PageEmptyException(Exception):
    pass


class Pager:
    """
    2048 bytes page size.

    Offset | Bytes | Description
    -------+-------+--------------------------------
       0   |   2   |  Remaining space on the page
    -------+-------+--------------------------------
       2   |   4   |  Term number for a log entry
    -------+-------+--------------------------------
       6   |   4   |  Size of log entry item in bytes
    -------+-------+--------------------------------
      10   |   N   |  Log entry item
    -------+-------+--------------------------------

    The log entry portion of a page is repeated until the page can't fit anymore entries
    """

    def __init__(self, log_file, page_size=2048):
        self.page_size = page_size
        self.log_file = log_file

        if not os.path.exists(log_file):
            with open(log_file, "wb+"):
                pass

    def read(self, page_num):
        with open(self.log_file, "rb+") as f:
            f.seek(self.page_size * page_num)
            raw_page = f.read(self.page_size)

        if raw_page:
            remaining_space = unpack(">H", raw_page[:2])[0]
            # lop off the null bytes
            data = raw_page[2:-remaining_space]
            page = Page(page_num, remaining_space, data)
        else:
            page = None

        return page

    def write(self, page):
        with open(self.log_file, "rb+") as f:
            f.seek(self.page_size * page.num)
            raw_page = pack(">H", page.remaining_space) + page.data
            # buffer remaining space with null bytes
            raw_page += b"\x00" * page.remaining_space
            f.write(raw_page)

    def truncate(self, page):
        # deletes the page and all following pages!

        with open(self.log_file, "rb+") as f:
            f.seek(self.page_size * page.num)
            f.truncate()

    def __iter__(self):
        page_num = 0
        with open(self.log_file, "rb") as f:
            while True:
                raw_page = f.read(self.page_size)

                if not raw_page:
                    break

                remaining_space = unpack(">H", raw_page[:2])[0]
                # lop off the null bytes
                data = raw_page[2:-remaining_space]
                yield Page(page_num, remaining_space, data)
                page_num += 1


class Page:
    def __init__(self, num, remaining_space, data):
        self.num = num
        self.remaining_space = remaining_space
        self.data = data
        self.log = []
        self.load()

    def load(self):
        cursor = 0
        end = len(self.data) - self.remaining_space
        while cursor < end:
            term, item_size = unpack(">LL", self.data[cursor : cursor + 8])
            cursor += 8
            item = self.data[cursor : cursor + item_size]
            cursor += item_size

            entry = LogEntry(term, json.loads(item))
            self.log.append(entry)

    def append(self, entry):
        serialized = entry.to_bytes()

        if len(serialized) > self.remaining_space:
            raise PageFullException()

        self.data += serialized
        self.remaining_space -= len(serialized)
        self.log.append(entry)

    def pop(self):
        if len(self.log) == 0:
            raise PageEmptyException()

        entry = self.log.pop()
        serialized = entry.to_bytes()
        self.data = self.data[: -len(serialized)]
        self.remaining_space += len(serialized)


class Log:
    def __init__(self, log_file):
        self._pager = Pager(log_file)
        self.pages = []
        self.load()

    def load(self):
        for page in self._pager:
            self.pages.append(page)

        if len(self.pages) == 0:
            self.pages = [Page(0, self._pager.page_size - 2, b"")]

    def append(self, entry):
        page = self.pages[-1]
        try:
            page.append(entry)
        except PageFullException:
            page = Page(page.num + 1, self._pager.page_size - 2, b"")
            page.append(entry)
            self.pages.append(page)

        self._pager.write(page)

    def pop(self):
        page = self.pages[-1]
        try:
            page.pop()
        except PageEmptyException:
            page = self.pages.pop()
            self._pager.truncate(page)
            page = self.pages[-1]
            page.pop()

        self._pager.write(page)

    def as_list(self):
        log = []
        for page in self.pages:
            log += page.log
        return log

    def __getitem__(self, idx):
        if isinstance(idx, slice):
            # only supports [n:] style slices
            items = []
            start = idx.start
            for page in self.pages:
                if len(page.log) > start:
                    items.append(page.log[start])
                else:
                    start -= len(page.log)
            return items

        real_idx = idx
        for page in self.pages:
            if len(page.log) > real_idx:
                return page.log[real_idx]
            else:
                real_idx -= len(page.log)
        raise IndexError("index out of range")

    def __len__(self):
        size = 0
        for page in self.pages:
            size += len(page.log)
        return size

    def __repr__(self):
        log = []
        for page in self.pages:
            log += page.log
        return str(log)


class LogEntry:
    def __init__(self, term, item):
        self.term = term
        self.item = item

    def __eq__(self, other):
        return self.term == other.term and self.item == other.item

    def __repr__(self):
        return f"<LogEntry term={self.term} item={self.item}>"

    def to_bytes(self):
        item_json = json.dumps(self.item, separators=(",", ":")).encode("utf8")
        return pack(">LL", self.term, len(item_json)) + item_json


def append_entries(log, prev_index, prev_term, entries):
    # No holes in the log!
    if prev_index >= len(log):
        return False

    # Special case, appending entries at index 0 (prev_index -1) always works
    # prev_term doen't matter in this case.
    if prev_index == -1:
        apply_all_entries(log, prev_index, entries)
        return True
    # Reply True if log contains an entry at prev_index whose term matches prev_term
    elif len(log) > prev_index and log[prev_index].term == prev_term:
        apply_all_entries(log, prev_index, entries)
        return True
    return False


def apply_all_entries(log, prev_index, entries):
    # If we have more than the prev_index entries in our log then those are
    # stale / uncommited due to a leader change and failure to replicate.
    # Delete everything after prev_index.
    if len(log) > prev_index + 1:
        clear_upto(log, prev_index + 1)

    for entry in entries:
        log.append(entry)


def clear_upto(log, upto):
    while len(log) > upto:
        log.pop()
