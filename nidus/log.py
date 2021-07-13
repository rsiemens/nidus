import io
import json
from struct import pack, unpack
from typing import NamedTuple


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

    def __init__(self, log_file):
        self.page_size = 2048
        self.log_file = log_file
        self.cached_last_page = None

        with open(log_file, "wb+"):
            pass

    def get_last_page(self):
        if self.cached_last_page:
            return self.cached_last_page

        page_num = 0
        remaining_space = 0
        data = None

        for n, space, page in self:
            page_num = n
            remaining_space = space
            data = page

        if data is None:
            remaining_space = self.page_size - 2
            data = b"\x00" * remaining_space

        self.cached_last_page = (page_num, remaining_space, data)
        return page_num, remaining_space, data

    def append_log_entry(self, entry):
        serialized = entry.to_bytes()
        serialized_size = len(serialized)
        page_num, remaining_space, data = self.get_last_page()

        data = data[: self.page_size - 2 - remaining_space] + serialized
        remaining_space -= serialized_size
        data += b"\x00" * remaining_space

        with open(self.log_file, "rb+") as f:
            f.seek(self.page_size * page_num)
            f.write(pack(">H", remaining_space) + data)

        self.cached_last_page = (page_num, remaining_space, data)

    def __iter__(self):
        page_num = 0
        with open(self.log_file, "rb") as f:
            while True:
                f.seek(self.page_size * page_num)
                page = f.read(self.page_size)

                if not page:
                    break

                remaining_space = unpack(">H", page[:2])[0]
                data = page[2:]
                yield page_num, remaining_space, data
                page_num += 1


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

    @classmethod
    def from_bytes(self):
        pass


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

    for i, entry in enumerate(entries, start=1):
        # The entry already exists so we are overriding as the leader may
        # be sending the same entry because it never recieved our response
        # (the idempotency property).
        if len(log) > prev_index + i:
            if log[prev_index + i].term < entry.term:
                log.append(entry)
            else:
                log[prev_index + i] = entry
        # New entry, append as normal
        else:
            log.append(entry)


def clear_upto(log, upto):
    while len(log) > upto:
        log.pop()
