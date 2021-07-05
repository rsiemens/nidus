class LogEntry:
    def __init__(self, term, item):
        self.term = term
        self.item = item

    def __eq__(self, other):
        return self.term == other.term and self.item == other.item

    def __repr__(self):
        return f"<LogEntry term={self.term} item={self.item}>"


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
