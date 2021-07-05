from unittest import TestCase
from unittest.mock import Mock

from nidus.log import LogEntry
from nidus.state import RaftState


class RaftStateTestCases(TestCase):
    def test_append_entires_success(self):
        state = RaftState()
        state.status = state.FOLLOWER
        state.current_term = 1

        success = state.append_entries(-1, -1, [LogEntry(1, ("SET", "foo", "bar"))])
        self.assertTrue(success)
        self.assertEqual(state.log, [LogEntry(1, ("SET", "foo", "bar"))])

        success = state.append_entries(0, 1, [LogEntry(1, ("DEL", "foo"))])
        self.assertTrue(success)
        self.assertEqual(
            state.log, [LogEntry(1, ("SET", "foo", "bar")), LogEntry(1, ("DEL", "foo"))]
        )

    def test_append_entries_failure(self):
        state = RaftState()
        state.status = state.FOLLOWER
        state.current_term = 1
        state.log = [LogEntry(1, ("SET", "foo", "bar"))]

        # prev_term mismatch
        success = state.append_entries(0, 2, [LogEntry(1, ("SET", "baz", "buzz"))])
        self.assertFalse(success)
        # log didn't change
        self.assertEqual(state.log, [LogEntry(1, ("SET", "foo", "bar"))])

        # prev_index doesn't exist
        success = state.append_entries(10, 1, [LogEntry(1, ("SET", "baz", "buzz"))])
        self.assertFalse(success)
        # log didn't change
        self.assertEqual(state.log, [LogEntry(1, ("SET", "foo", "bar"))])