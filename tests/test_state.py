import os
import shutil
from unittest import TestCase
from unittest.mock import Mock

from nidus.log import LogEntry
from nidus.state import RaftState


class RaftStateTestCases(TestCase):
    test_log_dir = "test_nidus_logs"

    def setUp(self):
        os.makedirs(self.test_log_dir)

    def tearDown(self):
        if os.path.exists(self.test_log_dir):
            shutil.rmtree(self.test_log_dir)

    def test_append_entires_success(self):
        state = RaftState(self.test_log_dir, "node-0")
        state.status = state.FOLLOWER
        state.current_term = 1

        success = state.append_entries(-1, -1, [LogEntry(1, ["SET", "foo", "bar"])])
        self.assertTrue(success)
        self.assertEqual(state.log.as_list(), [LogEntry(1, ["SET", "foo", "bar"])])

        success = state.append_entries(0, 1, [LogEntry(1, ["DEL", "foo"])])
        self.assertTrue(success)
        self.assertEqual(
            state.log.as_list(),
            [LogEntry(1, ["SET", "foo", "bar"]), LogEntry(1, ["DEL", "foo"])],
        )

    def test_append_entries_failure(self):
        state = RaftState(self.test_log_dir, "node-0")
        state.status = state.FOLLOWER
        state.current_term = 1
        state.log.append(LogEntry(1, ["SET", "foo", "bar"]))

        # prev_term mismatch
        success = state.append_entries(0, 2, [LogEntry(1, ["SET", "baz", "buzz"])])
        self.assertFalse(success)
        # log didn't change
        self.assertEqual(state.log.as_list(), [LogEntry(1, ["SET", "foo", "bar"])])

        # prev_index doesn't exist
        success = state.append_entries(10, 1, [LogEntry(1, ["SET", "baz", "buzz"])])
        self.assertFalse(success)
        # log didn't change
        self.assertEqual(state.log.as_list(), [LogEntry(1, ["SET", "foo", "bar"])])
