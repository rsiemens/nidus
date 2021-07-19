import json
import logging
import os
import shutil
from unittest import TestCase
from unittest.mock import Mock, patch

from nidus import actors
from nidus.kvstore import KVStore
from nidus.log import LogEntry
from nidus.messages import ElectionRequest, HeartbeatRequest, VoteRequest
from nidus.raft import RaftNetwork
from nidus.state import RaftState

logging.disable(logging.CRITICAL)
actors._system = actors.SyncSystem()

CLUSTER_CONFIG = {
    "cluster": {
        "node-0": "n0",
        "node-1": "n1",
        "node-2": "n2",
    },
    "heartbeat_interval": 0.05,
    "storage_dir": "test_nidus_logs",
}


class RaftNodeTestCases(TestCase):
    test_log_dir = "test_nidus_logs"

    def tearDown(self):
        if os.path.exists(CLUSTER_CONFIG["storage_dir"]):
            shutil.rmtree(CLUSTER_CONFIG["storage_dir"])

    @patch("nidus.raft.RaftNode.restart_election_timer")
    def test_has_consensus(self, mock_restart_timer):
        net = RaftNetwork(CLUSTER_CONFIG, KVStore)
        node0 = net.create_node("node-0")

        node0.state.match_index = {n: -1 for n in node0.peers + [node0.node_id]}

        # no one has index 0 in their log
        self.assertFalse(node0.has_consensus(0))
        # node-0 has index 0 in their log, but not a majority
        node0.state.match_index["node-0"] = 0
        self.assertFalse(node0.has_consensus(0))
        # majority has the log at index 0 replicated
        node0.state.match_index["node-1"] = 0
        self.assertTrue(node0.has_consensus(0))
        # everyone has the log at index 0 replicated
        node0.state.match_index["node-2"] = 0
        self.assertTrue(node0.has_consensus(0))

        # no one has index 1 in their log
        self.assertFalse(node0.has_consensus(1))
        # node-0 has index 1 in their log, but not a majority
        node0.state.match_index["node-0"] = 1
        self.assertFalse(node0.has_consensus(1))
        # majority has the log at index 1 replicated
        node0.state.match_index["node-1"] = 1
        self.assertTrue(node0.has_consensus(1))
        # everyone has the log at index 1 replicated
        node0.state.match_index["node-2"] = 1
        self.assertTrue(node0.has_consensus(1))

    def test_apply_any_commits(self):
        pass

    @patch("nidus.raft.RaftNode.restart_election_timer")
    def test_promote(self, mock_restart_timer):
        net = RaftNetwork(CLUSTER_CONFIG, KVStore)
        node0 = net.create_node("node-0")
        node0.election_timer = Mock()
        node0.promote()

        self.assertEqual(node0.state.status, RaftState.LEADER)
        node0.election_timer.cancel.assert_called_once()

        # make sure there is a HeartBeatRequest msg is in our mailbox
        # it's what will trigger sending a heartbeat to the other nodes
        heartbeat = net.actor_system._inboxes["n0"].get_nowait()
        self.assertIsInstance(heartbeat, HeartbeatRequest)

    @patch("nidus.raft.RaftNode.restart_election_timer")
    def test_demote(self, mock_restart_timer):
        net = RaftNetwork(CLUSTER_CONFIG, KVStore)
        node0 = net.create_node("node-0")
        node0.election_timer = Mock()

        node0.state.status = RaftState.LEADER
        node0.heartbeat_timer = Mock()
        mock_restart_timer.reset_mock()
        node0.demote()

        self.assertEqual(node0.state.status, RaftState.FOLLOWER)
        mock_restart_timer.assert_called_once()
        node0.heartbeat_timer.cancel.assert_called_once()

    @patch("nidus.raft.RaftNode.restart_election_timer")
    def test_handle_election_request(self, mock_election_timer):
        net = RaftNetwork(CLUSTER_CONFIG, KVStore)
        node0 = net.create_node("node-0")
        net.create_node("node-1")
        net.create_node("node-2")
        before_term = node0.state.current_term

        mock_election_timer.reset_mock()
        node0.handle_election_request(ElectionRequest())
        node1_msg = net.actor_system._inboxes["n1"].get_nowait()
        node2_msg = net.actor_system._inboxes["n2"].get_nowait()

        self.assertEqual(node0.state.status, RaftState.CANDIDATE)
        self.assertEqual(node0.state.current_term, before_term + 1)
        mock_election_timer.assert_called_once()
        self.assertIsInstance(node1_msg, VoteRequest)
        self.assertIsInstance(node2_msg, VoteRequest)
