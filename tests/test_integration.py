import json
import logging
from unittest import TestCase
from unittest.mock import Mock, patch

from nidus import actors
from nidus.kvstore import KVStore
from nidus.log import LogEntry
from nidus.messages import ClientRequest, ElectionRequest, HeartbeatRequest, VoteRequest
from nidus.raft import RaftNetwork
from nidus.state import RaftState

logging.disable(logging.CRITICAL)
actors._system = actors.SyncSystem()


class IntegrationTestsCases(TestCase):
    def assert_log(self, actual, expected):
        self.assertEqual(len(actual), len(expected))

        for i, log in enumerate(actual):
            self.assertEqual(log.term, expected[i])
            self.assertEqual(log.item, expected[i])

    def create_log(self, items):
        return [LogEntry(term=i, item=i) for i in items]

    @patch("nidus.raft.Timer")
    def test_figure_6(self, mock_timer_cls):
        config = {
            "cluster": {
                "node-0": "n0",
                "node-1": "n1",
                "node-2": "n2",
                "node-3": "n3",
                "node-4": "n4",
            },
            "heartbeat_interval": 0.05,
        }
        net = RaftNetwork(config, Mock())
        n0 = net.create_node("node-0")
        n1 = net.create_node("node-1")
        n2 = net.create_node("node-2")
        n3 = net.create_node("node-3")
        n4 = net.create_node("node-4")

        n0.state.current_term = 3
        n0.state.commit_index = 6
        n0.state.last_applied = 6
        n0.state.log = self.create_log([1, 1, 1, 2, 3, 3, 3, 3])
        n0.state.become_leader(n0.peers + [n0.node_id])

        n1.state.log = self.create_log([1, 1, 1, 2, 3])
        n2.state.log = self.create_log([1, 1, 1, 2, 3, 3, 3, 3])
        n3.state.log = self.create_log([1, 1])
        n4.state.log = self.create_log([1, 1, 1, 2, 3, 3, 3])

        # The hearbeat should cascade into a series of messages
        # that get everyone up to the same spot as node-0 and
        # provides consensus on index 7
        net.send("node-0", HeartbeatRequest())
        net.actor_system.flush()
        for node in [n1, n2, n3, n4]:
            with self.subTest(node=node):
                self.assert_log(node.state.log, [1, 1, 1, 2, 3, 3, 3, 3])
                self.assertEqual(node.state.current_term, 3)

        # The next heartbeat propagates the latest commit (index 7) to all followers
        net.send("node-0", HeartbeatRequest())
        net.actor_system.flush()
        for node in [n1, n2, n3, n4]:
            with self.subTest(node=node):
                self.assert_log(node.state.log, [1, 1, 1, 2, 3, 3, 3, 3])
                self.assertEqual(node.state.current_term, 3)
                self.assertEqual(node.state.commit_index, 7)
                self.assertEqual(node.state.last_applied, 7)

    @patch("nidus.raft.Timer")
    def test_figure_7(self, mock_timer_cls):
        config = {
            "cluster": {
                "leader": "leader",
                "a": "a",
                "b": "b",
                "c": "c",
                "d": "d",
                "e": "e",
                "f": "f",
            },
            "heartbeat_interval": 0.05,
        }

        net = RaftNetwork(config, Mock())
        leader = net.create_node("leader")
        a = net.create_node("a")
        b = net.create_node("b")
        c = net.create_node("c")
        d = net.create_node("d")
        e = net.create_node("e")
        f = net.create_node("f")

        # Figure 7: When the leader at the top comes to power, it ispossible that
        # any of scenarios (a–f) could occur in followerlogs. Each box represents
        # one log entry; the number in thebox is its term. A follower may be
        # missing entries (a–b), mayhave extra uncommitted entries (c–d), or
        # both (e–f). For ex-ample, scenario (f) could occur if that server was
        # the leaderfor term 2, added several entries to its log, then crashed
        # beforecommitting any of them; it restarted quickly, became leader for
        # term 3, and added a few more entries to its log; before anyof the
        # entries in either term 2 or term 3 were committed, theserver crashed
        # again and remained down for several terms.
        leader.state.current_term = 8
        leader.state.commit_index = 4
        leader.state.log = self.create_log([1, 1, 1, 4, 4, 5, 5, 6, 6, 6])
        leader.state.become_leader(leader.peers + [leader.node_id])

        a.state.log = self.create_log([1, 1, 1, 4, 4, 5, 5, 6, 6])
        b.state.log = self.create_log([1, 1, 1, 4])
        c.state.log = self.create_log([1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 6])
        d.state.log = self.create_log([1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 7, 7])
        e.state.log = self.create_log([1, 1, 1, 4, 4, 4, 4])
        f.state.log = self.create_log([1, 1, 1, 2, 2, 2, 3, 3, 3, 3, 3])

        net.send("leader", HeartbeatRequest())
        net.actor_system.flush()
        for node in [leader, a, b, c, d, e, f]:
            with self.subTest(node=node):
                self.assert_log(node.state.log, [1, 1, 1, 4, 4, 5, 5, 6, 6, 6])
                self.assertEqual(node.state.current_term, 8)
                # Only log entries from the leader’s current term are committed by counting replicas;
                self.assertEqual(node.state.commit_index, 4)

        # Once an entryf rom the current term has been committed in this way, then
        # all prior entries are committed indirectly becauseof the Log Matching Property.
        net.send("leader", ClientRequest("foo", 8))
        # replicate the value
        net.send("leader", HeartbeatRequest())
        # get consensus and commit the value
        net.actor_system.flush()
        net.send("leader", HeartbeatRequest())
        net.actor_system.flush()
        for node in [leader, a, b, c, d, e, f]:
            with self.subTest(node=node):
                self.assert_log(node.state.log, [1, 1, 1, 4, 4, 5, 5, 6, 6, 6, 8])
                self.assertEqual(node.state.current_term, 8)
                # Only log entries from the leader’s current term are committed by counting replicas;
                self.assertEqual(node.state.commit_index, 10)
