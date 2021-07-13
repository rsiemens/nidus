import logging
import random
from threading import Timer

from nidus.actors import Actor, get_system
from nidus.log import LogEntry
from nidus.messages import (
    AppendEntriesRequest,
    AppendEntriesResponse,
    ClientResponse,
    ElectionRequest,
    HeartbeatRequest,
    VoteRequest,
    VoteResponse,
)
from nidus.state import RaftState

logger = logging.getLogger("node_logger")


class RaftNetwork:
    """
    The raft network.
    """

    def __init__(self, config, state_machine_cls):
        self.actor_system = get_system()
        self.config = config
        self.raft_ctx = None
        self.state_machine_cls = state_machine_cls

    def create_node(self, node_id):
        addr = self.config["cluster"][node_id]
        peers = [n for n in self.config["cluster"] if n != node_id]

        self.actor_system.create(
            addr, RaftNode, node_id, peers, self, self.state_machine_cls()
        )
        logger.info(f"server running on {addr}", extra={"node_id": node_id})
        # this is just nice while debugging
        return self.actor_system._actors[addr]

    def send(self, node_id, msg):
        # node_id is key to a host,port raft node
        if node_id in self.config["cluster"]:
            self.actor_system.send(self.config["cluster"][node_id], msg)
        else:  # it's a host,port addr (probably a client)
            self.actor_system.send(node_id, msg)


class RaftNode(Actor):
    """
    All the message handling and state changing logic is here
    """

    def __init__(self, node_id, peers, network, state_machine):
        self.node_id = node_id
        self.peers = peers
        self.network = network
        self.state = RaftState()
        self.state_machine = state_machine
        self.heartbeat_interval = network.config["heartbeat_interval"]
        self.heartbeat_timer = None
        self.election_timer = None
        # dict of log_index: addr where log_index is the index a client is
        # waiting to be commited
        self.client_callbacks = {}
        self.leader_id = None
        self.restart_election_timer()

    def handle_client_request(self, req):
        """
        Handle ClientRequest messages to run a command. Append the command to
        the log. If this is not the leader deny the request and redirect.
        Replication will happen for the entry on the next HeartbeatRequest.
        """
        if self.state.status != RaftState.LEADER:
            client_res = ClientResponse(
                f"NotLeader: reconnect to {self.network.config['cluster'].get(self.leader_id, '?')}"
            )
            self.network.send(tuple(req.sender), client_res)
            return

        prev_index = len(self.state.log) - 1
        prev_term = self.state.log[prev_index].term if prev_index >= 0 else -1
        entries = [LogEntry(self.state.current_term, req.command)]

        # append to our own log
        success = self.state.append_entries(prev_index, prev_term, entries)
        if not success:
            raise Exception("This shouldn't happen!")
        self.log(f"append_entries success={success} entries={entries}")

        # update leaders match/next index
        match_index = len(self.state.log) - 1
        self.state.match_index[self.node_id] = match_index
        self.state.next_index[self.node_id] = match_index + 1

        # add client addr to callbacks so we can notify it of the result once
        # it's been commited
        self.client_callbacks[match_index] = tuple(req.sender)

    def handle_append_entries_request(self, req):
        self.restart_election_timer()

        # if rpc request or response contains term t > currentterm:
        # set currentterm = t, convert to follower (§5.1)
        if self.state.status != RaftState.FOLLOWER:
            self.demote()
        if req.term > self.state.current_term:
            self.state.current_term = req.term

        # so follower can redirect clients
        if self.leader_id != req.sender:
            self.leader_id = req.sender

        # Reply false if term < currentterm (§5.1)
        if req.term < self.state.current_term:
            res = AppendEntriesResponse(
                self.node_id, self.state.current_term, False, len(self.state.log) - 1
            )
            self.network.send(req.sender, res)
            return

        entries = [LogEntry(e[0], e[1]) for e in req.entries]
        # Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
        # If an existing entry conflicts with a new one (same index but different terms), delete the existing entry and all that follow it (§5.3)
        # Append any new entries not already in the log attempt to apply the entry to the log
        success = self.state.append_entries(req.prev_index, req.prev_term, entries)
        if entries:
            self.log(f"append_entries success={success} entries={entries}")

        if success:
            match_index = len(self.state.log) - 1
            # If leaderCommit > commitIndex, set commitIndex =min(leaderCommit, index of last new entry)
            if req.commit_index > self.state.commit_index:
                self.state.commit_index = min(req.commit_index, match_index)
                self.log(f"commit_index updated to {self.state.commit_index}")
        else:
            match_index = 0

        res = AppendEntriesResponse(
            self.node_id, self.state.current_term, success, match_index
        )
        self.network.send(req.sender, res)
        self.apply_any_commits()

    def handle_append_entries_response(self, res):
        # If RPC request or response contains term T > currentTerm:
        # set currentTerm = T, convert to follower (§5.1)
        if self.state.current_term < res.term:
            self.demote()

        if self.state.status != RaftState.LEADER:
            return

        sender = res.sender
        # If successful: update nextIndex and matchIndex for follower (§5.3)
        if res.success:
            self.state.match_index[sender] = max(
                res.match_index, self.state.match_index[sender]
            )
            self.state.next_index[sender] = self.state.match_index[sender] + 1
        else:
            # move back the index and try again
            self.state.next_index[sender] = max(0, self.state.next_index[sender] - 1)

        # If last log index ≥ nextIndex for a follower:
        # send AppendEntries RPC with log entries starting at nextIndex
        # if len(self.state.log) - 1 >= self.state.match_index[sender]:
        if self.state.match_index[sender] != len(self.state.log) - 1:
            append_entries_msg = AppendEntriesRequest.from_raft_state(
                self.node_id, sender, self.state
            )
            self.network.send(sender, append_entries_msg)

        # The leader can't commit until there is consensus on a item in the log
        # (majority have replicated it) and the leader has commited an entry
        # from it's current term. See Figure 8 in the raft paper for more
        # details on why this is.
        consensus_check_idx = self.state.match_index[sender]
        if (
            self.has_consensus(consensus_check_idx)
            and consensus_check_idx > -1
            and self.state.log[consensus_check_idx].term == self.state.current_term
            and self.state.commit_index < consensus_check_idx
        ):
            self.state.commit_index = self.state.match_index[sender]
            self.log(
                f"consensus reached on {self.state.commit_index}: {self.state.log[self.state.commit_index]}"
            )

        self.apply_any_commits()

    def handle_vote_request(self, req):
        self.restart_election_timer()

        if req.term < self.state.current_term:
            vote_msg = VoteResponse(self.node_id, self.state.current_term, False)
            self.log(
                f"vote request from {req.candidate} granted=False (candidate term lower than self)"
            )
            self.network.send(req.candidate, vote_msg)
            return

        if (
            req.term > self.state.current_term
            and self.state.status != RaftState.FOLLOWER
        ):
            self.state.current_term = req.term
            self.demote()

        last_log_index = len(self.state.log) - 1
        last_log_term = (
            self.state.log[last_log_index].term if last_log_index >= 0 else -1
        )
        # votedFor is null or candidateId, and candidate’s log is atleast as up-to-date
        # as receiver’s log, grant vote (§5.2, §5.4)
        if (
            self.state.voted_for is None
            or self.state.voted_for == req.candidate
            and (
                req.last_log_term > last_log_term
                or (
                    req.last_log_index >= last_log_index
                    and req.last_log_term == last_log_term
                )
            )
        ):
            vote_msg = VoteResponse(self.node_id, self.state.current_term, True)
            self.state.voted_for = req.candidate
            self.log(f"vote request from {req.candidate} granted=True")
            self.network.send(req.candidate, vote_msg)
            return

        # is this ok to default to?
        self.log(
            f"vote request from {req.candidate} granted=False (vote already granted or log not as up-to-date)"
        )
        vote_msg = VoteResponse(self.node_id, self.state.current_term, False)
        self.network.send(req.candidate, vote_msg)

    def handle_vote_response(self, res):
        if res.term > self.state.current_term:
            self.demote()

        if self.state.status != RaftState.CANDIDATE:
            # either already the leader or have been demoted
            return

        if res.vote_granted:
            self.state.votes.add(res.sender)

        if len(self.state.votes) > (len(self.peers) + 1) // 2:
            self.log(
                f"majority of votes granted {self.state.votes}, transitoning to leader"
            )
            self.promote()

    def handle_heartbeat_request(self, req):
        for peer in self.peers:
            append_entries_msg = AppendEntriesRequest.from_raft_state(
                self.node_id, peer, self.state
            )

            if req.empty:
                append_entries_msg.entries = []
            self.network.send(peer, append_entries_msg)

        target = lambda addr: self.network.send(addr, HeartbeatRequest())
        self.heartbeat_timer = Timer(
            self.heartbeat_interval, target, args=[self.node_id]
        )
        self.heartbeat_timer.start()

    def handle_election_request(self, req):
        self.log(f"haven't heard from leader or election failed; beginning election")
        self.state.become_candidate(self.node_id)

        prev_index = len(self.state.log) - 1
        if prev_index >= 0:
            prev_term = self.state.log[prev_index].term
        else:
            prev_term = -1

        self.restart_election_timer()
        for peer in self.peers:
            vote_msg = VoteRequest(
                self.state.current_term, self.node_id, prev_index, prev_term
            )
            self.network.send(peer, vote_msg)

    def has_consensus(self, indx):
        """
        Assuming a 5 node cluster with a matched index like:
            [1, 3, 2, 3, 3]
        We sort it to get:
            [1, 2, 3, 3, 3]
        Next we get the middle value (3) which gives us an index guarenteed to
        be on a majority of the servers logs.
        """
        sorted_matched_idx = sorted(self.state.match_index.values())
        commited_idx = sorted_matched_idx[(len(sorted_matched_idx) - 1) // 2]
        return commited_idx >= indx

    def apply_any_commits(self):
        # If commitIndex > lastApplied: increment lastApplied,
        # applylog[lastApplied] to state machine (§5.3)
        while self.state.commit_index > self.state.last_applied:
            log_entry = self.state.log[self.state.last_applied + 1]
            try:
                result = self.state_machine.apply(log_entry.item)
            except Exception as ex:
                result = repr(ex)
            self.state.last_applied += 1
            self.log(f"applied {log_entry.item} to state machine: {result}")

            # notify the client of the result
            if (
                self.state.status == RaftState.LEADER
                and self.client_callbacks.get(self.state.last_applied) is not None
            ):
                client_addr = self.client_callbacks.pop(self.state.last_applied)
                self.network.send(client_addr, ClientResponse(result))

    def promote(self):
        self.state.become_leader(self.peers + [self.node_id])
        heartbeat_request = HeartbeatRequest(empty=True)
        self.election_timer.cancel()
        # should we skip the send to ourself and just invoke?
        self.network.send(self.node_id, heartbeat_request)

    def demote(self):
        self.log("reverting to follower")
        if self.heartbeat_timer is not None:
            self.heartbeat_timer.cancel()
        self.state.become_follower()
        self.restart_election_timer()

    def restart_election_timer(self):
        if self.election_timer is not None:
            self.election_timer.cancel()

        target = lambda addr: self.network.send(addr, ElectionRequest())
        base_interval = self.heartbeat_interval * 30
        interval = base_interval + random.random() * base_interval

        self.election_timer = Timer(interval, target, args=[self.node_id])
        self.election_timer.start()

    def handle_destroy(self):
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        if self.election_timer:
            self.election_timer.cancel()

    def log(self, msg):
        logger.info(msg, extra={"node_id": self.node_id})

    def __repr__(self):
        return f"""\
<RaftNode {self.node_id} [{self.state.status}]
    current_term: {self.state.current_term}
    voted_for: {self.state.voted_for}
    votes: {self.state.votes}
    commit_index: {self.state.commit_index}
    last_applies: {self.state.last_applied}
    match_index: {self.state.match_index}
    next_index: {self.state.next_index}
    log:{self.state.log}
>"""
