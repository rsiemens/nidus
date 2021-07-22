import os
import struct

from nidus.log import Log, append_entries


class RaftState:
    # status states
    LEADER = "LEADER"
    CANDIDATE = "CANDIDATE"
    FOLLOWER = "FOLLOWER"

    def __init__(self, storage_dir, node_id):
        self._storage_dir = storage_dir
        self._node_id = node_id

        self.status = self.FOLLOWER
        self._current_term = 0
        self._voted_for = None
        self.votes = set()
        self.log = Log(os.path.join(storage_dir, f"{node_id}.log"))
        self.commit_index = -1
        self.last_applied = -1
        self.match_index = {}
        self.next_index = {}

        term_path = os.path.join(self._storage_dir, f"{self._node_id}.term")
        if os.path.exists(term_path):
            with open(term_path, "rb+") as f:
                f.seek(0)
                self._current_term = struct.unpack(">L", f.read(4))[0]
        else:
            with open(term_path, "wb+"):
                pass

        vote_path = os.path.join(self._storage_dir, f"{self._node_id}.vote")
        if os.path.exists(vote_path):
            with open(vote_path, "rb+") as f:
                f.seek(0)
                data = f.read()
                if data:
                    self._voted_for = data.decode("utf8")
                else:
                    self._voted_for = None
        else:
            with open(vote_path, "wb+"):
                pass

    @property
    def current_term(self):
        return self._current_term

    @current_term.setter
    def current_term(self, term):
        path = os.path.join(self._storage_dir, f"{self._node_id}.term")
        with open(path, "rb+") as f:
            f.seek(0)
            f.write(struct.pack(">L", term))
        self._current_term = term

    @property
    def voted_for(self):
        return self._voted_for

    @voted_for.setter
    def voted_for(self, candidate):
        path = os.path.join(self._storage_dir, f"{self._node_id}.vote")
        with open(path, "rb+") as f:
            f.seek(0)
            f.truncate()
            if candidate is not None:
                f.write(candidate.encode("utf8"))
        self._voted_for = candidate

    def become_leader(self, nodes):
        self.status = self.LEADER
        # next log entry to send to peer
        self.next_index = {n: len(self.log) for n in nodes}
        # highest index known to be replicated on a server
        self.match_index = {n: -1 for n in nodes}

    def become_follower(self):
        self.status = self.FOLLOWER
        self.voted_for = None

    def become_candidate(self, node_id):
        self.status = self.CANDIDATE
        self.current_term += 1
        self.voted_for = node_id
        self.votes = set([node_id])

    def append_entries(self, prev_index, prev_term, entries):
        return append_entries(self.log, prev_index, prev_term, entries)
