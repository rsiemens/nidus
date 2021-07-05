from nidus.log import append_entries


class RaftState:
    # status states
    LEADER = "LEADER"
    CANDIDATE = "CANDIDATE"
    FOLLOWER = "FOLLOWER"

    def __init__(self):
        self.status = self.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.votes = set()
        self.log = []
        self.commit_index = -1
        self.last_applied = -1
        self.match_index = {}
        self.next_index = {}

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
