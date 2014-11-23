from kon_tiki import raft
from kon_tiki import persist
from twisted.trial import unittest
from twisted.internet import task


class RaftCandidateTest:

    def create_candidate(self):
        persister = persist.ListPersist()
        identity = 'identity'
        peers = set(['peer1', 'peer2', 'peer3', 'peer4', 'peer5'])
        applyCommand = None
        timeoutRange = (.150, .350)
        cycle = raft.ServerCycle(identity=identity, peers=peers,
                                 applyCommand=applyCommand,
                                 persister=persister)

        def fake_conduct(*args):
            pass

        candidate = raft.Candidate(identity=identity, cycle=cycle,
                                   peers=peers, applyCommand=applyCommand,
                                   electionTimeoutRange=timeoutRange,
                                   persister=persister)
        return candidate

    def test_prepareForElection(self):
        """
        Just make sure things are set up

        """

        candidate = self.create_candidate()
        candidate.prepareForElection()
        self.assertEqual(candidate.votes, 0)
        self.assertEqual(candidate.persister.votedFor, 'identity')
        self.assertEqual(candidate.persister.currentTerm, 1)
