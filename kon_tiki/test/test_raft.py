from kon_tiki import raft
from kon_tiki import persist
from twisted.trial import unittest


class RaftServerTest(unittest.TestCase):

    def create_server(self):
        persister = persist.ListPersist()
        identity = 'identity'
        peers = set()
        applyCommand = None
        timeoutRange = (.150, .350)
        cycle = raft.ServerCycle(identity=identity, peers=peers,
                                 applyCommand=applyCommand,
                                 persister=persister)
        server = raft.Server(identity=identity, cycle=cycle,
                             peers=peers, applyCommand=applyCommand,
                             electionTimeoutRange=timeoutRange,
                             persister=persister)
        return server

    def test_candidateIdOK(self):
        """
        CandidateIdOK should check whether or not we voted
        for another candidate.
        """
        server = self.create_server()
        ID = 'some_ID'
        self.assertTrue(server.candidateIdOK(candidateId=ID))
        server.persister.votedFor = 'other_ID'
        self.assertFalse(server.candidateIdOK(candidateId=ID))
        server.persister.votedFor = 'some_ID'
        self.assertTrue(server.candidateIdOK(candidateId=ID))
