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


    def test_candidateLogUpToDate(self):
        """
        Is the candidate up to date? The term and the index together
        have to be at least as advanced as that of the current machine.

        """

        server = self.create_server()
        currentTerm = 100
        log = []
        for x in xrange(10):
            log.append(persist.LogEntry(term=currentTerm, command=x))

        server.persister.currentTerm = currentTerm
        server.persister.log = log
        res = server.candidateLogUpToDate(lastLogIndex=len(log),
                                          lastLogTerm=currentTerm)
        self.assertTrue(res)

        # Index is higher
        res = server.candidateLogUpToDate(lastLogIndex=len(log)+1,
                                          lastLogTerm=currentTerm)
        self.assertTrue(res)

        # Term is higher
        res = server.candidateLogUpToDate(lastLogIndex=len(log),
                                          lastLogTerm=currentTerm+1)
        self.assertTrue(res)

        # Index is lower
        res = server.candidateLogUpToDate(lastLogIndex=len(log)-2,
                                          lastLogTerm=currentTerm)
        self.assertFalse(res)

        # Term is lower
        res = server.candidateLogUpToDate(lastLogIndex=len(log),
                                          lastLogTerm=currentTerm-1)
        self.assertFalse(res)


    def test_requestVote(self):
        """
        Do I give you a vote? It depends.

        1) Is your term less than mine? If so, the answer is no.
        2) Have I voted for you before, and candidate log is up to date?
        Then yes
        """
        server = self.create_server()

        currentTerm = 100
        candidateId = 'ThisCandidate'
        lastLogIndex = 10
        lastLogTerm = currentTerm

        log = []
        for x in xrange(10):
            log.append(persist.LogEntry(term=currentTerm, command=x))
        server.persister.log = log

        # Test for term less then (case 1)
        server.persister.currentTerm = currentTerm + 1
        term, vg = server.remote_requestVote(term=currentTerm,
                                             candidateId=candidateId,
                                             lastLogIndex=lastLogIndex,
                                             lastLogTerm=lastLogTerm)
        self.assertEquals(term, 101)
        self.assertFalse(vg)

        # Test for success (never voted before)
        server.persister.currentTerm = currentTerm
        term, vg = server.remote_requestVote(term=currentTerm,
                                             candidateId=candidateId,
                                             lastLogIndex=lastLogIndex,
                                             lastLogTerm=lastLogTerm)
        self.assertTrue(vg)
        self.assertEquals(server.persister.votedFor, candidateId)
        self.assertEquals(type(server.cycle.state), raft.Follower)

        # Test for success (voted before)
        server.persister.votedFor = candidateId
        term, vg = server.remote_requestVote(term=currentTerm,
                                             candidateId=candidateId,
                                             lastLogIndex=lastLogIndex,
                                             lastLogTerm=lastLogTerm)
        self.assertTrue(vg)

        # Test for failure (voted before, but fell behind)
        term, vg = server.remote_requestVote(term=currentTerm,
                                             candidateId=candidateId,
                                             lastLogIndex=lastLogIndex,
                                             lastLogTerm=lastLogTerm-1)
        self.assertFalse(vg)
        
