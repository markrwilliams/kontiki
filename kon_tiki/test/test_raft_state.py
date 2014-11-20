from twisted.internet import defer, task
from kon_tiki import raft, rpc
from kon_tiki import persist
from twisted.trial import unittest


def applyCommand(*args):
    return defer.succeed(None)


class RaftStateTest(unittest.TestCase):

    def create_state(self):
        persister = persist.SQLitePersist(':memory:')
        persister.connect()
        identity = 'identity'
        peers = set()
        timeoutRange = (.150, .350)
        server = rpc.RaftServer(identity=identity, peers=peers,
                                applyCommand=applyCommand,
                                persister=persister)
        state = raft.State(identity=identity,
                           server=server,
                           peers=peers,
                           applyCommand=applyCommand,
                           electionTimeoutRange=timeoutRange,
                           persister=persister)

        originalClock = raft.StartsElection.clock
        self.patch(raft.StartsElection, 'clock', task.Clock())

        def restoreClock():
            raft.StartsElection.clock = originalClock

        self.addCleanup(restoreClock)
        return state

    def test_candidateIdOK(self):
        """
        CandidateIdOK should check whether or not we voted
        for another candidate.
        """
        state = self.create_state()
        ID = 'some_ID'
        d = state.candidateIdOK(candidateId=ID)
        d.addCallback(self.assertTrue)
        d.addCallback(lambda ignore:
                      state.persister.votedFor())
        d.addCallback(lambda ignore:
                      state.persister.voteFor('otherId'))
        d.addCallback(lambda ignore:
                      state.candidateIdOK(candidateId=ID))
        d.addCallback(self.assertFalse)
        return d

    def test_candidateLogUpToDate(self):
        """
        Is the candidate up to date? The term and the index together
        have to be at least as advanced as that of the current machine.

        """

        state = self.create_state()
        currentTerm = 100
        log = []
        for x in xrange(10):
            log.append(persist.LogEntry(term=currentTerm, command=x))
        d = state.persister.setCurrentTerm(currentTerm)
        d.addCallback(lambda ignore:
                      state.persister.matchAndAppendNewLogEntries(-1,
                                                                  log))

        # Index and term are equal
        d.addCallback(
            lambda ignore:
            state.candidateLogUpToDate(lastLogIndex=len(log) - 1,
                                       lastLogTerm=currentTerm))
        d.addCallback(self.assertTrue)

        # Index is higher
        d.addCallback(
            lambda ignore:
            state.candidateLogUpToDate(lastLogIndex=len(log),
                                       lastLogTerm=currentTerm))
        d.addCallback(self.assertTrue)

        # Term is higher
        d.addCallback(
            lambda ignore:
            state.candidateLogUpToDate(lastLogIndex=len(log) - 1,
                                       lastLogTerm=currentTerm + 1))
        d.addCallback(self.assertTrue)

        # # Index is lower
        d.addCallback(
            lambda ignore:
            state.candidateLogUpToDate(lastLogIndex=len(log) - 2,
                                       lastLogTerm=currentTerm))
        d.addCallback(self.assertFalse)

        # # Term is lower
        d.addCallback(
            lambda ignore:
            state.candidateLogUpToDate(lastLogIndex=len(log),
                                       lastLogTerm=currentTerm - 1))
        d.addCallback(self.assertFalse)
        return d

    def test_requestVote(self):
        """
        Do I give you a vote? It depends.

        1) Is your term less than mine? If so, the answer is no.
        2) Have I voted for you before, and candidate log is up to date?
        Then yes
        """
        state = self.create_state()

        currentTerm = 100
        candidateId = 'ThisCandidate'
        lastLogIndex = 10
        lastLogTerm = currentTerm

        log = []
        for x in xrange(10):
            log.append(persist.LogEntry(term=currentTerm, command=x))
        d = state.persister.setCurrentTerm(currentTerm)
        d.addCallback(lambda ignore:
                      state.persister.matchAndAppendNewLogEntries(-1,
                                                                  log))

        # Test for term less then (case 1)
        d.addCallback(lambda ignore:
                      state.requestVote(term=currentTerm - 1,
                                        candidateId=candidateId,
                                        lastLogIndex=lastLogIndex,
                                        lastLogTerm=lastLogTerm))

        d.addCallback(self.assertEquals, (currentTerm, False))

        # Test for success (never voted before and voted for this
        # candidate before)
        for _ in xrange(2):
            d.addCallback(lambda ignore:
                          state.requestVote(term=currentTerm,
                                            candidateId=candidateId,
                                            lastLogIndex=lastLogIndex,
                                            lastLogTerm=lastLogTerm))
            d.addCallback(self.assertEquals, (currentTerm, True))
            d.addCallback(lambda ignore:
                          self.assertTrue(isinstance(state.server.state,
                                                     raft.Follower)))

        # Test for failure (voted before, but fell behind)
        d.addCallback(lambda ignore:
                      state.requestVote(term=currentTerm,
                                        candidateId=candidateId,
                                        lastLogIndex=lastLogIndex,
                                        lastLogTerm=lastLogTerm - 1))

        d.addCallback(self.assertEquals, (currentTerm, False))
        return d
