from textwrap import fill
from twisted.trial import unittest
from twisted.internet import defer, task
from kontiki import raft, rpc
from kontiki import persist, rpc_objects
from kontiki.test.common import dropResult


def applyCommand(*args):
    return defer.succeed(None)


class RaftStateTest(unittest.TestCase):

    def setUp(self):
        self.persister = persist.SQLitePersist(':memory:')
        self.persister.connect()
        identity = 'identity'
        peers = set()
        timeoutRange = (.150, .350)
        self.server = rpc.RaftServer(identity=identity, peers=peers,
                                     applyCommand=applyCommand,
                                     persister=self.persister)
        self.state = raft.State(identity=identity,
                                server=self.server,
                                peers=peers,
                                applyCommand=applyCommand,
                                electionTimeoutRange=timeoutRange,
                                persister=self.persister)

        originalClock = raft.StartsElection.clock
        self.patch(raft.StartsElection, 'clock', task.Clock())

        def restoreClock():
            raft.StartsElection.clock = originalClock

        self.addCleanup(restoreClock)

    def test_candidateIdOK(self):
        """
        CandidateIdOK should check whether or not we voted
        for another candidate.
        """
        ID = 'someID'
        results = self.state.candidateIdOK(candidateId=ID)

        candidateIDOKFailureMessage = ("new state's candidateIDOK should"
                                       " OK any ID")
        results.addCallback(self.assertTrue, msg=candidateIDOKFailureMessage)

        results.addCallback(dropResult(self.state.persister.votedFor))
        results.addCallback(
            dropResult(self.state.persister.voteFor, 'otherId'))
        results.addCallback(
            dropResult(self.state.candidateIdOK, candidateId=ID))

        candidateIDNotOKFailureMessage = ('candidateIDOK should return'
                                          ' False after state has voted')
        results.addCallback(self.assertFalse,
                            msg=candidateIDNotOKFailureMessage)
        return results

    def test_candidateLogUpToDate(self):
        """
        Is the candidate up to date? The term and the index together
        have to be at least as advanced as that of the current machine.

        """

        currentTerm = 100
        log = []
        for x in xrange(10):
            log.append(rpc_objects.LogEntry(term=currentTerm, command=x))
        results = self.state.persister.setCurrentTerm(currentTerm)
        results.addCallback(
            dropResult(self.state.persister.matchAndAppendNewLogEntries,
                       -1, log))

        # Index and term are equal
        results.addCallback(
            dropResult(self.state.candidateLogUpToDate,
                       lastLogIndex=len(log) - 1,
                       lastLogTerm=currentTerm))

        indexAndTermOKFailureMessage = fill(
            '''candidateLogUpToDate should return True when lastLogIndex and
            lastLogTerm are equal to the state's last log index and the
            term of the last log entry.''')
        results.addCallback(self.assertTrue, msg=indexAndTermOKFailureMessage)

        results.addCallback(
            dropResult(self.state.candidateLogUpToDate,
                       lastLogIndex=len(log),
                       lastLogTerm=currentTerm))

        indexIsHigherFailureMessage = fill(
            '''candidateLogUpToDate should return True when lastLogIndex is
            greater than state's last log index and the lastLogTerm is
            equal to the term of the last log entry.''')
        results.addCallback(self.assertTrue, msg=indexIsHigherFailureMessage)

        results.addCallback(
            dropResult(self.state.candidateLogUpToDate,
                       lastLogIndex=len(log) - 1,
                       lastLogTerm=currentTerm + 1))

        termIsHigherFailureMessage = fill(
            '''candidateLogUpToDate should return True when lastLogIndex is
            equal to the state's last log index and the lastLogTerm is
            greater than the term of the last log entry.''')
        results.addCallback(self.assertTrue, msg=termIsHigherFailureMessage)

        results.addCallback(
            dropResult(self.state.candidateLogUpToDate,
                       lastLogIndex=len(log) - 2, lastLogTerm=currentTerm))

        indexIsLowerFailureMessage = fill(
            '''candidateLogUpToDate should return False when lastLogIndex is
            less than the state's last log index''')
        results.addCallback(self.assertFalse, msg=indexIsLowerFailureMessage)

        # # Term is lower
        results.addCallback(
            dropResult(self.state.candidateLogUpToDate,
                       lastLogIndex=len(log), lastLogTerm=currentTerm - 1))

        termIsLowerFailureMessage = fill(
            '''candidateLogUpToDate should return False when lastLogTerm is
            less than the term of the last log entry.''')
        results.addCallback(self.assertFalse, msg=termIsLowerFailureMessage)
        return results

    def test_requestVote(self):
        """
        Do I give you a vote? It depends.

        1) Is your term less than mine? If so, the answer is no.
        2) Have I voted for you before, and candidate log is up to date?
        Then yes
        """
        currentTerm = 100
        candidateId = 'ThisCandidate'
        lastLogIndex = 10
        lastLogTerm = currentTerm

        log = []
        for x in xrange(10):
            log.append(rpc_objects.LogEntry(term=currentTerm, command=x))
        results = self.state.persister.setCurrentTerm(currentTerm)
        results.addCallback(
            dropResult(self.state.persister.matchAndAppendNewLogEntries,
                       -1, log))

        # Test for term less then (case 1)
        results.addCallback(
            dropResult(self.state.requestVote,
                       term=currentTerm - 1,
                       candidateId=candidateId,
                       lastLogIndex=lastLogIndex,
                       lastLogTerm=lastLogTerm))

        results.addCallback(self.assertEquals, (currentTerm, False))

        currentTerm += 1

        # Test for success
        results.addCallback(
            dropResult(self.state.requestVote,
                       term=currentTerm,
                       candidateId=candidateId,
                       lastLogIndex=lastLogIndex,
                       lastLogTerm=lastLogTerm))
        results.addCallback(self.assertEquals, (currentTerm, True))
        results.addCallback(dropResult(self.assertTrue,
                                       isinstance(self.server.state,
                                                  raft.Follower)))

        # Test for failure (voted before, but fell behind)
        results.addCallback(
            dropResult(self.state.requestVote,
                       term=currentTerm,
                       candidateId=candidateId,
                       lastLogIndex=lastLogIndex,
                       lastLogTerm=lastLogTerm - 1))

        results.addCallback(self.assertEquals, (currentTerm, False))

        # Test for failure (already became follower for this term)
        results.addCallback(
            dropResult(self.state.requestVote,
                       term=currentTerm,
                       candidateId=candidateId,
                       lastLogIndex=lastLogIndex,
                       lastLogTerm=lastLogTerm))

        results.addCallback(self.assertEquals, (currentTerm, False))

        return results
