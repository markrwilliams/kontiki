'''An implementation of the Raft consensus algorithm'''
from kon_tiki.fundamentals import median
import operator
from twisted.internet import reactor
from twisted.spread import pb
from collections import namedtuple
import random


LogEntry = namedtuple('LogEntry', 'term command')


class PersistenceError(Exception):
    '''Raised when an error occurs in a Persist class'''


class MatchAfterTooHigh(PersistenceError):
    '''Raised when Persist.matchLogToEntries is given a prevIndex that's
    greater than the current size of the log.  Perhaps you didn't call
    Persist.indexMatchesTerm first?'''


class ListPersist(object):
    '''A Persist implementation that stores its state in-memory.  For
    testing only!
    '''

    def __init__(self, currentTerm=0, votedFor=None, log=None):
        # A real persister would restore the previous values, and
        # these would be properties that, when set, sync'd the values
        # to disk
        self.currentTerm = currentTerm
        self.votedFor = votedFor
        if not log:
            log = []
        self.log = log

    @property
    def lastIndex(self):
        return len(self.log) - 1

    def _compareIndexToTerm(self, index, term, op):
        if index == -1 and not self.log:
            return True
        return -1 < index < len(self.log) and op(self.log[index].term, term)

    def logSlice(self, start, end):
        return self.log[start:end]

    def indexMatchesTerm(self, index, term):
        return self._compareIndexToTerm(index, term, operator.eq)

    def lastIndexNewerThanTerm(self, term):
        return self._compareIndexToTerm(self.lastIndex, term, operator.le)

    def matchLogToEntries(self, matchAfter, entries):
        '''Delete existing log entries that conflict with `entries` and return
        only new entries.
        '''
        lastIndex = self.lastIndex
        if matchAfter > lastIndex:
            raise MatchAfterTooHigh('matchAfter = %d, '
                                    'lastIndex = %d' % (matchAfter,
                                                        self.lastIndex))
        elif matchAfter == lastIndex or not entries:
            # no entries can conflict, because entries begins
            # immediately after our last index or there are no entries
            return entries
        else:
            logIndex = matchAfter + 1
            for matchesUpTo, entry in enumerate(entries, 1):
                if logIndex >= len(self.log):
                    break
                elif self.log[logIndex].term != entry.term:
                    del self.log[logIndex:]
                    matchesUpTo -= 1
                    break
                else:
                    logIndex += 1
            return entries[matchesUpTo:]

    def appendNewEntries(self, entries):
        self.log.extend(entries)


RERUN_RPC = object()


class Server(object):
    '''A Raft participant.

    `peers`: identities for the server's peers

    `persister`: an object that implements the Persist protocol and
    can save and restore to stable storage

    `applyCommand`: callable invoked with the command to apply
     '''

    def __init__(self, cycle, identity, peers, persister, applyCommand,
                 electionTimeoutRange, commitIndex=0, lastApplied=0):
        self.cycle = cycle
        self.identity = identity
        self.peers = peers
        self.persister = persister
        self.applyCommand = applyCommand
        self.electionTimeoutRange = electionTimeoutRange
        self.commitIndex = commitIndex
        self.lastApplied = lastApplied
        self.applyCommitted()

    @classmethod
    def fromServer(cls, electionTimeoutRange, cycle, server):
        return cls(electionTimeoutRange=electionTimeoutRange,
                   cycle=server.cycle,
                   identity=server.identity,
                   peers=server.peers,
                   persister=server.persister,
                   commitIndex=server.commitIndex,
                   lastApplied=server.lastApplied)

    def applyCommitted(self):
        if self.lastApplied < self.commitIndex:
            for entry in self.persister.logSlice(self.lastApplied,
                                                 self.commitIndex + 1):
                self.applyCommand(entry.command)
            self.lastApplied = self.commitIndex

    def willBecomeFollower(self, term):
        if term > self.persister.currentTerm:
            self.persister.currentTerm = term
            self.cycle.changeState(Follower)
            return True
        return False

    def candidateIdOK(self, candidateId):
        return (self.persister.votedFor is None
                or self.persister.votedFor == candidateId)

    def candidateLogUpToDate(self, lastLogIndex, lastLogTerm):
        # Section 5.4.1
        if self.persister.indexMatchesTerm(index=self.persister.lastLogIndex,
                                           term=lastLogTerm):
            return self.persister.lastIndex <= lastLogIndex
        else:
            return self.persister.lastIndexNewerThanTerm(lastLogTerm)

    def remote_appendEntries(self,
                             term, leaderId, prevLogIndex,
                             prevLogTerm, entries, leaderCommit):
        # RPC
        if self.willBecomeFollower(term):
            return RERUN_RPC
        return self.persister.currentTerm, False

    def remote_requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):
        # RPC
        if term < self.persister.currentTerm:
            voteGranted = False
        else:
            voteGranted = (self.candidateIdOK(candidateId)
                           and self.candidateLogUpToDate(lastLogIndex,
                                                         lastLogTerm))
            self.persister.votedFor = candidateId
            self.willBecomeFollower(term)
        return self.persister.currentTerm, voteGranted


class StartsElection(Server):

    def resetElectionTimeout(self):
        if self.votingDeferred is not None:
            self.votingDeferred.cancel()
        self.electionTimeout = random.uniform(*self.electionTimeoutRange)
        self.becomeCandidateDeferred = reactor.callLater(self.electionTimeout,
                                                         self.cycle.changeState,
                                                         Candidate)

    def appendEntries(self, *args, **kwargs):
        self.resetElectionTimeout
        return super(StartsElection, self).appendEntries(*args, **kwargs)


class Follower(Server):
    '''A Raft follower.'''

    leaderId = None

    def remote_appendEntries(self,
                             term, leaderId, prevLogIndex,
                             prevLogTerm, entries, leaderCommit):
        # RPC
        # 1 & 2
        if (term < self.currentTerm
            or not self.persister.indexMatchesTerm(prevLogIndex,
                                                   prevLogTerm)):
            success = False
        else:
            # 3
            new = self.persister.matchLogToEntries(matchAfter=prevLogIndex,
                                                   entries=entries)
            # 4
            self.persister.appendNewEntries(new)

            # 5
            if leaderCommit > self.commitIndex:
                self.commitIndex = min(leaderCommit, self.persister.lastIndex)

            self.applyCommitted()

            self.leaderId = leaderId
            success = True
            self.resetElectionTimeout()

        return self.currentTerm, success

    def remote_command(self, command):
        return self.peers[self.leaderId].pb.callRemote('command', command)


class Candidate(StartsElection):

    def __init__(self, *args, **kwargs):
        super(Candidate, self).__init__(*args, **kwargs)

    def prepareForElection(self):
        self.persister.currentTerm += 1
        self.persister.votedFor = self.identity

    def willBecomeLeader(self, votesSoFar):
        if votesSoFar > len(self.peers) / 2 + 1:
            self.cycle.changeState(Leader)
            return True
        return False


class Leader(Server):
    '''A Raft leader.'''

    def __init__(self, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self.heartbeatInterval = min(self.electionTimeoutRange[0] - 50, 50)
        self.postElection()
        self.heartbeatLoopingCall = reactor.loopingCall(self.heartbeatInterval,
                                                        self.broadcastAppendEntries)

    def postElection(self):
        lastLogIndex = self.persister.lastLogIndex
        self.nextIndex = dict.fromkeys(self.peers, lastLogIndex + 1)
        self.matchIndex = dict.fromkeys(self.peers, 0)

    def updateCommitIndex(self):
        newCommitIndex = median(self.matchIndex.values())
        if newCommitIndex > self.commitIndex:
            self.commitIndex = newCommitIndex
            return True
        return False

    def receiveAppendEntries(self, result, identity, lastLogIndex):
        term, success = result
        if self.currentTerm < term:
            self.willBecomeFollower()
        elif not success:
            self.nextIndex[identity] -= 1
            # retry
        else:
            self.nextIndex[identity] = lastLogIndex + 1
            self.matchIndex[identity] = lastLogIndex
            if self.updateCommitIndex():
                self.applyCommitted()

    def sendAppendEntries(self, identity, pb):
        prevLogIndex = self.nextIndex[identity] - 1
        allEntries = self.persister.logSlice(start=prevLogIndex, end=None)
        prevLogTerm, entries = allEntries[0], allEntries[1:]
        lastLogIndex = self.persister.lastLogIndex

        d = pb.call('appendEntries',
                    term=self.persister.currentTerm,
                    candidateId=self.identity,
                    prevLogIndex=prevLogIndex,
                    prevLogTerm=prevLogTerm,
                    entries=entries)

        d.addCallback(self.receiveAppendEntries,
                      identity=identity,
                      lastLogIndex=lastLogIndex)
        return d

    def broadcastAppendEntries(self):
        for identity, pb in self.peers:
            self.sendAppendEntries(pb)

    def remote_command(self, command):
        self.persister.appendEntries([LogEntry(term=self.persister.currentTerm,
                                               command=command)])
        self.broadcastAppendEntries()
        return True


class ServerCycle(pb.Root):

    def __init__(self, identity, peers, persister, applyCommand,
                 electionTimeoutRange=(.150, .350)):
        self.identity = identity
        self.peers = peers
        self.persister = persister
        self.applyCommand = applyCommand
        self.electionTimeoutRange = electionTimeoutRange
        self.state = Follower(electionTimeoutRange=electionTimeoutRange,
                              cycle=self,
                              identity=identity,
                              peers=peers,
                              persister=persister,
                              applyCommand=applyCommand)

    def changeState(self, newState):
        self.state = newState.fromServer(self.electionTimeoutRange,
                                         cycle=self,
                                         server=self.state)

    def rerun(self, methodName, *args, **kwargs):
        result = RERUN_RPC
        while result is RERUN_RPC:
            method = getattr(self.state, methodName)
            result = method(*args, **kwargs)
        return result

    def remote_appendEntries(self,
                             term, leaderId, prevLogIndex,
                             prevLogTerm, entries, leaderCommit):
        return self.rereun('remote_appendEntries',
                           term, leaderId,
                           prevLogIndex,
                           prevLogTerm,
                           entries,
                           leaderCommit)

    def remote_requestVote(self,
                           term, candidateId, lastLogIndex, lastLogTerm):
        return self.rerun('remote_requestVote',
                          term, candidateId,
                          lastLogIndex, lastLogIndex)

    def remote_command(self, command):
        return self.rerun('remote_command', command)
