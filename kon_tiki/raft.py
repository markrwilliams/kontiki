'''An implementation of the Raft consensus algorithm'''
import operator
from twisted.internet import reactor, defer
from twisted.spread import pb
from collections import namedtuple


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
    nextState = None

    def __init__(self, identity, peers, persister, applyCommand,
                 commitIndex=0, lastApplied=0):
        self.identity = identity
        self.peers = peers
        self.persister = persister
        self.commitIndex = commitIndex
        self.lastApplied = lastApplied
        self.applyCommand = applyCommand
        self.applyCommited()

    def applyCommitted(self):
        if self.lastApplied < self.commitIndex:
            for entry in self.persister.logSlice(self.lastApplied,
                                                 self.commitIndex + 1):
                self.applyCommand(entry.command)
            self.lastApplied = self.commitIndex

    def willBecomeFollower(self, term):
        if term > self.persister.currentTerm:
            self.persister.currentTerm = term
            self.nextState = (Follower
                              if not isinstance(self, Follower)
                              else None)
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


class Candidate(Server):

    def prepareForElection(self):
        self.persister.currentTerm += 1
        self.persister.votedFor = self.identity

    def willBecomeLeader(self, votesSoFar):
        if votesSoFar > len(self.peers) / 2 + 1:
            self.next_state = Leader
            return True
        return False


class Leader(Server):
    '''A Raft leader.'''

    def __init__(self, *args, **kwargs):
        super(Server, self).__init__(*args, **kwargs)
        self.postElection()

    def postElection(self):
        lastLogIndex = len(self.persister.logs())
        self.nextIndex = dict.fromkeys(self.peer, lastLogIndex)
        self.matchIndex = dict.fromkeys(self.peers, 0)


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

        return self.currentTerm, success


class ServerCycle(pb.Root):

    def __init__(self, identity, peers, persister, applyCommand,
                 electionTimeoutRange=(.150, .350)):
        self.identity = identity
        self.peers = peers
        self.persister = persister
        self.applyCommand = applyCommand
        self.electionTimeoutRange = electionTimeoutRange
        self.state = Follower(identity, peers, persister, applyCommand)

    def resetElectionTimer(self):
        self.electionTimeout = random.randrange(*self.electionTimeout)
        reactor.callLater(random.uniform(*self.electionTimeout),
                          self.electionTimeout)

    def electionTimeout(self):
        self.reactor.callLater
        self.state = Candidate(self.state.identity,
                               self.state.peers,
                               self.state.persister,
                               self.state.applyCommand,
                               self.state.commitIndex,
                               self.state.lastApplied)
        for peer in self.peers:
            pass

    def remote_appendEntries(self,
                             term, leaderId, prevLogIndex,
                             prevLogTerm, entries, leaderCommit):
        return self.state.remote_appendEntries(term, leaderId,
                                               prevLogIndex,
                                               prevLogTerm,
                                               entries,
                                               leaderCommit)

    def remote_requestVote(self,
                           term, candidateId, lastLogIndex, lastLogTerm):
        pass
