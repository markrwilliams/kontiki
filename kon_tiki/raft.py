'''An implementation of the Raft consensus algorithm'''
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

    def indexMatchesTerm(self, index, term):
        if index == -1 and not self.log:
            return True
        return -1 < index < len(self.log) and self.log[index].term == term

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


class Server(object):
    '''A Raft participant.

    `peers`: identities for the server's peers

    `persister`: an object that implements the Persist protocol and
    can save and restore to stable storage
     '''
    nextState = None

    def __init__(self, peers, persister, commitIndex=0, lastApplied=0):
        self.peers = peers
        self.persister = persister
        self.commitIndex = commitIndex
        self.lastApplied = lastApplied

    def candidateIdOK(self, candidateId):
        return (self.persister.votedFor is not None
                or self.persister.votedFor == candidateId)

    def candidateLogUpToDate(self, lastLogIndex, lastLogTerm):
        if lastLogTerm < self.persister.log[-1].term:
            return False
        if lastLogIndex < len(self.persister.log) - 1:
            return False
        return True

    def requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):
        # RPC
        if term < self.currentTerm:
            voteGranted = False
        else:
            voteGranted = (self.candidateIdOK(candidateId)
                           and self.candidateLogUpToDate(lastLogIndex,
                                                         lastLogTerm))
            self.persister.votedFor = candidateId
        return self.currentTerm, voteGranted


class Leader(Server):
    '''A Raft leader.'''

    def wonElection(self):
        lastLogIndex = len(self.persister.logs())
        self.nextIndex = dict.fromkeys(self.peer, lastLogIndex)
        self.matchIndex = dict.fromkeys(self.peers, 0)


class Follower(Server):
    '''A Raft follower.'''

    leaderId = None

    def appendEntries(self,
                      term, leaderId, prevLogIndex,
                      prevLogTerm, entries, leaderCommit):
        # RPC
        # 1 & 2
        if (term < self.currentTerm
           or not self.persister.indexMatchesTerm(prevLogIndex, prevLogTerm)):
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

            self.leaderId = leaderId
            success = True

        return self.currentTerm, success
