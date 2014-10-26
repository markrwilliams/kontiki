'''An implementation of the Raft consensus algorithm'''
import itertools
from collections import namedtuple

LogEntry = namedtuple('LogEntry', 'term command')


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
        self.lastIndex = 0

    def indexMatchesTerm(self, index, term):
        if index == -1 and not self.log:
            return True
        return index < len(self.log) and self.log[index].term == term

    def updateEntries(self, prevIndex, prevTerm, entries):
        # appendEntries #3
        if not self.indexMatchesTerm(prevIndex, prevTerm):
            return False

        index = prevIndex + 1
        newEntries = entries
        entriesIterator = iter(entries) if index < len(self.log) else ()

        for entry in entriesIterator:
            if self.log[index].term != entry.term:
                del self.log[index:]
                newEntries = itertools.chain([entry], entriesIterator)
                break
            index += 1

        # appendEntries #4
        self.log.extend(newEntries)
        self.lastIndex = len(self.log) - 1
        return True


class Server(object):
    '''A Raft participant.

    `peers`: identities for the server's peers

    `persister`: an object that implements the Persist protocol and
    can save and restore to stable storage
    '''

    def __init__(self, peers, persister):
        self.peers = peers
        self.persister = persister
        self.commitIndex = 0
        self.lastApplied = 0

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
        # 1.
        if term < self.currentTerm:
            return self.currentTerm, False
        # 2
        if (prevLogIndex >= len(self.persister.log)
           or self.persister.log[prevLogIndex].term != term):
            return self.currentTerm, False

        # 5
        if leaderCommit > self.commitIndex:
            lastIndex = len(self.log) - 1
            self.commitIndex = min(leaderCommit, lastIndex)

        self.leaderId = leaderId
        return self.currentTerm, True
