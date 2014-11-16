'''An implementation of the Raft consensus algorithm.

References:

[1] Ongaro, Diego and Ousterhout, John. "In Search of an
   Understandable Consensus Algorithm (Extended Version)". May 2014
   <https://ramcloud.stanford.edu/raft.pdf>

'''
from kon_tiki.persist import LogEntry
from kon_tiki.fundamentals import majorityMedian
from twisted.python import log
from twisted.internet import reactor, defer
from twisted.spread import pb
import random

RERUN_RPC = object()


class State(object):
    '''A Raft participant.

    `peers`: identities for the server's peers

    `persister`: an object that implements the Persist protocol and
    can save and restore to stable storage

    `applyCommand`: callable invoked with the command to apply
     '''

    def __init__(self, server, identity, peers, persister, applyCommand,
                 electionTimeoutRange, commitIndex=0, lastApplied=0):
        self.server = server
        self.identity = identity
        self.peers = peers
        self.persister = persister
        self.applyCommand = applyCommand
        self.electionTimeoutRange = electionTimeoutRange
        self.commitIndex = commitIndex
        self.lastApplied = lastApplied
        self.pending = set()

    @classmethod
    def fromState(cls, electionTimeoutRange, server, state):
        state.cancelAll()
        return cls(electionTimeoutRange=electionTimeoutRange,
                   server=server,
                   identity=state.identity,
                   peers=state.peers,
                   persister=state.persister,
                   commitIndex=state.commitIndex,
                   lastApplied=state.lastApplied)

    def logFailure(self, failure, call):
        log.msg("Call %s fai led" % call, failure)

    def track(self, deferred):
        self.pending.add(deferred)

        def removeFromPending(result):
            self.pending.remove(deferred)
            return result

        deferred.addBoth(removeFromPending)
        return deferred

    def cancelAll(self):
        while self.pending:
            self.pending.pop().cancel()

    def begin(self):
        self.applyCommitted()

    def applyCommitted(self):

        def apply(logEntries):

            def incrementLastApplied(ignored):
                self.lastApplied += 1

            commandDeferreds = []
            for entry in logEntries:
                cmd = defer.maybeDeferred(self.applyCommmand(entry.command))
                cmd.addCallback(incrementLastApplied)
                cmd.addErrback(self.logFailure)
                commandDeferreds.append(cmd)

            return defer.DeferredList(commandDeferreds)

        d = self.track(self.persister.committableLogEntries(self.lastApplied))
        d.addCallback(apply)
        d.addErrback(self.logFailure)
        return d

    def willBecomeFollower(self, term):
        currentTermDeferred = self.persister.getCurrentTerm()

        def judge(currentTerm):
            if term > currentTerm:
                d = self.persister.setCurrentTerm(term)
                self.server.changeState(Follower)
                result = True
            else:
                d = defer.succeed(currentTerm)
                result = False

            def formatResult(resultTerm):
                return resultTerm, result

            d.addCallback(formatResult)
            return d

        currentTermDeferred.addCallback(judge)
        currentTermDeferred.addErrback(self.logFailure)
        return currentTermDeferred

    def candidateIdOK(self, candidateId):
        votedForDeferred = self.persister.votedFor()

        def judgeCandidateId(votedFor):
            return votedFor is None or votedFor == candidateId

        votedForDeferred.addCallback(judgeCandidateId)
        votedForDeferred.addErrback(self.logFailure)
        return votedForDeferred

    def candidateLogUpToDate(self, lastLogIndex, lastLogTerm):
        # Section 5.4.1
        currentTermDeferred = self.persister.getCurrentTerm()

        def gotCurrentTerm(currentTerm):
            if self.persister.currentTerm == lastLogTerm:

                def compareLastIndices(lastIndex):
                    return lastIndex <= lastLogIndex

                judgementDeferred = self.persister.getLastIndex()
                judgementDeferred.addCallback(compareLastIndices)
            else:
                judgementDeferred = self.persister.lastIndexLETerm(lastLogTerm)
            return judgementDeferred

        currentTermDeferred.addCallback(gotCurrentTerm)

    def appendEntries(self, term, leaderId, prevLogIndex,
                      prevLogTerm, entries, leaderCommit):

        d = self.willBecomeFollower(term)

        def rerunOrReturn(termAndBecameFollower):
            term, becameFollower = termAndBecameFollower
            if becameFollower:
                return self.server.state.remote_appendEntries(term,
                                                              leaderId,
                                                              prevLogIndex,
                                                              prevLogTerm,
                                                              entries,
                                                              leaderCommit)
            else:
                return term, False

        d.addCallback(rerunOrReturn)
        d.addErrback(self.logFailure)
        return d

    def requestVote(self, term, candidateId, lastLogIndex, lastLogTerm):
        # RPC
        currentTermDeferred = self.persister.getCurrentTerm()

        def considerTerm(currentTerm):
            if term < currentTerm:
                return currentTerm, False
            else:
                criteria = [self.candidateIdOk(candidateId),
                            self.candidateLogUpToDate(lastLogIndex,
                                                      lastLogTerm)]
                resultsDeferred = defer.gatherResults(criteria)

                def determineVote(results):
                    if all(results):
                        setVote = self.persister.voteFor(candidateId)

                        def becomeFollower(identity):
                            assert identity == candidateId
                            return self.willBecomeFollower(term)

                        def concedeVote(termAndBecameFollower):
                            term, becameFollower = termAndBecameFollower
                            assert becameFollower
                            return term, True

                        setVote.addCallback(becomeFollower)
                        setVote.addCallback(concedeVote)
                        return setVote
                    else:
                        return currentTerm, False

                resultsDeferred.addCallback(determineVote)
                return resultsDeferred

        currentTermDeferred.addCallback(considerTerm)
        return currentTermDeferred


class StartsElection(State):
    becomeCandidateTimeout = None
    clock = reactor

    def begin(self):
        super(StartsElection, self).begin()
        self.resetElectionTimeout()

    def cancelBecomeCandidateTimeout(self):
        if (self.becomeCandidateTimeout is not None
           and self.becomeCandidateTimeout.active()):
            self.becomeCandidateTimeout.cancel()

    def cancelAll(self):
        self.cancelBecomeCandidateTimeout()
        super(StartsElection, self).cancelAll()

    def resetElectionTimeout(self):
        self.cancelBecomeCandidateTimeout()
        self.electionTimeout = random.uniform(*self.electionTimeoutRange)
        d = self.clock.callLater(self.electionTimeout,
                                 self.server.changeState,
                                 Candidate)
        self.becomeCandidateTimeout = d

    def remote_appendEntries(self, *args, **kwargs):
        self.resetElectionTimeout()
        return super(StartsElection, self).appendEntries(*args, **kwargs)


class Follower(State):
    '''A Raft follower.'''

    leaderId = None

    def remote_appendEntries(self,
                             term, leaderId, prevLogIndex,
                             prevLogTerm, entries, leaderCommit):
        # RPC
        # 1 & 2
        if (term < self.persister.currentTerm
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

        return self.persister.currentTerm, success

    def remote_command(self, command):
        d = self.track(self.peers[self.leaderId].callRemote('command',
                                                            command))
        d.addErrback(self.logFailure, call="command")
        return d


class Candidate(StartsElection):

    def begin(self):
        super(Candidate, self).begin()
        self.conductElection()

    def prepareForElection(self):
        self.persister.currentTerm += 1
        self.persister.votedFor = self.identity
        self.votes = 0

    def willBecomeLeader(self, votesSoFar):
        if votesSoFar >= len(self.peers) / 2 + 1:
            self.server.changeState(Leader)
            return True
        return False

    def completeRequestVote(self, result):
        term, voteGranted = result
        if not self.willBecomeFollower(term) and voteGranted:
            self.willBecomeLeader(self.votes)

    def sendRequestVote(self, perspective):
        term = self.persister.currentTerm
        lastLogIndex = self.persister.lastLogIndex
        lastLogTerm = self.logSlice(lastLogIndex - 1)
        if lastLogTerm:
            (lastLogTerm,) = lastLogTerm
        else:
            lastLogTerm = 0
        d = self.track(perspective.callRemote('requestVote',
                                              term,
                                              self.identity,
                                              lastLogIndex,
                                              lastLogTerm))
        d.addCallback(self.completeRequestVote)
        d.addErrback(self.logFailure, call="requestVote")
        return d

    def broadcastRequestVote(self):
        for perspective in self.peers.values():
            self.sendRequestVote(perspective)

    def conductElection(self):
        self.prepareForElection()
        self.broadcastRequestVote()

    def remote_appendEntries(self, *args, **kwargs):
        return self.persister.currentTerm, False


class Leader(State):
    '''A Raft leader.'''
    heartbeatLoopingCall = None
    clock = reactor

    def begin(self):
        # see Section 5.6, "Timing and availability" in [1]
        self.heartbeatInterval = min(self.electionTimeoutRange[0] / 10.0, 0.02)
        self.postElection()

    def cancelAll(self):
        if self.heartbeatLoopingCall:
            self.heartbeatLoopingCall.stop()
        super(Leader, self).cancelAll()

    def postElection(self):
        d = self.clock.loopingCall(self.heartbeatInterval,
                                   self.broadcastAppendEntries)
        self.heartbeatLoopingCall = d

        lastLogIndex = self.persister.lastLogIndex
        self.nextIndex = dict.fromkeys(self.peers, lastLogIndex + 1)
        self.matchIndex = dict.fromkeys(self.peers, 0)

    def updateCommitIndex(self):
        newCommitIndex = majorityMedian(self.matchIndex.values())
        if newCommitIndex > self.commitIndex:
            self.commitIndex = newCommitIndex
            return True
        return False

    def completeAppendEntries(self, result, identity, lastLogIndex):
        term, success = result
        if self.persister.currentTerm < term:
            self.willBecomeFollower()
        elif not success:
            self.nextIndex[identity] -= 1
            # retry
        else:
            self.nextIndex[identity] = lastLogIndex + 1
            self.matchIndex[identity] = lastLogIndex
            if self.updateCommitIndex():
                self.applyCommitted()

    def sendAppendEntries(self, identity, perspective):
        prevLogIndex = self.nextIndex[identity] - 1
        allEntries = self.persister.logSlice(start=prevLogIndex, end=None)
        prevLogTerm, entries = allEntries[0], allEntries[1:]
        lastLogIndex = self.persister.lastLogIndex

        d = self.track(perspective.callRemote('appendEntries',
                                              term=self.persister.currentTerm,
                                              candidateId=self.identity,
                                              prevLogIndex=prevLogIndex,
                                              prevLogTerm=prevLogTerm,
                                              entries=entries))

        d.addCallback(self.completeAppendEntries,
                      identity=identity,
                      lastLogIndex=lastLogIndex)
        d.addErrback(self.logFailure, call="appendEntries")
        return d

    def broadcastAppendEntries(self):
        for identity, perspective in self.peers:
            self.sendAppendEntries(identity, perspective)

    def remote_command(self, command):
        self.persister.appendEntries([LogEntry(term=self.persister.currentTerm,
                                               command=command)])
        self.broadcastAppendEntries()
        return True


class StateCycle(pb.Root):

    def __init__(self, identity, peers, persister, applyCommand,
                 electionTimeoutRange=(.150, .350)):
        self.identity = identity
        self.peers = peers
        self.persister = persister
        self.applyCommand = applyCommand
        self.electionTimeoutRange = electionTimeoutRange
        self.state = Follower(electionTimeoutRange=electionTimeoutRange,
                              server=self,
                              identity=identity,
                              peers=peers,
                              persister=persister,
                              applyCommand=applyCommand)

    def changeState(self, newState, begin=True):
        self.state = newState.fromState(self.electionTimeoutRange,
                                         server=self,
                                         state=self.state)
        if begin:
            self.state.begin()

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
