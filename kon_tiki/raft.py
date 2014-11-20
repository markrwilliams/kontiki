'''An implementation of the Raft consensus algorithm.

References:

[1] Ongaro, Diego and Ousterhout, John. "In Search of an
    Understandable Consensus Algorithm (Extended Version)". May 2014
    <https://ramcloud.stanford.edu/raft.pdf>

'''
import traceback
from kon_tiki.persist import LogEntry, MatchAfterTooHigh
from kon_tiki.fundamentals import majorityMedian
from twisted.python import log
from twisted.internet import reactor, defer, task
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
                   applyCommand=state.applyCommand,
                   persister=state.persister,
                   commitIndex=state.commitIndex,
                   lastApplied=state.lastApplied)

    def logFailure(self, failure, call=None):
        failure.trap(defer.CancelledError)
        log.msg('Failure in call %s: %s' % (call or "<Unknown>", failure))

    def track(self, deferred):
        self.pending.add(deferred)

        def removeFromPending(result):
            if deferred in self.pending:
                self.pending.remove(deferred)
            return result

        deferred.addBoth(removeFromPending)
        return deferred

    def cancelAll(self):
        # iterate over a shallow copy self.pending as the cancellation
        # will remove deferreds from it
        for pending in self.pending.copy():
            pending.cancel()

    def begin(self):
        return self.applyCommitted()

    def applyCommitted(self):

        def apply(logEntries):

            def incrementLastApplied(ignored):
                self.lastApplied += 1

            commandDeferreds = []
            for entry in logEntries:
                cmd = defer.maybeDeferred(self.applyCommand, entry.command)
                cmd.addCallback(incrementLastApplied)
                cmd.addErrback(self.logFailure)
                commandDeferreds.append(cmd)

            return defer.DeferredList(commandDeferreds)

        d = self.track(self.persister.committableLogEntries(self.lastApplied,
                                                            self.commitIndex))
        d.addCallback(apply)
        d.addErrback(self.logFailure)
        return d

    def becomeFollower(self, term):
        prep = [self.persister.setCurrentTerm(term),
                self.persister.voteFor(None)]

        resultsDeferred = defer.gatherResults(prep)

        def changeServerStateToFollower(results):
            newTerm, identity = results
            assert identity is None
            changeStateDeferred = self.server.changeState(Follower)
            changeStateDeferred.addCallback(lambda ignore: newTerm)
            return changeStateDeferred

        resultsDeferred.addCallback(changeServerStateToFollower)
        return resultsDeferred

    def willBecomeFollower(self, term, currentTerm=None):
        if currentTerm is None:
            currentTermDeferred = self.persister.getCurrentTerm()
        else:
            currentTermDeferred = defer.succeed(currentTerm)

        def judge(currentTerm):
            if term > currentTerm:
                d = self.becomeFollower(term)
                result = True
            else:
                d = defer.succeed(currentTerm)
                result = False

            def formatResult(resultTerm):
                log.msg('willBecomeFollower (currentTerm %s) '
                        'for term %s: %s' % (resultTerm, term, result))
                return resultTerm, result

            d.addCallback(formatResult)
            return d

        currentTermDeferred.addCallback(judge)
        currentTermDeferred.addErrback(self.logFailure)
        return currentTermDeferred

    def candidateIdOK(self, candidateId):
        votedForDeferred = self.persister.votedFor()

        def judgeCandidateId(votedFor):
            ok = votedFor is None or votedFor == candidateId
            log.msg('candidateIdOK: %s' % ok)
            return ok

        votedForDeferred.addCallback(judgeCandidateId)
        votedForDeferred.addErrback(self.logFailure)
        return votedForDeferred

    def candidateLogUpToDate(self, lastLogIndex, lastLogTerm):
        # Section 5.4.1
        currentTermDeferred = self.persister.getCurrentTerm()

        def gotCurrentTerm(currentTerm):
            if currentTerm == lastLogTerm:
                def compareLastIndices(lastIndex):
                    ok = lastIndex <= lastLogIndex
                    log.msg('candidateLogUpToDate: terms equal, '
                            'logs ok: %s' % ok)
                    return ok

                judgementDeferred = self.persister.getLastIndex()
                judgementDeferred.addCallback(compareLastIndices)
            else:
                judgementDeferred = self.persister.lastIndexLETerm(lastLogTerm)

                def report(result):
                    log.msg('candidateLogUpToDate: term is greater, '
                            'logs ok: %s' % result)
                    return result

                judgementDeferred.addCallback(report)

            return judgementDeferred

        currentTermDeferred.addCallback(gotCurrentTerm)
        return currentTermDeferred

    def appendEntries(self, term, leaderId, prevLogIndex,
                      prevLogTerm, entries, leaderCommit):

        d = self.willBecomeFollower(term)

        def rerunOrReturn(termAndBecameFollower):
            newTerm, becameFollower = termAndBecameFollower
            if becameFollower:
                return self.server.state.appendEntries(newTerm,
                                                       leaderId,
                                                       prevLogIndex,
                                                       prevLogTerm,
                                                       entries,
                                                       leaderCommit)
            else:
                log.msg("Rejecting appendEntries from %s because "
                        "term %s <= currentTerm %s"
                        % (leaderId, term, newTerm))
                return newTerm, False

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
                criteria = [self.candidateIdOK(candidateId),
                            self.candidateLogUpToDate(lastLogIndex,
                                                      lastLogTerm)]

                resultsDeferred = defer.gatherResults(criteria)

                def determineVote(results):
                    if all(results) and term > currentTerm:
                        log.msg("requestVote: candidate %s is OK"
                                % candidateId)
                        setVote = self.persister.voteFor(candidateId)

                        def becomeFollower(vote):
                            assert vote == candidateId
                            return self.becomeFollower(term)

                        def concedeVote(newTerm):
                            assert newTerm == term, (newTerm, term)
                            return newTerm, True

                        setVote.addCallback(becomeFollower)
                        setVote.addCallback(concedeVote)
                        return setVote
                    else:
                        log.msg('requestVote: candidate %s is not OK, '
                                'May become follower ' % candidateId)
                        return self.willBecomeFollower(term,
                                                       currentTerm=currentTerm)

                resultsDeferred.addCallback(determineVote)
                return resultsDeferred

        currentTermDeferred.addCallback(considerTerm)
        return currentTermDeferred


class StartsElection(State):
    becomeCandidateTimeout = None
    clock = reactor

    def begin(self):
        startupDeferred = super(StartsElection, self).begin()
        startupDeferred.addCallback(self.resetElectionTimeout)
        return startupDeferred

    def cancelBecomeCandidateTimeout(self):
        if (self.becomeCandidateTimeout is not None
           and self.becomeCandidateTimeout.active()):
            self.becomeCandidateTimeout.cancel()

    def cancelAll(self):
        self.cancelBecomeCandidateTimeout()
        super(StartsElection, self).cancelAll()

    def resetElectionTimeout(self, ignored=None):
        self.cancelBecomeCandidateTimeout()
        self.electionTimeout = random.uniform(*self.electionTimeoutRange)

        def timeoutOccured():
            log.msg('Election timeout occurred')
            return self.server.changeState(Candidate)

        d = self.clock.callLater(self.electionTimeout, timeoutOccured)
        self.becomeCandidateTimeout = d


class Follower(StartsElection):
    '''A Raft follower.'''

    leaderId = None

    def begin(self):
        log.msg('Became follower')
        return super(Follower, self).begin()

    def appendEntries(self, term, leaderId, prevLogIndex, prevLogTerm,
                      entries, leaderCommit):
        # RPC
        # 1 & 2
        criteria = [self.persister.getCurrentTerm(),
                    self.persister.indexMatchesTerm(index=prevLogIndex,
                                                    term=prevLogTerm)]

        def evaluateCriteria(results):
            currentTerm, indexMatchesTerm = results
            if term < currentTerm or not indexMatchesTerm:
                log.msg('Rejecting appendEntries:'
                        ' term < currentTerm %s indexMatchesTerm %s'
                        % (term < currentTerm, indexMatchesTerm))
                return defer.succeed((currentTerm, False))

            self.resetElectionTimeout()
            self.leaderId = leaderId

            if term > currentTerm:
                updateDeferred = self.persister.setCurrentTerm(term)
            elif term == currentTerm:
                updateDeferred = defer.succeed(currentTerm)
            else:
                assert False, "Why is term not >= currentTerm here?"

            def handleEntries(currentTerm):
                assert currentTerm == term
                d = self.persister.matchAndAppendNewLogEntries(
                    matchAfter=prevLogIndex, entries=entries)
                d.addCallback(lambda ignore: currentTerm)
                return d

            updateDeferred.addCallback(handleEntries)

            def getLastCommitIndex(currentTerm):
                d = self.persister.getLastIndex()

                def formatResult(lastCommitIndex):
                    return currentTerm, lastCommitIndex

                d.addCallback(formatResult)
                d.addErrback(lambda failure: failure)
                return d

            updateDeferred.addCallback(getLastCommitIndex)

            def updateCommitIndexAndApplyCommitted(currentTermAndlastIndex):
                currentTerm, lastIndex = currentTermAndlastIndex
                if leaderCommit > self.commitIndex:
                    self.commitIndex = min(leaderCommit, lastIndex)

                stateMachineDeferred = self.applyCommitted()

                def returnSuccess(ignored):
                    return currentTerm, True

                stateMachineDeferred.addCallback(returnSuccess)
                return stateMachineDeferred

            updateDeferred.addCallback(updateCommitIndexAndApplyCommitted)

            def returnFalseOnMatchAfterTooHigh(failure):
                failure.trap(MatchAfterTooHigh)
                return currentTerm, False

            updateDeferred.addErrback(returnFalseOnMatchAfterTooHigh)

            return updateDeferred

        resultsDeferred = defer.gatherResults(criteria)
        resultsDeferred.addCallback(evaluateCriteria)
        return resultsDeferred

    def command(self, command):
        d = self.track(self.peers[self.leaderId].callRemote('command',
                                                            command))
        d.addErrback(self.logFailure, call="command")
        return d


class Candidate(StartsElection):

    def begin(self):
        log.msg('Became candidate')
        # TODO: Should bother to apply committable entries?
        startupDeferred = super(Candidate, self).begin()
        startupDeferred.addCallback(self.conductElection)
        return startupDeferred

    def prepareForElection(self):
        self.resetElectionTimeout()
        self.votes = 0
        # these are purely for inspection
        self.cachedCurrentTerm = None
        self.cachedLastLogIndex = None
        self.cachedLastLogTerm = None

        prep = [self.persister.setCurrentTerm(None, increment=True),
                self.persister.getLastIndex(),
                self.persister.getLastTerm(),
                self.persister.voteFor(self.identity)]

        def cacheAndProcess(results):
            assert results[-1] == self.identity
            self.votes += 1
            processed = (self.cachedCurrentTerm,
                         self.cachedLastLogIndex,
                         self.cachedLastLogTerm) = results[:-1]
            return processed

        waitForAllPreparations = defer.gatherResults(prep)
        waitForAllPreparations.addCallback(cacheAndProcess)
        return waitForAllPreparations

    def willBecomeLeader(self, votesSoFar):
        majority = len(self.peers) / 2 + 1
        log.msg('willBecomeLeader: votesSoFar: %d, majority: %s' % (votesSoFar,
                                                                    majority))
        if votesSoFar >= majority:
            changeStateDeferred = self.server.changeState(Leader)
            changeStateDeferred.addCallback(lambda ignore: True)
        return defer.succeed(False)

    def completeRequestVote(self, result, peer):
        term, voteGranted = result
        log.msg('completeRequestVote from %r: term: %r, voteGranted %r'
                % (peer.identity, term, voteGranted))
        becameFollowerDeferred = self.willBecomeFollower(term)

        def shouldBecomeLeader(termAndBecameFollower):
            _, becameFollower = termAndBecameFollower

            if not becameFollower and voteGranted:
                self.votes += 1
                return self.willBecomeLeader(self.votes)
            else:
                log.msg('Lost Election')

        return becameFollowerDeferred.addCallback(shouldBecomeLeader)

    def sendRequestVote(self, peer, term, lastLogIndex, lastLogTerm):
        log.msg('sendRequestVote to %s; currentTerm %s' % (peer.identity,
                                                           term))
        d = self.track(peer.callRemote('requestVote',
                                       term,
                                       self.identity,
                                       lastLogIndex,
                                       lastLogTerm))
        d.addCallback(self.completeRequestVote, peer)
        d.addErrback(self.logFailure, call="requestVote")
        return d

    def broadcastRequestVote(self, cached, returnDeferred=False):
        peerPoll = [self.sendRequestVote(peer, *cached)
                    for peer in self.peers.values()]

        resultsDeferred = defer.gatherResults(peerPoll)
        if returnDeferred:
            # this is for unit testing, where we want to make sure all
            # deferreds are accessible for clean up by trial.
            # generally we do NOT want to return this, as it will be
            # sent downward through self.conductElection, through
            # self.begin to the server's begin, which is guarded by
            # the RPC DeferredLock.  if this deferred DID reach that
            # lock, then no RPC calls could be received during an
            # election!
            return resultsDeferred

    def conductElection(self, ignored=None, *args, **kwargs):
        log.msg('Conducting election')
        preparationDeferred = self.prepareForElection()

        preparationDeferred.addCallback(self.broadcastRequestVote,
                                        *args, **kwargs)
        return preparationDeferred

    def command(self, command):
        # TODO: a deferred that lasts until an election has been won?
        return False


class Leader(State):
    '''A Raft leader.'''
    heartbeatLoopingCall = None
    lowerBound = 0.02

    def __init__(self, *args, **kwargs):
        super(Leader, self).__init__(*args, **kwargs)
        etRange = self.electionTimeoutRange
        self.heartbeatInterval = self.calculateHeartbeatInterval(etRange)

    def calculateHeartbeatInterval(self, electionTimeoutRange,
                                   lowerBound=None):
        if lowerBound is None:
            lowerBound = self.lowerBound
        lowerTimeoutBound, _ = self.electionTimeoutRange
        return min(lowerTimeoutBound / 10.0, lowerBound)

    def begin(self, lowerBound=None):
        log.msg('BECAME LEADER! ' * 10)
        startupDeferred = super(Leader, self).begin()
        startupDeferred.addCallback(self.postElection)
        return startupDeferred

    def cancelAll(self):
        if self.heartbeatLoopingCall and self.heartbeatLoopingCall.running:
            self.heartbeatLoopingCall.stop()
        super(Leader, self).cancelAll()

    def postElection(self, ignored=None):
        print 'HEARTBEAT INTERVAL', self.heartbeatInterval
        d = task.LoopingCall(self.broadcastAppendEntries)
        self.heartbeatLoopingCall = d

        lastLogIndexDeferred = self.persister.getLastIndex()

        def getLastLogIndex(lastLogIndex):
            self.nextIndex = dict.fromkeys(self.peers, lastLogIndex + 1)
            self.matchIndex = dict.fromkeys(self.peers, 0)
            self.matchIndex[self.identity] = lastLogIndex

        lastLogIndexDeferred.addCallback(getLastLogIndex)

        def start(ignored):
            self.heartbeatLoopingCall.start(self.heartbeatInterval)

        lastLogIndexDeferred.addCallback(start)

        return lastLogIndexDeferred

    def updateCommitIndex(self):
        newCommitIndex = majorityMedian(self.matchIndex.values())
        if newCommitIndex > self.commitIndex:
            self.commitIndex = newCommitIndex
            return True
        return False

    def completeAppendEntries(self, result, identity, lastLogIndex):
        term, success = result
        getCurrentTermDeferred = self.persister.getCurrentTerm()

        def compareTerm(currentTerm):
            if currentTerm < term:
                return self.willBecomeFollower(term)
            elif not success:
                self.nextIndex[identity] -= 1
                # explicit rety maybe?
                # return self.sendAppendEntries(identity, self.peers[identity])
            else:
                self.nextIndex[identity] = lastLogIndex + 1
                self.matchIndex[identity] = lastLogIndex
                if self.updateCommitIndex():
                    return self.applyCommitted()

        return getCurrentTermDeferred.addCallback(compareTerm)

    def sendAppendEntries(self, identity, perspective):
        prevLogIndex = self.nextIndex[identity] - 1

        viewDeferred = self.persister.appendEntriesView(prevLogIndex)

        def sendPeerEntries(result):
            currentTerm, lastLogIndex, prevLogTerm, entries = result
            entries = [tuple(entry) for entry in entries]
            commitIndex = self.commitIndex
            d = self.track(perspective.callRemote('appendEntries',
                                                  term=currentTerm,
                                                  leaderId=self.identity,
                                                  prevLogIndex=prevLogIndex,
                                                  prevLogTerm=prevLogTerm,
                                                  entries=entries,
                                                  leaderCommit=commitIndex))

            d.addCallback(self.completeAppendEntries,
                          identity=identity,
                          lastLogIndex=lastLogIndex)
            d.addErrback(self.logFailure, call="appendEntries")

        viewDeferred.addCallback(sendPeerEntries)
        return viewDeferred

    def broadcastAppendEntries(self, ignored=None):
        log.msg('BROADCASTING APPENDENTRIES')
        for identity, perspective in self.peers.items():
            self.sendAppendEntries(identity, perspective)

    def command(self, command):
        newEntriesDeferred = self.persister.getCurrentTerm()

        def addEntries(term):
            entries = [LogEntry(term=term,
                                command=command)]
            addDeferred = self.persister.addNewEntries(entries)

            def updateMyMatchIndex(matchIndex):
                self.matchIndex[self.identity] = matchIndex

            addDeferred.addCallback(updateMyMatchIndex)
            addDeferred.addCallback(self.broadcastAppendEntries)
            addDeferred.addCallback(lambda _: True)

        return newEntriesDeferred.addCallback(addEntries)
