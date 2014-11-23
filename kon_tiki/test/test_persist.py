import itertools
from twisted.internet import defer
from twisted.trial import unittest
from kon_tiki import persist
from kon_tiki.test.common import dropResult


firstEntries = [persist.LogEntry(term=0, command='first1'),
                persist.LogEntry(term=0, command='first2'),
                persist.LogEntry(term=1, command='first3')]
appendedEntries = [persist.LogEntry(term=2, command='append1'),
                   persist.LogEntry(term=3, command='append2')]


class SQLitePersistTestCase(unittest.TestCase):

    def setUp(self):
        self.persister = persist.SQLitePersist(':memory:')
        self.persister.connect()

    def test_setAndGetCurrentTerm(self):
        '''getCurrentTerm should return the current term; if the persister is
        new, the term should be 0.  setcurrentTerm should durably set
        the current term, and it should be retrievable by
        getCurrentTerm.

        '''
        results = self.persister.getCurrentTerm()

        zeroFailureMessage = 'getCurrentTerm for new persister should return 0'
        results.addCallback(self.assertEqual, 0, msg=zeroFailureMessage)

        results.addCallback(
            dropResult(self.persister.setCurrentTerm, 123))

        setCurrentTermFailureMessage = 'setCurrentTerm did not set the term'
        results.addCallback(self.assertEqual, 123,
                            msg=setCurrentTermFailureMessage)

        results.addCallback(dropResult(self.persister.getCurrentTerm))

        getCurrentTermFailureMessage = ('getCurrentTerm did not agree'
                                        ' with setCurrentTerm')
        results.addCallback(self.assertEqual, 123,
                            msg=getCurrentTermFailureMessage)
        return results

    def test_incrementCurrentTerm(self):
        '''setCurrentTerm should durably increment the currentTerm and return
        it.

        '''
        results = defer.succeed(None)

        for expected in (1, 2, 3):
            results.addCallback(dropResult(self.persister.setCurrentTerm,
                                           None, increment=True))

            incrementFailureMessage = ('setCurrentTerm incremented term'
                                       ' incorrectly')
            results.addCallback(self.assertEqual, expected,
                                msg=incrementFailureMessage)

            results.addCallback(dropResult(self.persister.getCurrentTerm))

            getCurrentTermFailureMessage = ('getCurrentTerm disagreed with'
                                            ' incremented term')
            results.addCallback(self.assertEqual, expected,
                                msg=getCurrentTermFailureMessage)

        return results

    def test_votedForAndvoteFor(self):
        '''votedFor should return the identity of the candidate voted for most
        recently; if the persister is new, it should return None.

        voteFor should durably set the identity of the candidate voted
        for, and that identity should be retrievable by votedFor

        '''
        results = self.persister.votedFor()

        noneVotedForFailure = 'votedFor for a empty persister should be None'

        results.addCallback(self.assertEqual, None, msg=noneVotedForFailure)

        for identity in ('first id', 'second id'):
            results.addCallback(dropResult(self.persister.voteFor, identity))

            voteForFailureMessage = ('voteFor did not correctly set the'
                                     ' candidate identity')
            results.addCallback(self.assertEqual, identity,
                                msg=voteForFailureMessage)

            results.addCallback(dropResult(self.persister.votedFor))

            votedForFailureMessage = 'votedFor disagreed with voteFor'
            results.addCallback(self.assertEqual, identity,
                                msg=votedForFailureMessage)

        return results

    def test_getLastTerm(self):
        '''getLastTerm should return the term of the last log entry; an empty
        persister's getLastTerm should return 0.

        '''
        results = self.persister.getLastTerm()

        zeroFailureMessage = "getLastTerm for an empty persister should return 0"
        results.addCallback(self.assertEqual, 0, msg=zeroFailureMessage)

        for entries in (firstEntries, appendedEntries):
            results.addCallback(
                dropResult(self.persister.addNewEntries, entries))

            results.addCallback(
                dropResult(self.persister.getLastTerm))

            failureMessage = 'getLastTerm returned wrong term'

            results.addCallback(self.assertEqual, entries[-1].term,
                                msg=failureMessage)
        return results

    def test_getLastIndex(self):
        '''getLastIndex should return the index of the last log entry; an empty
        persister's getLastIndex should return -1.

        '''
        results = self.persister.getLastIndex()

        negOneFailureMessage = ('getLastTerm for an empty persister'
                                ' should return -1')
        results.addCallback(self.assertEqual, -1, msg=negOneFailureMessage)

        lastIndex = -1
        for entries in (firstEntries, appendedEntries):
            lastIndex += len(entries)
            results.addCallback(
                dropResult(self.persister.addNewEntries, entries))

            results.addCallback(
                dropResult(self.persister.getLastIndex))

            failureMessage = 'getLastIndex returned the wrong index'
            results.addCallback(self.assertEqual, lastIndex,
                                msg=failureMessage)
        return results

    def test_committableLogEntries(self):
        '''committableLogEntries should return log entries with indices in the
        range (lastApplied, commitIndex].  An empty persister has no
        committable log entries.

        '''
        results = self.persister.committableLogEntries(0, 100)

        emptyFailureMessage = ('committableLogEntries for an empty'
                               ' persister should return an empty list')
        results.addCallback(self.assertEqual, [], msg=emptyFailureMessage)

        results.addCallback(
            dropResult(self.persister.addNewEntries, firstEntries))

        indices = range(-1, len(firstEntries))
        for lastApplied, commitIndex in itertools.combinations(indices, 2):
            results.addCallback(
                dropResult(self.persister.committableLogEntries,
                           lastApplied, commitIndex))

            def assertEntries(result,
                              lastApplied=lastApplied,
                              commitIndex=commitIndex):
                # convert (,] interval to [,)
                start, stop = lastApplied + 1, commitIndex + 1
                expected = firstEntries[start:stop]

                failureMessage = ('committableLogEntries returned wrong'
                                  ' entries for lastApplied %d and'
                                  ' commitIndex %d' % (lastApplied,
                                                       commitIndex))

                self.assertEqual(result, expected, msg=failureMessage)

            results.addCallback(assertEntries)

        return results

    def test_disjointMatchAndAppendNewLogEntries(self):
        '''matchAndAppendNewLogEntries should append disjoint logs.

        '''
        results = self.persister.matchAndAppendNewLogEntries(-1, firstEntries)

        results.addCallback(dropResult(self.persister.getLastIndex))

        indexWrongFailure = ('getLastIndex claims'
                             ' matchAndAppendNewLogEntries'
                             ' failed to append entries')

        firstEntriesNewIndex = len(firstEntries) - 1

        results.addCallback(self.assertEqual, firstEntriesNewIndex,
                            msg=indexWrongFailure)

        results.addCallback(dropResult(self.persister._logSlice, 0, -1))

        logSliceWrongFailure = ('logSlice claims matchAndAppendNewLogEntries'
                                ' failed to append entries')

        results.addCallback(self.assertEqual, firstEntries,
                            msg=logSliceWrongFailure)

        results.addCallback(
            dropResult(self.persister.matchAndAppendNewLogEntries,
                       firstEntriesNewIndex, appendedEntries))

        results.addCallback(
            dropResult(self.persister.getLastIndex))

        results.addCallback(self.assertEqual,
                            len(firstEntries) + len(appendedEntries) - 1,
                            msg=indexWrongFailure)

        results.addCallback(
            dropResult(self.persister._logSlice, 0, -1))

        results.addCallback(self.assertEqual, firstEntries + appendedEntries,
                            msg=logSliceWrongFailure)
        return results

    def test_indexMatchesTerm(self):
        '''indexMatchesTerm should return True if the log entry's term matches
        term at the index, or if there are no entries in the log.

        '''
        results = self.persister.indexMatchesTerm(10, 123)
        trueFailureMessage = ('indexMatchesTerm should return True for'
                              ' an empty persister')
        results.addCallback(self.assertTrue, msg=trueFailureMessage)

        results.addCallback(
            dropResult(self.persister.addNewEntries, firstEntries))

        for idx, entry in enumerate(firstEntries):

            def closureCallbacks(idx=idx, entry=entry):
                results.addCallback(
                    dropResult(self.persister.indexMatchesTerm,
                               idx, entry.term))

                failureMessage = ('indexMatchesTerm returned False for'
                                  ' idx %d and term %d' % (idx, entry.term))
                results.addCallback(self.assertTrue, msg=failureMessage)

            closureCallbacks()

        return results

    def test_lastIndexLETerm(self):
        '''lastIndexLETerm should return True if the persister's lastIndex is
        less than or equal to the term.  An empty'''
        results = self.persister.lastIndexLETerm(123)

        trueFailureMessage = ('lastIndexLETerm should return True for'
                              ' an empty persister')
        results.addCallback(self.assertTrue, msg=trueFailureMessage)

        for idx, entry in enumerate(firstEntries + appendedEntries):

            def closureCallbacks(idx=idx, entry=entry):
                results.addCallback(
                    dropResult(self.persister.addNewEntries,
                               [entry]))

                results.addCallback(
                    dropResult(self.persister.lastIndexLETerm,
                               entry.term))

                failureMessage = ('lastIndexLETerm returned False for'
                                  ' idx %d and term %d' % (idx, entry.term))
                results.addCallback(self.assertTrue, msg=failureMessage)

            closureCallbacks()

        return results

    def test_overlappingMatchAndAppendNewLogEntries(self):
        '''matchAndAppendNewLogEntries should delete conflicting entries and
        ignore ones that match.

        '''
        results = defer.succeed(None)

        for idx in xrange(len(firstEntries)):
            # drop existing data
            results.addCallback(dropResult(self.persister._truncateLog))

            results.addCallback(
                dropResult(self.persister.addNewEntries, firstEntries))

            # construct overlappingEntries such that:
            # 1) its 0th item matches the currentLog[idx]
            # 2) its subsequent items do not match currentLog[idx+1:]
            #
            # then ensure that it's inserted at lastIndex = idx - 1
            #
            # overlappingLog:      [1][4][5]
            # currentlog:    ...[1][1][2][3]...
            #                       ^  ^
            #                      /    \
            #               lastIndex   idx
            #
            # matchAndAppendNewLogEntries should combine these such
            # that:
            #
            # currentLog:    ...[1][1][4][5]

            lastMatchingIndex = idx - 1
            overlappingEntries = [firstEntries[idx]] + appendedEntries
            expectedEntries = firstEntries[:idx] + overlappingEntries

            results.addCallback(
                dropResult(self.persister.matchAndAppendNewLogEntries,
                           lastMatchingIndex, overlappingEntries))

            results.addCallback(
                dropResult(self.persister._logSlice, 0, -1))

            def assertMatch(logSlice,
                            idx=idx,
                            overlappingEntries=overlappingEntries,
                            expectedEntries=expectedEntries):
                failureMessage = ('logSlice disagrees with overlapping'
                                  ' matchAndAppendNewLogEntries @ %d' % idx)
                self.assertEqual(logSlice,
                                 expectedEntries,
                                 msg=failureMessage)

            results.addCallback(assertMatch)

        return results

    def test_tooLargeIndexMatchAndAppendNewLogEntries(self):
        '''Attempting to match logs after the last index should result in a
        MatchAfterTooHigh failure.

        '''
        results = self.persister.matchAndAppendNewLogEntries(123, firstEntries)
        return self.assertFailure(results, persist.MatchAfterTooHigh)

    def test_addNewEntries(self):
        '''addNewEntries should append the entries to the end of the log
        without any matching.

        '''
        results = self.persister.addNewEntries(firstEntries)

        results.addCallback(dropResult(self.persister._logSlice, 0, -1))

        entriesFailureMessage = 'addNewEntries failed to add all entries'
        results.addCallback(self.assertEqual, firstEntries,
                            msg=entriesFailureMessage)

        results.addCallback(dropResult(self.persister.addNewEntries,
                                       appendedEntries))

        results.addCallback(dropResult(self.persister._logSlice, 0, -1))
        results.addCallback(self.assertEqual,
                            firstEntries + appendedEntries,
                            msg=entriesFailureMessage)
        return results

    def test__logSlice(self):
        '''Sanity test for test crutch _logSlice.  _logSlice should return
        log entries between [start, end).

        '''
        results = self.persister._logSlice(0, 1000)

        falseFailure = '_logSlice on empty persister return non-empty list'
        results.addCallback(self.assertFalse, msg=falseFailure)

        results.addCallback(dropResult(self.persister._logSlice, 0, -1))
        results.addCallback(self.assertFalse, msg=falseFailure)

        results.addCallback(dropResult(self.persister.addNewEntries,
                                       firstEntries))
        for gte, lt in itertools.combinations(range(len(firstEntries)), 2):

            def closureCallbacks(gte=gte, lt=lt):
                results.addCallback(
                    dropResult(self.persister._logSlice, gte, lt))

                failureMesage = '_logSlice(%d, %d) returned incorrect list'

                results.addCallback(self.assertEquals,
                                    firstEntries[gte:lt],
                                    msg=failureMesage)

            closureCallbacks()

        results.addCallback(dropResult(self.persister._logSlice, 0, -1))

        fullEntriesFailureMessage = '_logSlice(0, -1) returned incorrect list'
        results.addCallback(self.assertEquals, firstEntries,
                            msg=fullEntriesFailureMessage)

        return results

    def test__truncateLog(self):
        '''Sanity test for test crutch _truncateLog.  _truncateLog should do
        just that: truncate the log.

        '''
        results = self.persister.addNewEntries(firstEntries)
        results.addCallback(dropResult(self.persister._truncateLog))
        results.addCallback(dropResult(self.persister._logSlice, 0, -1))

        emptyLogFailure = "_truncateLog didn't truncate the log"
        results.addCallback(self.assertFalse, msg=emptyLogFailure)
        return results

    def test_appendEntriesView(self):
        '''appendEntriesView should return a "view" tuple:

        1) the currentTerm of the persister,
        2) the lastLogIndex of the persister,
        3) the prevLogTerm for the log entry at the provided prevLogIndex,
        4) a list of entries after prevLogIndex.

        For an empty persister, these should be their defaults as above.
        '''

        results = self.persister.appendEntriesView(123)
        emptyExpected = persist.AppendEntriesView(currentTerm=0,
                                                  lastLogIndex=-1,
                                                  prevLogTerm=0,
                                                  entries=[])
        emptyFailureMessage = ('appendEntriesView empty persister returned'
                               ' incorrect AppendEntriesView object')

        results.addCallback(self.assertEqual, emptyExpected,
                            msg=emptyFailureMessage)

        results.addCallback(dropResult(self.persister.addNewEntries,
                                       firstEntries))

        for prevLogIndex in xrange(len(firstEntries)):

            def closureCallback(prevLogIndex=prevLogIndex):
                results.addCallback(
                    dropResult(self.persister.appendEntriesView, prevLogIndex))

                def verifyView(view):
                    currentTerm = 0
                    prevLogTerm = firstEntries[prevLogIndex].term
                    entries = firstEntries[prevLogIndex + 1:]
                    lastLogIndex = len(firstEntries) - 1
                    expected = persist.AppendEntriesView(currentTerm,
                                                         lastLogIndex,
                                                         prevLogTerm,
                                                         entries)
                    self.assertEqual(view, expected)

                results.addCallback(verifyView)
            closureCallback()
        return results
