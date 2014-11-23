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

    def test_getNewCurrentTerm(self):
        '''The currentTerm of a fresh persister should be 0'''

        results = self.persister.getCurrentTerm()

        failureMessage = 'getCurrentTerm for new persister should return 0'
        results.addCallback(self.assertEqual, 0, msg=failureMessage)

        return results

    def test_setCurrentTerm(self):
        '''setcurrentTerm should durably set the current term.

        '''
        results = self.persister.setCurrentTerm(123)

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
        it

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

    def test_newVotedFor(self):
        '''votedFor should return None from a fresh persister.'''
        results = self.persister.votedFor()

        failureMessage = 'votedFor did not return None from a fresh persister'
        results.addCallback(self.assertIs, None, msg=failureMessage)

        return results

    def test_voteFor(self):
        '''voteFor should durably set the identity of the candidate voted
        for.

        '''
        results = defer.succeed(None)

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

    def test_disjointMatchAndAppendNewLogEntries(self):
        '''matchAndAppendNewLogEntries should append disjoint logs'''
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

    def test_new_committableLogEntries(self):
        d = self.persister.committableLogEntries(0, 100)
        d.addCallback(self.assertEqual, second=[])
        return d

    def test_getLastTerm(self):
        d = self.persister.getLastTerm()
        d.addCallback(self.assertEqual, 0)
        d.addCallback(
            lambda ignore:
            self.persister.matchAndAppendNewLogEntries(-1,
                                                       appendedEntries))

        d.addCallback(lambda ignore:
                      self.persister.getLastTerm())
        d.addCallback(self.assertEqual, 3)
        return d

    def test_addNewEntries(self):
        d = self.persister.addNewEntries(firstEntries)
        d.addCallback(lambda ignore:
                      self.persister._logSlice(0, -1))
        d.addCallback(self.assertEqual, firstEntries)

        d.addCallback(lambda ignore:
                      self.persister.addNewEntries(appendedEntries))
        d.addCallback(lambda ignore:
                      self.persister._logSlice(0, -1))
        d.addCallback(self.assertEqual, firstEntries + appendedEntries)

    def test_logSlice(self):
        d = self.persister._logSlice(0, 1000)
        d.addCallback(self.assertFalse)
        d.addCallback(lambda ignore:
                      self.persister._logSlice(0, -1))
        d.addCallback(self.assertFalse)

        d.addCallback(lambda ignore:
                      self.persister.addNewEntries(firstEntries))

        for gte, lt in itertools.combinations(range(len(firstEntries)), 2):
            d.addCallback(
                lambda ignore, lower=gte, upper=lt:
                self.persister._logSlice(lower, upper))
            d.addCallback(
                lambda result, lower=gte, upper=lt:
                self.assertEquals(result, firstEntries[lower:upper],
                                  msg='lower %d, upper %d' % (lower, upper)))

        d.addCallback(lambda ignore:
                      self.persister._logSlice(0, -1))
        d.addCallback(self.assertEquals, firstEntries)

        return d

    def test_indexMatchesTerm(self):
        d = self.persister.indexMatchesTerm(10, 123)
        d.addCallback(self.assertTrue)

        d.addCallback(lambda ignore:
                      self.persister.addNewEntries(firstEntries))

        for idx, entry in enumerate(firstEntries):
            d.addCallback(
                lambda ignore, idx=idx, entry=entry:
                self.persister.indexMatchesTerm(idx, entry.term))
            d.addCallback(self.assertTrue)

        return d

    def test_indexLETerm(self):
        d = self.persister.lastIndexLETerm(123)
        d.addCallback(self.assertTrue)

        for entry in firstEntries + appendedEntries:
            d.addCallback(lambda ignore, entry=entry:
                          self.persister.addNewEntries([entry]))
            d.addCallback(lambda ignore, entry=entry:
                          self.persister.lastIndexLETerm(entry.term))
            d.addCallback(self.assertTrue)

        return d

    def test_appendEntriesView(self):
        d = self.persister.appendEntriesView(123)
        emptyExpected = persist.AppendEntriesView(currentTerm=0,
                                                  lastLogIndex=-1,
                                                  prevLogTerm=None,
                                                  entries=[])
        d.addCallback(self.assertEqual, emptyExpected)

        d.addCallback(lambda ignore:
                      self.persister.addNewEntries(firstEntries))

        for prevLogIndex in xrange(len(firstEntries)):
            d.addCallback(lambda ignore, prevLogIndex=prevLogIndex:
                          self.persister.appendEntriesView(prevLogIndex))

            def verifyView(view, prevLogIndex=prevLogIndex):
                currentTerm = 0
                prevLogTerm = firstEntries[prevLogIndex]
                entries = firstEntries[prevLogIndex + 1:]
                lastLogIndex = len(firstEntries) + 1
                expected = persist.AppendEntriesView(currentTerm,
                                                     lastLogIndex,
                                                     prevLogIndex,
                                                     prevLogTerm,
                                                     entries)
                self.assertEqual(view, expected)

                d.addCallback(verifyView)

        return d
