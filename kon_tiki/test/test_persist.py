from kon_tiki import persist
from twisted.trial import unittest


firstEntries = [persist.LogEntry(term=0, command='first1'),
                    persist.LogEntry(term=0, command='first2'),
                    persist.LogEntry(term=1, command='first3')]
appendedEntries = [persist.LogEntry(term=2, command='append1'),
                   persist.LogEntry(term=3, command='append2')]



class ListPersistTestCase(unittest.TestCase):
    '''Basic sanity tests for ListPersist.  When more than one persister
    exists, most of these tests should be broken out into a base class
    or mixin...
    '''

    def test_init(self):
        '''A new ListPersist should have its state variables initialized as
        though it is a Server's first boot.
        '''
        persister = persist.ListPersist()
        self.assertEqual(persister.currentTerm, 0)
        self.assertEqual(persister.votedFor, None)
        self.assertFalse(persister.log)

    def emptyListPersist(self):
        persister = persist.ListPersist()
        self.assertFalse(persister.log)
        self.assertEqual(persister.lastIndex, -1)
        return persister

    def test_appendNewEntries(self):
        '''A ListPersist should append new entries to its log.
        '''
        persister = self.emptyListPersist()
        persister.appendNewEntries(firstEntries)

        self.assertEqual(persister.log, firstEntries)
        self.assertEqual(persister.lastIndex, len(firstEntries) - 1)

        persister.appendNewEntries(appendedEntries)

        self.assertEqual(persister.log,
                         firstEntries + appendedEntries)
        self.assertEqual(persister.lastIndex,
                         len(firstEntries)
                         + len(appendedEntries)
                         - 1)

    def test_indexMatchesTerm(self):
        '''A ListPersist should determine when a log entry a given index
        matches a given term'''
        persister = self.emptyListPersist()

        firstTerm = firstEntries[0].term
        self.assertTrue(persister.indexMatchesTerm(index=-1, term=firstTerm))

        persister.appendNewEntries(firstEntries)

        badTerm = firstTerm + 1
        self.assertFalse(persister.indexMatchesTerm(index=-1, term=badTerm))
        self.assertFalse(persister.indexMatchesTerm(index=0, term=badTerm))

        lastTerm = firstEntries[-1].term
        lastIndex = len(firstEntries) - 1
        self.assertTrue(persister.indexMatchesTerm(index=lastIndex,
                                                   term=lastTerm))

    def _test_matchLogToEntries(self, currentEntries,
                                matchAfter,
                                newEntries,
                                expectedEntries):
        persister = self.emptyListPersist()
        persister.appendNewEntries(currentEntries)
        matchedEntries = persister.matchLogToEntries(matchAfter=matchAfter,
                                                     entries=newEntries)
        self.assertEqual(matchedEntries, expectedEntries)
        return persister

    def test_matchLogToEntries_withEmptyLog(self):
        '''A ListPersist instance with an empty log should allow all incoming
        entries unless matchAfter indicates the entries begin after the
        instance's own log.'''
        entries = firstEntries
        matchAfter = -1
        persister = self._test_matchLogToEntries(currentEntries=[],
                                                 matchAfter=matchAfter,
                                                 newEntries=entries,
                                                 expectedEntries=entries)

        self.assertRaises(persist.MatchAfterTooHigh,
                          persister.matchLogToEntries,
                          matchAfter=matchAfter + 1,
                          entries=firstEntries)

    def test_matchLogEntries_allowsContiguousEntries(self):
        '''A ListPersist should allow all incoming entries when they are
        contiguous with its log.'''
        currentEntries = firstEntries
        newEntries = appendedEntries
        matchAfter = len(firstEntries) - 1

        persister = self._test_matchLogToEntries(currentEntries=currentEntries,
                                                 matchAfter=matchAfter,
                                                 newEntries=newEntries,
                                                 expectedEntries=newEntries)

        self.assertRaises(persist.MatchAfterTooHigh,
                          persister.matchLogToEntries,
                          matchAfter=matchAfter + 1,
                          entries=newEntries)

    def test_matchLogEntries_deletesConflictingEntries(self):
        '''A ListPersist should delete entries from its log that conflict with
        incoming entries.'''
        # all ranges for which this could conflict
        for matchAfter in range(0, len(firstEntries)):
            current = firstEntries
            appended = appendedEntries
            persister = self._test_matchLogToEntries(currentEntries=current,
                                                     matchAfter=matchAfter,
                                                     newEntries=appended,
                                                     expectedEntries=appended)
            cutOff = matchAfter + 1
            self.assertEqual(persister.log, current[:cutOff])

    def test_matchLogEntries_skipsMatchingEntries(self):
        '''A ListPersist should skip all overlapping entries'''
        for matchAfter in range(-1, len(firstEntries)):
            current = firstEntries
            appended = firstEntries[matchAfter + 1:]
            persister = self._test_matchLogToEntries(currentEntries=current,
                                                     matchAfter=matchAfter,
                                                     newEntries=appended,
                                                     expectedEntries=[])
            self.assertEqual(persister.log, current)

    def test_matchLogEntries_overwritesConflictingEntries(self):
        '''A ListPersist should delete conflicting entries and skip over
        matching ones.'''
        # This is a basic sanity test to ensure that
        # deletesConflictingEntries and skipsMatchingEntries compose
        # correctly
        current = firstEntries
        partiallyConflicts = firstEntries[:2] + appendedEntries
        expected = appendedEntries
        matchAfter = -1
        persister = self._test_matchLogToEntries(currentEntries=current,
                                                 matchAfter=matchAfter,
                                                 newEntries=partiallyConflicts,
                                                 expectedEntries=expected)
        self.assertEquals(persister.log, firstEntries[:2])


class SQLitePersistTestCase(unittest.TestCase):

    def setUp(self):
        self.persister = persist.SQLitePersist(':memory:')
        self.persister.connect()

    def test_currentTerm(self):
        d = self.persister.getCurrentTerm()
        d.addCallback(self.assertEqual, second=0)
        d.addCallback(lambda ignore: self.persister.incrementTerm())
        d.addCallback(self.assertEqual, second=1)
        d.addCallback(lambda ignore: self.persister.getCurrentTerm())
        d.addCallback(self.assertEqual, second=1)
        return d

    def test_votedFor(self):
        d = self.persister.votedFor()
        d.addCallback(self.assertIs, second=None)
        d.addCallback(lambda ignore: self.persister.voteFor("me"))
        d.addCallback(lambda ignore: self.persister.votedFor())
        d.addCallback(self.assertEqual, second='me')
        return d

    def test_appendNewEntries(self):
        d = self.persister.appendNewEntries(firstEntries)
        d.addCallback(lambda ignore: self.persister.getLastIndex())
        d.addCallback(self.assertEqual, second=len(firstEntries))
        return d

    def test_new_logSlice(self):
        d = self.persister.logSlice(0, 100)
        d.addCallback(self.assertEqual, second=[])
        return d
