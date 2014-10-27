from kon_tiki import raft
from twisted.trial import unittest


class ListPersistTestCase(unittest.TestCase):
    '''Basic sanity tests for ListPersist.  When more than one persister
    exists, most of these tests should be broken out into a base class
    or mixin...
    '''

    firstEntries = [raft.LogEntry(term=0, command='first1'),
                    raft.LogEntry(term=0, command='first2'),
                    raft.LogEntry(term=1, command='first3')]
    appendedEntries = [raft.LogEntry(term=2, command='append1'),
                       raft.LogEntry(term=3, command='append2')]

    def test_init(self):
        '''A new ListPersist should have its state variables initialized as
        though it is a Server's first boot.
        '''
        persister = raft.ListPersist()
        self.assertEqual(persister.currentTerm, 0)
        self.assertEqual(persister.votedFor, None)
        self.assertFalse(persister.log)

    def emptyListPersist(self):
        persister = raft.ListPersist()
        self.assertFalse(persister.log)
        self.assertEqual(persister.lastIndex, -1)
        return persister

    def test_appendNewEntries(self):
        '''A ListPersist should append new entries to its log.
        '''
        persister = self.emptyListPersist()
        persister.appendNewEntries(self.firstEntries)

        self.assertEqual(persister.log, self.firstEntries)
        self.assertEqual(persister.lastIndex, len(self.firstEntries) - 1)

        persister.appendNewEntries(self.appendedEntries)

        self.assertEqual(persister.log,
                         self.firstEntries + self.appendedEntries)
        self.assertEqual(persister.lastIndex,
                         len(self.firstEntries)
                         + len(self.appendedEntries)
                         - 1)

    def test_indexMatchesTerm(self):
        '''A ListPersist should determine when a log entry a given index
        matches a given term'''
        persister = self.emptyListPersist()

        firstTerm = self.firstEntries[0].term
        self.assertTrue(persister.indexMatchesTerm(index=-1, term=firstTerm))

        persister.appendNewEntries(self.firstEntries)

        badTerm = firstTerm + 1
        self.assertFalse(persister.indexMatchesTerm(index=-1, term=badTerm))
        self.assertFalse(persister.indexMatchesTerm(index=0, term=badTerm))

        lastTerm = self.firstEntries[-1].term
        lastIndex = len(self.firstEntries) - 1
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
        entries = self.firstEntries
        matchAfter = -1
        persister = self._test_matchLogToEntries(currentEntries=[],
                                                 matchAfter=matchAfter,
                                                 newEntries=entries,
                                                 expectedEntries=entries)

        self.assertRaises(raft.MatchAfterTooHigh,
                          persister.matchLogToEntries,
                          matchAfter=matchAfter + 1,
                          entries=self.firstEntries)

    def test_matchLogEntries_allowsContiguousEntries(self):
        '''A ListPersist should allow all incoming entries when they are
        contiguous with its log.'''
        currentEntries = self.firstEntries
        newEntries = self.appendedEntries
        matchAfter = len(self.firstEntries) - 1

        persister = self._test_matchLogToEntries(currentEntries=currentEntries,
                                                 matchAfter=matchAfter,
                                                 newEntries=newEntries,
                                                 expectedEntries=newEntries)

        self.assertRaises(raft.MatchAfterTooHigh,
                          persister.matchLogToEntries,
                          matchAfter=matchAfter + 1,
                          entries=newEntries)

    def test_matchLogEntries_deletesConflictingEntries(self):
        '''A ListPersist should delete entries from its log that conflict with
        incoming entries.'''
        # all ranges for which this could conflict
        for matchAfter in range(0, len(self.firstEntries)):
            current = self.firstEntries
            appended = self.appendedEntries
            persister = self._test_matchLogToEntries(currentEntries=current,
                                                     matchAfter=matchAfter,
                                                     newEntries=appended,
                                                     expectedEntries=appended)
            cutOff = matchAfter + 1
            self.assertEqual(persister.log, current[:cutOff])

    def test_matchLogEntries_skipsMatchingEntries(self):
        '''A ListPersist should skip all overlapping entries'''
        for matchAfter in range(-1, len(self.firstEntries)):
            current = self.firstEntries
            appended = self.firstEntries[matchAfter + 1:]
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
        current = self.firstEntries
        partiallyConflicts = self.firstEntries[:2] + self.appendedEntries
        expected = self.appendedEntries
        matchAfter = -1
        persister = self._test_matchLogToEntries(currentEntries=current,
                                                 matchAfter=matchAfter,
                                                 newEntries=partiallyConflicts,
                                                 expectedEntries=expected)
        self.assertEquals(persister.log, self.firstEntries[:2])
