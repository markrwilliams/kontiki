from kon_tiki import raft
from twisted.trial import unittest


class ListPersistInitTestCase(unittest.TestCase):
    '''Basic sanity tests for ListPersist.  When more than one persister
    exists, most of these tests should be broken out into a base class
    or mixin...
    '''

    def test_init(self):
        '''A new ListPersist should have its state variables initialized as
        though it is a Server's first boot.
        '''
        persister = raft.ListPersist()
        self.assertEqual(persister.currentTerm, 0)
        self.assertEqual(persister.votedFor, None)
        self.assertFalse(persister.log)


class ListPersistUpdateEntriesTestCase(unittest.TestCase):

    def test_updateEntries_withEmptyLog(self):
        '''A ListPersist should append entries when its log is entry
        '''
        persister = raft.ListPersist()
        newEntries = [raft.LogEntry(term=0, command='command1'),
                      raft.LogEntry(term=1, command='command2')]
        numberOfEntries = len(newEntries)

        result = persister.updateEntries(prevIndex=-1,
                                         prevTerm=None,
                                         entries=newEntries)
        self.assertTrue(result)
        self.assertEqual(persister.lastIndex, numberOfEntries - 1)
        return persister

    def test_updateEntries_withConflictPreviousTerm(self):
        '''A ListPersist should fail when the term of prevIndex does not match prevTerm
        '''
        persister = self.test_updateEntries_withEmptyLog()
        ignoredEntries = [raft.LogEntry(term=99, command="Doesn't matter")]

        previousLog = persister.log[:]
        prevIndex = persister.lastIndex
        conflictingTerm = persister.log[prevIndex].term + 1

        result = persister.updateEntries(prevIndex=prevIndex,
                                         prevTerm=conflictingTerm,
                                         entries=ignoredEntries)

        self.assertFalse(result)
        self.assertEqual(previousLog, persister.log)

    def test_updateEntries_appendsToExisting(self):
        '''A ListPersist should append entries to the end of its log with a
        matching prevTerm'''

        persister = self.test_updateEntries_withEmptyLog()
        numberOfEntries = len(persister.log)

        previousLog = persister.log[:]
        prevIndex = persister.lastIndex
        prevTerm = persister.log[prevIndex].term

        appendedEntries = [raft.LogEntry(term=1, command='command3'),
                           raft.LogEntry(term=2, command='command4')]
        numberOfEntries += len(appendedEntries)

        result = persister.updateEntries(prevIndex=prevIndex,
                                         prevTerm=prevTerm,
                                         entries=appendedEntries)

        self.assertTrue(result)
        self.assertEquals(persister.log, previousLog + appendedEntries)
        self.assertEquals(persister.lastIndex, numberOfEntries - 1)

    def test_updateEntries_overwritesConflicting(self):
        '''A ListPersist should overwrite entries in its log that conflict
        with new entries
        '''
        persister = self.test_updateEntries_withEmptyLog()
        numberOfEntries = len(persister.log)

        untouchedEntries = persister.log[:-1]
        conflictingTerm = persister.log[-1].term + 1
        conflictingEntries = [raft.LogEntry(term=conflictingTerm,
                                            command='conflicting1'),
                              raft.LogEntry(term=conflictingTerm + 1,
                                            command='conflicting2')]

        numberOfEntries = numberOfEntries + len(conflictingEntries) - 1

        prevIndex = persister.lastIndex - 1
        prevTerm = persister.log[prevIndex].term

        result = persister.updateEntries(prevIndex=prevIndex,
                                         prevTerm=prevTerm,
                                         entries=conflictingEntries)

        self.assertTrue(result)
        self.assertEquals(persister.log, untouchedEntries + conflictingEntries)
        self.assertEquals(persister.lastIndex, numberOfEntries - 1)
