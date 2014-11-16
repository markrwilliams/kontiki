import operator
from collections import namedtuple
from twisted.internet.defer import Deferred, succeed
from twisted.enterprise import adbapi
import sqlite3


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

    def lastIndexLETerm(self, term):
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


def rowToLogEntry(row):
    return LogEntry(term=row['term'], command=row['command'])


class SQLitePersist(object):
    INITIALDB = '''
                    CREATE TABLE IF NOT EXISTS raft_log
                                  (logIndex INTEGER PRIMARY KEY,
                                   term INTEGER,
                                   command TEXT);
                     CREATE TABLE IF NOT EXISTS raft_log_match
                                  (logIndex INTEGER PRIMARY KEY,
                                   term INTEGER,
                                   command TEXT);
                     CREATE INDEX IF NOT EXISTS raft_log_term_idx
                                  ON raft_log(term);

                     CREATE TABLE IF NOT EXISTS raft_variables
                                  (currentTerm INTEGER,
                                   votedFor    TEXT);

                     INSERT INTO raft_variables
                                 (currentTerm, votedFor)
                                 SELECT 0, null
                                 WHERE NOT EXISTS (SELECT rowid
                                                   FROM raft_variables);'''

    _MISSING = object()
    dbPool = None
    _currentTerm = _MISSING
    _votedFor = _MISSING

    def __init__(self, path, poolMin=None, poolMax=None):
        self.path = path
        if self.path == ':memory:':
            # we need to make sure there's only one connection in the
            # pool.  otherwise a new connection may be created,
            # pointing at a fresh :memory: database without any of the
            # data we expect!
            self.poolMax = self.poolMin = 1
        else:
            self.poolMin = poolMin
            self.poolMax = poolMax

    def prepareDatabaseAndConnection(self, connection):
        connection.row_factory = sqlite3.Row
        with connection:
            connection.executescript(self.INITIALDB)

    def connect(self):
        cf_openfun = self.prepareDatabaseAndConnection

        kwargs = {}
        if self.poolMin is not None:
            kwargs['cp_min'] = self.poolMin
        if self.poolMax is not None:
            kwargs['cp_max'] = self.poolMin

        self.dbPool = adbapi.ConnectionPool('sqlite3', self.path,
                                            check_same_thread=False,
                                            cp_openfun=cf_openfun,
                                            **kwargs)

    def _runQuery(self, query, params, callbacks):
        d = self.dbPool.runQuery(query, params)
        for cb in callbacks:
            d.addCallback(cb)
        return d

    def _runInteraction(self, func, callbacks):
        d = self.dbPool.runInteraction(func)
        for cb in callbacks:
            d.addCallback(cb)
        return d

    def getLastIndex(self):
        query = '''SELECT MAX(logIndex) AS lastIndex
                   FROM raft_log'''
        params = {}

        def extractIndex(result):
            lastIndex = result[0]['lastIndex']
            if lastIndex is None:
                lastIndex = -1
            return lastIndex

        return self._runQuery(query, params, [extractIndex])

    def committableLogEntries(self, lastApplied):
        query = '''SELECT term, command
                   FROM raft_log
                   WHERE logIndex >= :lastApplied
                   AND logIndex < (SELECT commitIndex
                                   FROM raft_variables
                                   WHERE rowid = 1)'''
        params = {'lastApplied': lastApplied}

        def convertRows(result):
            return [rowToLogEntry(row) for row in result]

        return self._runQuery(query, params, [convertRows])

    def indexMatchesTerm(self, index, term):
        query = '''SELECT term = :term AS termMatches
                   FROM raft_log
                   WHERE logIndex = :index'''
        params = {'index': index, 'term': term}

        def boolify(result):
            return bool(result[0]['termMatches'])

        return self._runQuery(query, params, [boolify])

    def lastIndexLETerm(self, term):
        query = '''SELECT term <= :term AS termOK
               FROM raft_log
               WHERE logIndex = (SELECT MAX(logIndex)
                                 FROM raft_log)'''
        params = {'term': term}

        def boolify(result):
            return bool(result[0]['termOK'])

        return self._runQuery(query, params, [boolify])

    def getCurrentTerm(self):
        if self._currentTerm is not self._MISSING:
            return succeed(self._currentTerm)
        else:
            query = '''SELECT currentTerm
                       FROM raft_variables
                       WHERE rowid = 1'''
            params = {}

            def extractCurrentTerm(result):
                return result[0]['currentTerm']

            def _setCurrentTerm(currentTerm):
                self._currentTerm = currentTerm
                return currentTerm

            return self._runQuery(query, params, [extractCurrentTerm,
                                                  _setCurrentTerm])

    def setCurrentTerm(self, term, increment=False):
        if term is None and increment:
            def updateReturning(txn):
                update = '''UPDATE raft_variables
                            SET currentTerm = currentTerm + 1'''
                txn.execute(update)

                select = '''SELECT currentTerm
                            FROM raft_variables'''
                return txn.execute(select).fetchone()['currentTerm']
        else:
            def updateReturning(txn):
                update = '''UPDATE raft_variables
                            SET currenTerm = :newTerm'''
                updateParams = {'newTerm': term}
                txn.execute(update, updateParams)
                return term

        def _setCurrentTerm(currentTerm):
            self._currentTerm = currentTerm
            return currentTerm

        return self._runInteraction(updateReturning, [_setCurrentTerm])

    def votedFor(self):
        if self._votedFor is not self._MISSING:
            return succeed(self._votedFor)
        else:
            query = '''SELECT votedFor
                       FROM raft_variables
                       WHERE rowid = 1'''
            params = {}

            def extractVotedFor(result):
                return result[0]['votedFor']

            return self._runQuery(query, params, [extractVotedFor])

    def voteFor(self, identity):
        def updateReturning(txn):
            update = '''UPDATE raft_variables
                        SET votedFor = :identity'''
            updateParams = {'identity': identity}
            txn.execute(update, updateParams)

            select = '''SELECT votedFor
                        FROM raft_variables'''
            return txn.execute(select).fetchone()['votedFor']

        def setVotedFor(votedFor):
            self._votedFor = votedFor
            if self._votedFor is None:
                raise ValueError("HERE")
            return votedFor

        d = self.dbPool.runInteraction(updateReturning)
        d.addCallback(setVotedFor)
        return d

    def matchAndAppendNewLogEntries(self, matchAfter, entries):

        def match(txn):
            determineLastIndex = '''SELECT COALESCE(MAX(logIndex), 0)
                                    FROM raft_log'''

            result = txn.execute(determineLastIndex)
            lastIndex = result.fetchone()[0]

            if matchAfter > lastIndex:
                raise MatchAfterTooHigh('matchAfter = %d, '
                                        'lastIndex = %d' % (matchAfter,
                                                            lastIndex))
            truncateMatchTable = "DELETE FROM raft_log_match"
            txn.execute(truncateMatchTable)

            loadMatchTable = '''INSERT INTO raft_log_match
                                (logIndex, term, command)
                                VALUES (:logIndex, :term, :command)'''

            loadMatchTableParams = [{'logIndex': i,
                                     'term': entry.term,
                                     'command': entry.command}
                                    for i, entry
                                    in enumerate(entries, matchAfter + 1)]

            txn.executemany(loadMatchTable, loadMatchTableParams)

            matchEntries = '''SELECT COALESCE(MAX(raft_log.logIndex),
                                                  :matchAfter)
                                     AS myIndex
                              FROM raft_log, raft_log_match
                              WHERE raft_log.logIndex = raft_log_match.logIndex
                              AND   raft_log.term = raft_log_match.term'''
            matchEntriesParams = {'matchAfter': matchAfter}

            txn.execute(matchEntries, matchEntriesParams)
            myIndex = txn.fetchone()['myIndex']

            deleteMismatch = '''DELETE FROM raft_log
                                WHERE logIndex > :myIndex'''
            deleteMismatchParams = {'myIndex': myIndex}

            txn.execute(deleteMismatch, deleteMismatchParams)

            insertNew = '''INSERT INTO raft_log
                           (logIndex, term, command)
                           SELECT logIndex, term, command
                                  FROM raft_log_match
                                  WHERE logIndex > :myIndex'''
            insertNewParams = {'myIndex': myIndex}

            txn.execute(insertNew, insertNewParams)

        return self._runInteraction(match, [])
