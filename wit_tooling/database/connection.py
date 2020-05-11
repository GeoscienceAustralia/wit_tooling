# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero Public License version 3 as
# published by the Free Software Foundation.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
# See the GNU Affero Public License for more details.
#
# You should have received a copy of the GNU Affero Public License
# along with this program.  If not, see http://www.gnu.org/licenses.

import logging
import platform
import traceback
import os
from subprocess import Popen, PIPE, call
from time import sleep

import psycopg2
from psycopg2 import pool

max_concurrency = None
max_concurrency_raise_exception = False

class ConcurrencyExceededError(Exception):
    """ This exception is raised when g_max_concurrency is exceeded """
    pass



def enableConcurrencyChecks(maxConcurrency, raiseException=True):
    """ Enable the diagnostic feature for debugging unexpected concurrency in
    acquiring ConnectionWrapper instances.

    NOTE: This MUST be done early in your application's execution, BEFORE any
    accesses to ConnectionFactory or connection policies from your application
    (including imports and sub-imports of your app).

    Parameters:
    ----------------------------------------------------------------
    maxConcurrency:   A non-negative integer that represents the maximum expected
                      number of outstanding connections.  When this value is
                      exceeded, useful information will be logged and, depending
                      on the value of the raiseException arg,
                      ConcurrencyExceededError may be raised.
    raiseException:   If true, ConcurrencyExceededError will be raised when
                      maxConcurrency is exceeded.
    """
    global max_concurrency, max_concurrency_raise_exception

    assert maxConcurrency >= 0

    max_concurrency = maxConcurrency
    max_concurrency_raise_exception = raiseException
    return



def disableConcurrencyChecks():
    global max_concurrency, max_concurrency_raise_exception

    max_concurrency = None
    max_concurrency_raise_exception = False
    return



class ConnectionFactory(object):
    """ Database connection factory.

    WARNING: Minimize the scope of connection ownership to cover
    only the execution of SQL statements in order to avoid creating multiple
    outstanding SQL connections in gevent-based apps (e.g.,
    ProductionWorker) when polling code that calls timer.sleep()
    executes in the scope of an outstanding SQL connection, allowing a
    context switch to another greenlet that may also acquire an SQL connection.
    This is highly undesirable because SQL/RDS servers allow a limited number
    of connections. So, release connections before calling into any other code.
    Since connections are pooled by default, the overhead of calling
    ConnectionFactory.get() is insignificant.


    Usage Examples:

    # Add Context Manager (with ...) support for Jython/Python 2.5.x, if needed
    from __future__ import with_statement

    example1 (preferred):
      with ConnectionFactory.get() as conn:
        conn.cursor.execute("SELECT ...")

    example2 (if 'with' statement can't be used for some reason):
      conn = ConnectionFactory.get()
      try:
        conn.cursor.execute("SELECT ...")
      finally:
        conn.release()
    """

    _connectionPolicy = None
    @classmethod
    def get(cls):
        """ Acquire a ConnectionWrapper instance that represents a connection
        to the SQL server per nupic.cluster.database.* configuration settings.

        NOTE: caller is responsible for calling the ConnectionWrapper instance's
        release() method after using the connection in order to release resources.
        Better yet, use the returned ConnectionWrapper instance in a Context Manager
        statement for automatic invocation of release():
        Example:
            # If using Jython 2.5.x, first import with_statement at the very top of
            your script (don't need this import for Jython/Python 2.6.x and later):
            from __future__ import with_statement
            # Then:
            from nupic.database.Connection import ConnectionFactory
            # Then use it like this
            with ConnectionFactory.get() as conn:
              conn.cursor.execute("SELECT ...")
              conn.cursor.fetchall()
              conn.cursor.execute("INSERT ...")

        WARNING: DO NOT close the underlying connection or cursor as it may be
        shared by other modules in your process.  ConnectionWrapper's release()
        method will do the right thing.

        Parameters:
        ----------------------------------------------------------------
        retval:       A ConnectionWrapper instance. NOTE: Caller is responsible
                        for releasing resources as described above.
        """
        if cls._connectionPolicy is None:
            logger = _getLogger(cls)
            logger.info("Creating db connection policy via provider %r",
                        cls._createDefaultPolicy)
            cls._connectionPolicy = cls._createDefaultPolicy()

            logger.debug("Created connection policy: %r", cls._connectionPolicy)

        return cls._connectionPolicy.acquireConnection()


    @classmethod
    def close(cls):
        """ Close ConnectionFactory's connection policy. Typically, there is no need
        to call this method as the system will automatically close the connections
        when the process exits.

        NOTE: This method should be used with CAUTION. It is designed to be
        called ONLY by the code responsible for startup and shutdown of the process
        since it closes the connection(s) used by ALL clients in this process.
        """
        if cls._connectionPolicy is not None:
            cls._connectionPolicy.close()
            cls._connectionPolicy = None

        return

    @classmethod
    def _createDefaultPolicy(cls):
        """ [private] Create the default database connection policy instance

        Parameters:
        ----------------------------------------------------------------
        retval:            The default database connection policy instance
        """
        logger = _getLogger(cls)

        logger.debug(
          "Creating database connection policy: platform=%r; psycopg2.VERSION=%r",
          platform.system(), psycopg2.__version__)

        policy = PooledConnectionPolicy()
        return policy

    # <-- End of class ConnectionFactory



class ConnectionWrapper(object):
    """ An instance of this class is returned by
    acquireConnection() methods of our database connection policy classes.
    """

    _clsNumOutstanding = 0
    """ For tracking the count of outstanding instances """

    _clsOutstandingInstances = set()
    """ tracks outstanding instances of this class while g_max_concurrency is
    enabled
    """

    def __init__(self, dbConn, cursor, releaser, logger):
        """
        Parameters:
        ----------------------------------------------------------------
        dbConn:         the underlying database connection instance
        cursor:         database cursor
        releaser:       a method to call to release the connection and cursor;
                          method signature:
                            None dbConnReleaser(dbConn, cursor)
        """

        global max_concurrency

        try:
            self._logger = logger

            self.dbConn = dbConn
            """ database connection instance """

            self.cursor = cursor
            """ Public cursor instance. Don't close it directly:  Connection.release()
            will do the right thing.
            """

            self._releaser = releaser

            self._addedToInstanceSet = False
            """ True if we added self to _clsOutstandingInstances """

            self._creationTracebackString = None
            """ Instance creation traceback string (if g_max_concurrency is enabled) """


            if max_concurrency is not None:
            # NOTE: must be called *before* _clsNumOutstanding is incremented
                self._trackInstanceAndCheckForConcurrencyViolation()


            logger.debug("Acquired: %r; numOutstanding=%s",
                       self, self._clsNumOutstanding)
        except:
            logger.exception("Exception while instantiating %r;", self)
            # Clean up and re-raise
            if self._addedToInstanceSet:
                self._clsOutstandingInstances.remove(self)
            releaser(dbConn=dbConn, cursor=cursor)
            raise
        else:
            self.__class__._clsNumOutstanding += 1

        return


    def __repr__(self):
        return "%s<dbConn=%r, dbConnImpl=%r, cursor=%r, creationTraceback=%r>" % (
          self.__class__.__name__, self.dbConn,
          getattr(self.dbConn, "_con", "unknown"),
          self.cursor, self._creationTracebackString,)


    def __enter__(self):
        """ [Context Manager protocol method] Permit a ConnectionWrapper instance
        to be used in a context manager expression (with ... as:) to facilitate
        robust release of resources (instead of try:/finally:/release()).  See
        examples in ConnectionFactory docstring.
        """
        return self


    def __exit__(self, exc_type, exc_val, exc_tb):
        """ [Context Manager protocol method] Release resources. """
        self.release()

        # Return False to allow propagation of exception, if any
        return False


    def release(self):
        """ Release the database connection and cursor

        The receiver of the Connection instance MUST call this method in order
        to reclaim resources
        """

        self._logger.debug("Releasing: %r", self)

        # Discard self from set of outstanding instances
        if self._addedToInstanceSet:
            try:
                self._clsOutstandingInstances.remove(self)
            except:
                self._logger.exception(
                "Failed to remove self from _clsOutstandingInstances: %r;", self)
                raise

        self._releaser(dbConn=self.dbConn, cursor=self.cursor)

        self.__class__._clsNumOutstanding -= 1
        assert self._clsNumOutstanding >= 0,  \
               "_clsNumOutstanding=%r" % (self._clsNumOutstanding,)

        self._releaser = None
        self.cursor = None
        self.dbConn = None
        self._creationTracebackString = None
        self._addedToInstanceSet = False
        self._logger = None
        return


    def _trackInstanceAndCheckForConcurrencyViolation(self):
        """ Check for concurrency violation and add self to
        _clsOutstandingInstances.

        ASSUMPTION: Called from constructor BEFORE _clsNumOutstanding is
        incremented
        """
        global max_concurrency, max_concurrency_raise_exception

        assert max_concurrency is not None
        assert self not in self._clsOutstandingInstances, repr(self)

        # Populate diagnostic info
        self._creationTracebackString = traceback.format_stack()

        # Check for concurrency violation
        if self._clsNumOutstanding >= max_concurrency:
          # NOTE: It's possible for _clsNumOutstanding to be greater than
          #  len(_clsOutstandingInstances) if concurrency check was enabled after
          #  unrelease allocations.
            errorMsg = ("With numOutstanding=%r, exceeded concurrency limit=%r "
                      "when requesting %r. OTHER TRACKED UNRELEASED "
                      "INSTANCES (%s): %r") % (
                self._clsNumOutstanding, g_max_concurrency, self,
                len(self._clsOutstandingInstances), self._clsOutstandingInstances,)

            self._logger.error(errorMsg)

            if max_concurrency_raise_exception:
                raise ConcurrencyExceededError(errorMsg)


        # Add self to tracked instance set
        self._clsOutstandingInstances.add(self)
        self._addedToInstanceSet = True

        return

class PooledConnectionPolicy(object):
    """This connection policy maintains a pool of connections that are doled out
    as needed for each transaction.  NOTE: Appropriate for multi-threaded
    applications. NOTE: The connections are NOT shared concurrently between
    threads.
    """


    def __init__(self):
        """ Consruct an instance. The instance's open() method must be
        called to make it ready for acquireConnection() calls.
        """
        self._logger = _getLogger(self.__class__)

        self._pool = pool.ThreadedConnectionPool(1, 1, **get_database_args())
        self._logger.info("Created %s", self.__class__.__name__)
        return


    def close(self):
        """ Close the policy instance and its database connection pool. """
        self._logger.info("Closing")

        if self._pool is not None:
            self._pool.closeall()
            self._pool = None
        else:
            self._logger.warning(
                "close() called, but connection policy was alredy closed")
        return


    def acquireConnection(self):
        """ Get a connection from the pool.

        Parameters:
        ----------------------------------------------------------------
        retval:       A ConnectionWrapper instance. NOTE: Caller
                        is responsible for calling the  ConnectionWrapper
                        instance's release() method or use it in a context manager
                        expression (with ... as:) to release resources.
        """
        self._logger.debug("Acquiring connection")

        while True:
            try:
                dbConn = self._pool.getconn(key=1)
                cursor = dbConn.cursor()
                # to avoid connection idle too long and then closed by the server
                # not sure if it is a good idea
                cursor.execute("SELECT 1")
            except psycopg2.Error as e:
                self._logger.warning("connection error %s" % (e))
                self._pool = pool.ThreadedConnectionPool(1, 1, **get_database_args())
            else:
                break

        connWrap = ConnectionWrapper(dbConn=dbConn,
                                     cursor=cursor,
                                     releaser=self._releaseConnection,
                                     logger=self._logger)
        return connWrap


    def _releaseConnection(self, dbConn, cursor):
        """ Release database connection and cursor; passed as a callback to
        ConnectionWrapper
        """
        self._logger.debug("Releasing connection")

        try:
            dbConn.commit()
        except:
            pass

        # Close the cursor
        cursor.close()

        # ... then return db connection back to the pool
        #dbConn.close()
        self._pool.putconn(dbConn, key=1)
        return

def _getLogger(cls, logLevel=None):
    """ Gets a logger for the given class in this module
    """
    logger = logging.getLogger(
    ".".join(['database', cls.__name__]))

    if logLevel is not None:
        logger.setLevel(logLevel)

    return logger

def get_database_args():
    """ Returns a dictionary of arguments for DBUtils.SteadyDB.SteadyDBConnection
    constructor.
    """

    from .io import DIO
    dbname = DIO.get_db_name()
    host  = os.getenv('WIT_DB_HOSTNAME')
    user = os.getenv('DB_USERNAME')
    passwd = os.getenv('DB_PASSWORD')
    if host is None:
        host = '150.203.254.14'
    if user is None:
        user = os.getenv('LOGNAME')
    port = 5432

    #command = "SELECT 1 FROM pg_database WHERE datname= '%s'" % (dbname)
    #args = ['psql','-At', '-U', user,  '-d', 'postgres', '-h', host, '-p', str(port), '-c', command]
    #proc = Popen(args, stdout=PIPE, stderr=PIPE)
    #out, err = proc.communicate()
    #if out == b'1\n':
    #    print("database exists")
    #else:
    #    print("create database")
    #    command = 'CREATE DATABASE %s' % (dbname)
    #    call(['psql','-U', user,  '-d', 'postgres', '-h', host, '-p', str(port), '-c', command])

    return dict(
          dbname = dbname,
          host = host,
          port = port,
          user = user,
          password=passwd
      )
