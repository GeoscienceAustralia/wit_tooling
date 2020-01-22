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

import collections
import logging
import sys
import traceback
import uuid
import os
from subprocess import call
import psycopg2
from time import sleep

from .connection import ConnectionFactory
from .special_sql import * 

_LOGGER = logging.getLogger(__name__)


class InvalidConnectionException(Exception):
    """ This exception is raised when a worker tries to update a model record that
    belongs to another worker. Ownership of a model is determined by the database
    connection id
    """
    pass

def _abbreviate(text, threshold):
    """ Abbreviate the given text to threshold chars and append an ellipsis if its
    length exceeds threshold; used for logging;

    NOTE: the resulting text could be longer than threshold due to the ellipsis
    """
    if text is not None and len(text) > threshold:
        text = text[:threshold] + "..."

    return text



class DIO(object):
    """ 
    There are two tables in this database, the data table and the polygon table.

    data table. 
    field     description
    ---------------------------------------------------------------------------
    item_id: Generated by the database when a new item is entered 
    poly_id: Generated id of polygon when a new polygon is entered
    datetime: Datetime of the data 
    fc_bs: BS from fractional cover
    fc_pv: PV from fractional cover
    fc_npv: NPV from fractional cover
    tci_w: Wetness from tci
    wofs_water: Water from wofs

    polygon table
    field     description
    ----------------------------------------------------------------------------
    poly_id Generated by the database when a new data is entered
    poly_hash: Hashed geojson of polygon bounding box
    geometry: GIS of polygon
    crs: CRS string
    shapefile: Path of shapefile where the polygon is from
    feature_id: Feature id of the polygon in the shapefile
    """

    HASH_MAX_LEN = 16
    """ max size, in bytes, of the hash used for polygon """

    class TableInfoBase(object):
        """ Common table info fields; base class """
        def __init__(self):
            self.tableName = None
            """ Database-qualified table name (databasename.tablename) """

            self.dbFieldNames = None
            """ Names of fields in schema """

    class DataTableInfo(TableInfoBase):
        def __init__(self):
            super().__init__()


    class PolyTableInfo(TableInfoBase):
        def __init__(self):
            super().__init__()

    class AlltimeInfo(TableInfoBase):
        def __init__(self):
            super().__init__()

    class FirstObsInfo(TableInfoBase):
        def __init__(self):
            super().__init__()

    class YearInfo(TableInfoBase):
        def __init__(self):
            super().__init__()

    class EventTimeInfo(TableInfoBase):
        def __init__(self):
            super().__init__()

    class EventInfo(TableInfoBase):
        def __init__(self):
            super().__init__()

    class IncompleteEventInfo(TableInfoBase):
        def __init__(self):
            super().__init__()

    class CatchmentInfo(TableInfoBase):
        def __init__(self):
            super().__init__()

    _SEQUENCE_TYPES = (list, set, tuple)
    """ Sequence types that we accept in args """
    _instance = None


    # The root name and version of the database. The actual database name is
    #  something of the form "client_jobs_v2_suffix".
    _DB_ROOT_NAME = 'wit_test'

    @classmethod
    def get_db_name(cls):
        return cls._DB_ROOT_NAME

    @staticmethod
    def get():
        """ Get the instance of the DIO created for this process (or
        perhaps at some point in the future, for this thread).

        Parameters:
        ----------------------------------------------------------------
        retval:  instance of ClientJobsDAO
        """

        # Instantiate if needed
        if DIO._instance is None:
            dio = DIO()
            dio.connect()

            DIO._instance = dio

        # Return the instance to the caller
        return DIO._instance


    def __init__(self):
        """ Instantiate a DIO instance.

        Parameters:
        ----------------------------------------------------------------
        """
        self._logger = _LOGGER

        # Usage error to instantiate more than 1 instance per process
        assert (DIO._instance is None)

        # Create the name of the current version database
        self.dbName = self.get_db_name()

        # NOTE: we set the table names here; the rest of the table info is set when
        #  the tables are initialized during connect()
        self.data = self.DataTableInfo()
        self.data.tableName = 'data'

        self.polygons = self.PolyTableInfo()
        self.polygons.tableName = 'polygons'

        self.alltime_metrics = self.AlltimeInfo()
        self.alltime_metrics.tableName = 'alltime_count'

        self.first_observe = self.FirstObsInfo()
        self.first_observe.tableName = 'first_observe'

        self.year_metrics = self.YearInfo()
        self.year_metrics.tableName = 'year_metrics'

        self.event_metrics_time = self.EventTimeInfo()
        self.event_metrics_time.tableName = 'event_metrics_time'

        self.event_metrics = self.EventInfo()
        self.event_metrics.tableName = 'event_metrics'

        self.incomplete_event = self.IncompleteEventInfo()
        self.incomplete_event.tableName = 'incomplete_event'

        self.catchment = self.CatchmentInfo()
        self.catchment.tableName = 'catchments'

    @property
    def data_tablename(self):
        return self.data.tableName

    @property
    def poly_tablename(self):
        return self.polygons.tableName

    def connect(self):
        """
        """
        # Initialize tables, if needed
        with ConnectionFactory.get() as conn:
            # Initialize tables
            self.init_tables(conn=conn)

        return


    def init_tables(self, conn):
        """ Initialize tables, if needed

        Parameters:
        ----------------------------------------------------------------
        cursor:              SQL cursor
        """
        
        # Get the list of tables
        conn.cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
        output = conn.cursor.fetchall()
        table_names = [x[0] for x in output]

        conn.cursor.execute("SELECT table_name FROM information_schema.views WHERE table_schema='public'")
        output = conn.cursor.fetchall()
        view_names = [x[0] for x in output]

        # only one process can create table
        lock = False
        if self.data_tablename not in table_names or self.poly_tablename not in table_names:
            conn.cursor.execute("SELECT pg_try_advisory_lock(1)")
            row = conn.cursor.fetchall()
            lock = row[0][0]

        # wait table to be created by some process
        if not lock:
            while self.data_tablename not in table_names or self.poly_tablename not in table_names:
                conn.cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema='public'")
                output = conn.cursor.fetchall()
                table_names = [x[0] for x in output]

        # ------------------------------------------------------------------------
        # Create the models table if it doesn't exist
        # Fields that start with '_eng' are intended for private use by the engine
        #  and should not be used by the UI
        if self.poly_tablename not in table_names and lock:
            self._logger.info("Creating table %r", self.poly_tablename)
            fields = [
              "poly_id          BIGSERIAL",
              "poly_name        TEXT DEFAULT NULL", 
              "poly_hash        TEXT NOT NULL",
              "geometry         geometry(GEOMETRY, 3577) NOT NULL",
              "shapefile        TEXT",
              "feature_id       INT",
              "result_ready     BOOL DEFAULT FALSE",
              "last_update      TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL",
              "area             FLOAT DEFAULT 0",
              "catchment_id     INT",
              "PRIMARY KEY (poly_id)",
              "UNIQUE (poly_hash)"
              ]

            query = "CREATE TABLE %s (%s)" % \
                    (self.poly_tablename, ",".join(fields))
            conn.cursor.execute(query)

            query = "CREATE INDEX time_idx ON %s (%s DESC)" % \
                    (self.poly_tablename, ",".join(['last_update']))
            conn.cursor.execute(query)

            query = "CREATE INDEX polygons_gix ON %s USING GIST (%s)" % \
                    (self.poly_tablename, ",".join(['geometry']))
            conn.cursor.execute(query)
 
        # ------------------------------------------------------------------------
        # Create the jobs table if it doesn't exist
        # Fields that start with '_eng' are intended for private use by the engine
        #  and should not be used by the UI
        if self.data_tablename not in table_names and lock:
            self._logger.info("Creating table %r", self.data_tablename)
            fields = [
              "item_id    BIGSERIAL",
              "poly_id    INT REFERENCES polygons (poly_id)",
              "datetime     TIMESTAMP WITHOUT TIME ZONE DEFAULT NULL",
              "fc_bs        FLOAT DEFAULT 0",
              "fc_pv        FLOAT DEFAULT 0",
              "fc_npv       FLOAT DEFAULT 0",
              "tci_w        FLOAT DEFAULT 0",
              "wofs_water   FLOAT DEFAULT 0",
              "PRIMARY KEY (item_id)",
              "UNIQUE (poly_id, datetime)"
              ]

            query = "CREATE TABLE %s (%s)" % \
                      (self.data_tablename, ",".join(fields))
            conn.cursor.execute(query)

        if lock:
            conn.dbConn.commit()
            conn.cursor.execute("SELECT pg_advisory_unlock(1)")
        # ---------------------------------------------------------------------
        # Get the field names for each table
        conn.cursor.execute("SELECT column_name from information_schema.columns WHERE table_name='%s'" % (self.data_tablename))
        fields = conn.cursor.fetchall()
        self.data.dbFieldNames = [str(field[0]) for field in fields]

        conn.cursor.execute("SELECT column_name from information_schema.columns WHERE table_name='%s'" % (self.poly_tablename))
        fields = conn.cursor.fetchall()
        self.polygons.dbFieldNames = [str(field[0]) for field in fields]

        if self.alltime_metrics.tableName not in table_names:
            conn.cursor.execute(alltime_count_view)

        if self.first_observe.tableName not in table_names:
            conn.cursor.execute(first_observe_view)

        if self.year_metrics.tableName not in view_names:
            conn.cursor.execute(year_metric_view)

        if self.event_metrics_time.tableName not in table_names: 
            # this is messy and you don't want this
            conn.cursor.execute(event_metrics_time_table)
            conn.dbConn.commit()
            # popularize the table
            conn.cursor.execute(first_event_metrics)
            conn.dbConn.commit()
            conn.cursor.execute("SELECT count(poly_id) from %s" % (self.event_metrics_time.tableName))
            last_size = conn.cursor.fetchall()[0][0]
            this_size = 0
            while last_size != this_size:
                last_size = this_size
                conn.cursor.execute(more_event_metrics)
                conn.dbConn.commit()
                conn.cursor.execute("SELECT count(poly_id) from %s" % (self.event_metrics_time.tableName))
                this_size = conn.cursor.fetchall()[0][0]
            conn.cursor.execute(last_event_metrics)

        if self.incomplete_event.tableName not in table_names: 
            conn.cursor.execute(incomplete_event_table)
            conn.cursor.execute("alter table incomplete_event add column event_id bigserial")
            conn.cursor.execute("alter table incomplete_event add primary key (event_id)")
            conn.cursor.execute("alter table incomplete_event add constraint incomplete_event_fk foreign" \
                    "key (poly_id) references polygons (poly_id)")
            conn.dbConn.commit()

        if self.event_metrics.tableName not in view_names:
            conn.cursor.execute(event_metrics_view)

        if self.catchment.tableName not in table_names:
            self._logger.info("Creating table %r", self.data_tablename)
            fields = [
              "catchment_id    BIGSERIAL",
              "catchment_name  TEXT DEFAULT NULL", 
              "catchment_hash  TEXT NOT NULL",
              "shapefile        TEXT",
              "feature_id       INT",
              "geometry     geometry(GEOMETRY, 3577) NOT NULL",
              "PRIMARY KEY (catchment_id)",
              "UNIQUE (catchment_hash)"
              ]
            query = "CREATE TABLE %s (%s)" % \
                      (self.catchment.tableName, ",".join(fields))
            conn.cursor.execute(query)

            query = "CREATE INDEX catchments_gix ON %s USING GIST (%s)" % \
                    (self.catchment.tableName, ",".join(['geometry']))
            conn.cursor.execute(query)

        return

    def construct_query(self, tableInfo, fieldsToMatch, selectFieldNames, func=None, maxRows=None):
        """ Return a sql query from a table or empty sequence if nothing matched.

        tableInfo:       Table information: a DIO.TableInfoBase  instance
        fieldsToMatch:   Dictionary of databale column names
        selectFieldNames: list of fields to return, using internal field names
        maxRows:         maximum number of rows to return; unlimited if maxRows
                          is None

        retval:         A sql string 
        """

        # NOTE: make sure match expressions and values are in the same order
        matchPairs = list(fieldsToMatch.items())
        sqlParams = []

        def matchExpressionGen():
            def dumpFieldValues(obj):
                  if isinstance(obj, self._SEQUENCE_TYPES):
                    for val in obj:
                      dumpFieldValues(val)
                  else:
                      matchFieldValues.append(obj)

            for p in matchPairs:
                matchFieldValues = []
                if isinstance(p[1], bool):
                    yield (p[0] + ' IS ' + {True:'TRUE', False:'FALSE'}[p[1]])
                elif p[1] is None:
                    yield (p[0] + ' IS NULL')
                else:
                    dumpFieldValues(p[1])

                if len(matchFieldValues) == 1:
                    matchFieldValues = matchFieldValues[0]
                    yield(p[0] + '=%s')
                elif len(matchFieldValues) > 1:
                    matchFieldValues = tuple(matchFieldValues)
                    yield(p[0] + ' IN %s')

                if matchFieldValues != []:
                    sqlParams.append(matchFieldValues)

        if func is not None:
            query = 'SELECT ' + func + '(%s) FROM %s WHERE (%s)' % (
              ','.join(selectFieldNames), tableInfo.tableName,
              ' AND '.join(matchExpressionGen()))
        else:
            query = 'SELECT %s FROM %s WHERE (%s)' % (
              ','.join(selectFieldNames), tableInfo.tableName,
              ' AND '.join(matchExpressionGen()))
        if maxRows is not None:
            query += ' LIMIT %s'
            sqlParams.append(maxRows)
        return query, sqlParams, maxRows



    def get_matching_rows(self, conn, query, sqlParams, maxRows):
        conn.cursor.execute(query, sqlParams)
        rows = conn.cursor.fetchall()
        if rows:
            assert maxRows is None or len(rows) <= maxRows, "%d !<= %d" % (
              len(rows), maxRows)
        else:
            rows = tuple()

        return rows

    @classmethod
    def normalize_hash(cls, hashValue):
        hashLen = len(hashValue)
        if hashLen < cls.HASH_MAX_LEN:
            hashValue += '\0' * (cls.HASH_MAX_LEN - hashLen)
        else:
            assert hashLen <= cls.HASH_MAX_LEN, (
            "Hash is too long: hashLen=%r; hashValue=%r") % (hashLen, hashValue)

        return hashValue

    def insert_get_polygon(self, conn, poly_name,  geometry, shapefile, feature_id):
        """ Attempt to insert a row with the given parameters into the polygons table.
        Return poly_id of the inserted row, or of an existing row with matching
        poly_hash.

        The poly_hash is expected to be unique (enforced by a unique index on the column).
        retval: poly_id of the inserted polygons row, or of an existing polygon entry row
                           with matching poly_hash key
        """
        poly_id = 0
        query = "INSERT  INTO %s (poly_name, poly_hash, geometry, " \
                " shapefile, feature_id, result_ready, last_update) " \
                " VALUES (%%s, ST_GeoHash(ST_Transform(%%s::geometry, 4326), 32), %%s, %%s, %%s, FALSE, to_timestamp(0)) " \
                " ON CONFLICT (poly_hash) DO UPDATE SET poly_name=%%s" \
                " RETURNING poly_id, result_ready" \
                % (self.poly_tablename,)
        sqlParams = (poly_name, geometry, geometry, shapefile, feature_id, poly_name)
        conn.cursor.execute(query, sqlParams)
        numRowsInserted = conn.cursor.fetchall() 
        assert len(numRowsInserted) == 1, 'Unexpected num fields: ' + repr(len(numRowsInserted))

        poly_id = numRowsInserted[0][0]
        state = numRowsInserted[0][1]
        self._logger.debug('polygon insert %s' %(poly_id))
        return poly_id, state

    def update_polygon(self, conn, poly_id, **kwargs):
        query = ""
        sqlParams = ()
        for key, value in kwargs.items():
            if query != "":
                query += ", "
            query += key + "=%s"
            if key == 'last_update':
                query += key + "=to_timestamp(%%s, 'YYYY-MM-DD HH24:MI:SS.US') "
            sqlParams += (value, )
        sqlParams += (poly_id, )

        query = "UPDATE %s SET " % (self.poly_tablename,) + query + " WHERE poly_id=%s" \
                " RETURNING poly_id"
        conn.cursor.execute(query, sqlParams)
        numRowsAffected = conn.cursor.fetchall() 

        if len(numRowsAffected) == 1:
            poly_id = numRowsAffected[0][0]
        return poly_id

    def insert_get_data(self, conn, poly_id, datetime, fc_bs, fc_pv, fc_npv, tci_w, wofs_water):
        """ Attempt to insert a row with the given parameters into the data table.
        Return item_id of the inserted row, or of an existing row with matching
        poly_id and datetime key.

        The combination of poly_id and datetime are expected to be unique (enforced
        by a unique index on the two columns).
        retval:           item_id of the inserted data row, or of an existing data entry row
                           with matching poly_id/datetime key
        """

        # Create a new data entry
        item_id = 0
        query = "INSERT  INTO %s (poly_id, datetime, fc_bs, fc_pv," \
                " fc_npv, tci_w, wofs_water) " \
                " VALUES (%%s, to_timestamp(%%s, 'YYYY-MM-DD HH24:MI:SS.US'), %%s, %%s, %%s, %%s, %%s) " \
                " ON CONFLICT DO NOTHING " \
                " RETURNING item_id" \
                % (self.data_tablename,)
        
        sqlParams = (poly_id, datetime, fc_bs, fc_pv, fc_npv, tci_w, wofs_water)

        conn.cursor.execute(query, sqlParams)
        numRowsInserted = conn.cursor.fetchall() 

        if len(numRowsInserted) == 1:
            item_id = numRowsInserted[0][0]
        # ---------------------------------------------------------------------
        # If asked to enter the job in the running state, set the connection id
        #  and start time as well
        self._logger.debug('data insert %s' %(item_id))
        return item_id

    def update_result_state(self, conn, poly_id, ready, update_time):
        """ 
        The poly_id  is expected to be unique (enforced by a unique index on the column).
        """

        # Create a new data entry

        state = ready

        if ready:
            ready = 'TRUE'
        else:
            ready = 'FALSE'

        query = " UPDATE %s SET result_ready=%%s," \
                " last_update=to_timestamp(%%s, 'YYYY-MM-DD HH24:MI:SS.US') " \
                " WHERE poly_id=%%s AND result_ready = FALSE " \
                " AND last_update<=to_timestamp(%%s, 'YYYY-MM-DD HH24:MI:SS.US')" \
                " RETURNING result_ready" \
                % (self.poly_tablename,)
        sqlParams = (ready, update_time, poly_id, update_time)
        conn.cursor.execute(query, sqlParams)
        numRowsAffected = conn.cursor.fetchall() 

        if len(numRowsAffected) == 1:
            state = numRowsAffected[0][0]
        return state

    def insert_get_catchment(self, conn, catchment_name,  shapefile, feature_id, geometry):
        """ Attempt to insert a row with the given parameters into the polygons table.
        Return poly_id of the inserted row, or of an existing row with matching
        poly_hash.

        The poly_hash is expected to be unique (enforced by a unique index on the column).
        retval: poly_id of the inserted polygons row, or of an existing polygon entry row
                           with matching poly_hash key
        """
        catchment_id = 0
        query = "INSERT  INTO %s (catchment_name, catchment_hash, shapefile, feature_id, geometry) " \
                " VALUES (%%s, ST_GeoHash(ST_Transform(%%s::geometry, 4326), 32), %%s, %%s, %%s) " \
                " ON CONFLICT (catchment_hash) DO UPDATE SET catchment_name=%%s" \
                " RETURNING catchment_id" \
                % (self.catchment.tableName,)
        sqlParams = (catchment_name, geometry, shapefile, feature_id, geometry, catchment_name)
        conn.cursor.execute(query, sqlParams)
        numRowsInserted = conn.cursor.fetchall() 
        assert len(numRowsInserted) == 1, 'Unexpected num fields: ' + repr(len(numRowsInserted))

        catchment_id = numRowsInserted[0][0]
        self._logger.debug('catchment insert %s' %(catchment_id))
        return catchment_id

    def insert_polygon(self, poly_name, geometry, shapefile, feature_id):
        with ConnectionFactory.get() as conn:
            poly_id, state = self.insert_get_polygon(conn, poly_name, geometry, shapefile, feature_id)
        return poly_id, state

    def update_polygon_geom(self, poly_id, geometry):
        with ConnectionFactory.get() as conn:
            poly_id = self.update_polygon(conn, poly_id, geometry=geometry)
        return poly_id

    def insert_update_result(self, poly_id, ready, datetime, fc_bs, fc_pv, fc_npv, tci_w, wofs_water):
        with ConnectionFactory.get() as conn:
            state = self.update_result_state(conn, str(poly_id), ready, datetime)
            if fc_bs > 0 or fc_pv > 0 or fc_npv > 0 or tci_w > 0 or wofs_water > 0:
                item_id = self.insert_get_data(conn, int(poly_id), datetime, fc_bs, fc_pv, fc_npv, tci_w, wofs_water)
            else:
                item_id = None
        return item_id, state

    def insert_catchment(self, catchment_name, shapefile, feature_id, geometry):
        with ConnectionFactory.get() as conn:
            catchment_id = self.insert_get_catchment(conn, catchment_name, shapefile, feature_id, geometry)
        return catchment_id

    def get_latest_time(self, poly_list):
        query, sql_params, max_rows = self.construct_query(self.polygons,
                dict(poly_id=poly_list, result_ready=False), ['last_update'], func='max')
        with ConnectionFactory.get() as conn:
            row = self.get_matching_rows(conn, query, sql_params, max_rows)
        assert len(row) == 1, 'Unexpected num fields: ' + repr(len(row))
        time = row[0][0]
        return time

    def get_min_time(self, poly_list):
        query, sql_params, max_rows = self.construct_query(self.polygons,
                dict(poly_id=poly_list, result_ready=False), ['last_update'], func='min')
        with ConnectionFactory.get() as conn:
            row = self.get_matching_rows(conn, query, sql_params, max_rows)
        assert len(row) == 1, 'Unexpected num fields: ' + repr(len(row))
        time = row[0][0]
        return time
      
    def get_id_by_geom(self, table_name, geometry):
        if table_name == self.catchment.tableName:
            poly_id = 'catchment_id'
            poly_name = 'catchment_name'
            poly_hash = 'catchment_hash'
        elif table_name == self.polygons.tableName:
            poly_id = 'poly_id'
            poly_name = 'poly_name'
            poly_hash = 'poly_hash'
        query = "SELECT %s, %s from %s " \
                "where %s=ST_GeoHash(ST_Transform(%%s::geometry,4326), 32)" \
                % (poly_id, poly_name, table_name, poly_hash,)
        sql_params = (geometry,) 
        with ConnectionFactory.get() as conn:
            row = self.get_matching_rows(conn, query, sql_params, None)
            if len(row) == 0:
                return 0, '' 
            poly_id = row[0][0]
            poly_name = row[0][1]
            return poly_id, poly_name
    
    def get_data_by_poly_id(self, poly_id):
        query, sql_params, max_rows = self.construct_query(self.data,
                dict(poly_id=poly_id), ['datetime', 'fc_bs', 'fc_npv', 'fc_pv', 'tci_w', 'wofs_water'])
        with ConnectionFactory.get() as conn:
            row = self.get_matching_rows(conn, query, sql_params, max_rows)
        return row

    def get_data_by_geom(self, geometry):
        poly_id, poly_name = self.get_id_by_geom(self.poly_tablename, geometry)
        if poly_id == 0:
            return '', []
        row = self.get_data_by_poly_id(poly_id)
        return poly_name, row

    def get_polys_by_catchment_id(self, catchment_id, maxrows=None):
        query = "SELECT poly_id from %s as p, %s as c " \
                " WHERE ST_Contains(c.geometry, p.geometry) AND c.catchment_id=%%s " \
                " ORDER BY ST_Area(p.geometry)" % (self.poly_tablename, self.catchment.tableName,)
        if maxrows is not None:
            query += " LIMIT %s" % (maxrows,)
        sql_params = (catchment_id,)
        with ConnectionFactory.get() as conn:
            row = self.get_matching_rows(conn, query, sql_params, None)
        return row

    def get_polys_by_geom(self, geometry, maxrows=None):
        catchment_id, catchment_name = self.get_id_by_geom(self.catchment.tableName, geometry)
        if catchment_id == 0:
            return '', []
        row = self.get_polys_by_catchment_id(catchment_id, maxrows)
        return catchment_name, row

    def get_alltime_metrics(self, poly_list):
        if not isinstance(poly_list, self._SEQUENCE_TYPES):
            poly_list = tuple([poly_list])
        query = "SELECT * FROM (SELECT poly_id, coalesce(pv/total::float, 0) as pv_perc, " \
                " coalesce(openwater/total::float, 0) as openwater_penc, coalesce(wet/total::float, 0) as wet_perc " \
                " FROM %s) AS a NATURAL JOIN (SELECT poly_id, pv as pv_fot, openwater as openwater_fot, " \
                " wet as wet_fot FROM %s) as b WHERE poly_id IN %%s" % (self.alltime_metrics.tableName, self.first_observe.tableName)
        sql_params = (tuple(poly_list),) 
        with ConnectionFactory.get() as conn:
            row = self.get_matching_rows(conn, query, sql_params, None)
        return row

    def get_year_metrics(self, poly_list):
        query, sql_params, max_rows = self.construct_query(self.year_metrics,
                dict(poly_id=poly_list), ['poly_id', 'year', 'min', 'max'])
        query = query + " ORDER BY poly_id ASC, year ASC"
        with ConnectionFactory.get() as conn:
            row = self.get_matching_rows(conn, query, sql_params, max_rows)
        return row 

    def get_event_metrics(self, poly_list):
        query, sql_params, max_rows = self.construct_query(self.event_metrics,
                dict(poly_id=poly_list), ['poly_id', 'start_time', 'end_time', 'duration', 'max', 'mean', 'area'])
        query = query + " ORDER BY poly_id ASC, start_time ASC"
        with ConnectionFactory.get() as conn:
            row = self.get_matching_rows(conn, query, sql_params, max_rows)
        return row 

    def query_with_return(self, query):
        with ConnectionFactory.get() as conn:
            conn.cursor.execute(query)
            output = conn.cursor.fetchall()
        return output
