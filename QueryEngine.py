# -*-conding utf-8-*-

import importlib
import prestodb
import psycopg2
import yaml
import logging

logging = logging.getLogger('queryexecuter').getChild(__name__)


class QueryEngine:

    def __init__(self, qe, dbinfo, option=None, *arg, **kwarg):
        logging.info('QueryEngine : %s', qe)
        logging.info('DatabaseInfo : %s', dbinfo)
        logging.info('Option : %s', option)
        self.qe = eval(qe + '()')
        self.dbinfo = dbinfo
        self.option = option

    def connect(self):
        logging.info('Connecting')

        try:
            self.con = self.qe.connect(self.dbinfo)
            logging.info('Connection Sucess')
        except Exception:
            logging.error('Connection failed')
            raise

        return True

    def execute(self, sql):
        try:
            result = self.qe.execute(self.con, sql)
            logging.info('Executing Sucess')
        except Exception:
            logging.error('Executing failed')
            raise

        return result


class DbConnectionSkelton:

    def __init__(self):
        self.connectiontype = self.__class__.__name__

    def connect(self, dbinfo):
        raise NotImplementedError

    def execute(self, con, sql, *arg, **kwarg):
        raise NotImplementedError


class SampleDbDef(DbConnectionSkelton):

    def __init__(self):
        super().__init__()

    def connect(self, dbinfo):
        conobj = dbinfo
        print('Create Connection Object')
        return conobj

    def execute(self, con, sql, *arg, **kwarg):
        print('Connecting Object: ' + con)
        print('Query: ' + sql)


class Postgresql(DbConnectionSkelton):

    def __init__(self):
        super().__init__()

    def connect(self, dbinfo):
        conn = psycopg2.connect(
          **dbinfo
        )

        return conn

    def execute(self, con, sql, *arg, **kwarg):
        cur = con.cursor()
        result = cur.execute(sql)
        rows = cur.fetchall()
        return rows


class Presto(DbConnectionSkelton):

    def __init__(self):
        super().__init__()

    def connect(self, dbinfo):
        conn = prestodb.dbapi.connect(
            **dbinfo
        )

        return conn


    def execute(self, con, sql, *arg, **kwarg):

        def _remove_semicolon(targetsql):
            sql_parsed = targetsql.replace(";", "")
            return sql_parsed

        cur = con.cursor()
        result = cur.execute(_remove_semicolon(sql))
        rows = cur.fetchall()
        return rows


class Spark(DbConnectionSkelton):

    def __init__(self):
        super().__init__()

    def connect(self, dbinfo):
        pass

    def execute(self, con, sql, *arg, **kwarg):
        pass
