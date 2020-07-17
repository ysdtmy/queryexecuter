# -*-conding utf-8-*-

import importlib
import configparser
import prestodb
import psycopg2

# Read Configfile
dbinfo = configparser.ConfigParser()
dbinfo.read('./dbinfo.conf', encoding='utf-8')


class QueryEngine:

    def __init__(self, qe, coninfo):
        self.qe = eval(qe + '()')
        self.coninfo = coninfo

    def connect(self, *arg, **kwarg):
        self.con = self.qe.connect(self.coninfo)
        return True

    def execute(self, sql, *arg, **kwarg):
        result = self.qe.execute(self.con, sql)
        return result


class DbConnectionSkelton:

    def __init__(self):
        self.connectiontype = self.__class__.__name__
        print("Query Engine : " + self.connectiontype)

    def connect(self, coninfo):
        raise NotImplementedError

    def execute(self, con, sql, *arg, **kwarg):
        raise NotImplementedError


class SampleDbDef(DbConnectionSkelton):

    def __init__(self):
        super().__init__()

    def connect(self, coninfo):
        conobj = coninfo
        print('Create Connection Object')
        return conobj

    def execute(self, con, sql, *arg, **kwarg):
        print('Connecting Object: ' + con)
        print('Query: ' + sql)


class Postgresql(DbConnectionSkelton):

    def __init__(self):
        super().__init__()

    def connect(self):
        conn = psycopg2.connect(
            host=self.coninfo['host'], port=self.coninfo['port'], user=self.coninfo[
                'user'], password=self.coninfo['user'], database=self.coninfo['database']
        )

        return conn

    def execute(self, con, sql, *arg, **kwarg):
        cur = con.cursur()
        result = cur.execute()
        rows = result.fetchall()
        return rows


class Presto(DbConnectionSkelton):

    def __init__(self):
        super().__init__()

    def connect(self):
        conn = prestodb.connect(
            host=self.coninfo['host'], port=self.coninfo['port'], user=self.coninfo[
                'user'], password=self.coninfo['user'], database=self.coninfo['database']
        )

        return conn

    def execute(self, con, sql, *arg, **kwarg):
        cur = con.cursur()
        result = cur.execute()
        rows = result.fetchall()
        return rows


class Spark(DbConnectionSkelton):

    def __init__(self):
        super().__init__()

    def connect(self):
        pass

    def execute(self, con, sql, *arg, **kwarg):
        pass


if __name__ == "__main__":
    lis = ['Postgresql', 'Presto', 'Spark']
    coninfo = 'MyConnectionInformation'
    for l in lis:
        dbdef = QueryEngine(l, coninfo)
    sql = "SELECT * FROM test.tbl"
    qe = QueryEngine("SampleDbDef", coninfo)
    qe.connect()
    qe.execute(sql)
