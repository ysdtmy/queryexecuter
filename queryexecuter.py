# -*-conding utf-8-*-

import prestodb
import psycopg2
import logging
import configparser
import os
import argparse


formatter = '%(levelname)s : %(asctime)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=formatter)

# Directory
SQLBATCHFILE = './sql_batch.conf'
SQLDIR = './sql'


def main(dryrun=False, engine='postgresql'):

    def _create_connection(engine):

        try:
            logging.info('DBINFO host : %s', dbinfo[engine]['host'])
            logging.info('DBINFO port : %s', dbinfo[engine]['port'])
            logging.info('DBINFO user : %s', dbinfo[engine]['user'])

            if engine == 'presto':
                conn = prestodb.dbapi.connect(
                    host=dbinfo[engine]['host'], port=int(dbinfo[engine]['port']), user=dbinfo[engine]['user']
                )

        except KeyError:
            logging.info('Not found engine %s', engine)
            exit(1)

        return conn

    def _execute_query(conn, sql):
        cur = conn.cursor()
        result = cur.execute(sql)
        row = cur.fetchall()

        return result, row

    def _execute_sql_from_file(conn, sqlfile, parse_dict, dryrun=False):
        logging.info('EXECUTE SQLFILE : %s', sqlfile)
        resultset = (None, None)
        with open(os.path.join(SQLDIR, sqlfile), 'r') as f:
            sql = f.read()
            parsed_sql = _parse_query(sql, parse_dict)
            logging.info('EXECUTE SQL : %s', parsed_sql)
            if dryrun == False:
                resultset = _execute_query(conn, parsed_sql)

        return resultset

    def _parse_query(sql, parse_dict):
        parsed_sql = sql.format(**parse_dict)

        return parsed_sql

    def _create_parse_dict(inputtable, outputtable):
        pass

    logging.info('DRYRUNMODE : %s', dryrun)
    logging.info('QUERY ENGINE : %s', engine)
    conn = _create_connection(engine)
    parsed_dict = {'TBLNAME': 't_calendar'}
    logging.info('BATCHFILE : %s', SQLBATCHFILE)
    with open(SQLBATCHFILE, 'r') as f:
        for i, sql in enumerate(f.readlines()):
            sql = sql.rstrip(os.linesep)
            logging.info('QUERYSTEP : %i', i + 1)
            result, row = _execute_sql_from_file(
                conn, sql, parsed_dict, dryrun)
            logging.info('RESULT : %s', result)
            logging.info('ROW : %s', row)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dryrun', action='store_true', default=False)
    parser.add_argument('--engine', '-e', default='postgresql')
    args = parser.parse_args()
    main(dryrun=args.dryrun, engine=args.engine)
