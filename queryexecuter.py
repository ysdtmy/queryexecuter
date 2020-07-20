# -*-conding utf-8-*-

import logging
import configparser
import os
import argparse
import json
import collections
import yaml
from QueryEngine import QueryEngine


formatter = '%(levelname)s : %(name)s : %(asctime)s : %(message)s'
logging.basicConfig(level=logging.INFO, format=formatter)
logging.getLogger(__name__)


def main(batchfile, dryrun=False):

    def _parse_query(sql, parameterfile):
        with open(parameterfile) as pf:
            paramdict = json.load(pf)
        parsed_sql = sql.format(**paramdict)

        return parsed_sql

    def _parse_batchfile(batchfile):
        with open(batchfile, 'r') as bf:
            batchdict = json.load(bf)
            defaultsetting = batchdict["DefaultSetting"]
            executes = batchdict["Execute"]

        return defaultsetting, executes

    def _get_targetdbinfo(dbinfofile, dbinfo):
        with open(dbinfofile, "r") as di:
            dbs = yaml.safe_load(di)
        dbdict = dbs[dbinfo]

        return dbdict

    def _get_query_from_file(sqlfile):
        with open(sqlfile, 'r') as sqlf:
            query = sqlf.read()

        return query

    def _overwritesetting(defaultsetting, execute):
        defalultkeys = defaultsetting.keys()
        for dkey in defalultkeys:
            if dkey not in execute.keys():
                execute[dkey] = defaultsetting[dkey]

        return execute

    logging.info('DRYRUNMODE : %s', dryrun)
    logging.info('BATCH FILE : %s', batchfile)

    defautsetting, executes = _parse_batchfile(batchfile)

    for step, execute in executes.items():
        logging.info('QUERYSTEP : %i', int(step))
        setting = _overwritesetting(defautsetting, execute)
        dbinfofile = setting["DatabaseInfoFile"]
        engine = setting["QueryEngine"]
        dbinfo = setting["DatabaseInfo"]
        sqlfile = setting["SQLFile"]

        logging.info('DBinfofile : %s', dbinfofile)
        logging.info('Engine : %s', engine)
        logging.info('DBInfo : %s', dbinfo)
        logging.info('SQLFile : %s', sqlfile)

        parameter = setting["Parameter"] if "Parameter" in setting.keys(
        ) else None
        option = setting["Option"] if "Option" in setting.keys() else None

        logging.info('ParameterFile : %s', parameter)
        logging.info('Option : %s', option)

        targetdbdict = _get_targetdbinfo(dbinfofile, dbinfo)

        qe = QueryEngine(engine, targetdbdict)

        query = _get_query_from_file(sqlfile)

        if parameter != None:
            query = _parse_query(query, parameter)

        logging.info('Execute Query')
        logging.info('\n' + query)

        # Skip connection & execute if dryrun mode.
        if dryrun:
            continue

        try:
            qe.connect()
        except Exception as e:
            logging.error(e)
            break

        try:
            result = qe.execute(query)
        except Exception as e:
            logging.error(e)
            break

        logging.info('RESULT : %s', result)

    logging.info("END")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dryrun', action='store_true', default=False)
    parser.add_argument('--batchfile', '-f')
    args = parser.parse_args()
    main(batchfile=args.batchfile, dryrun=args.dryrun)
