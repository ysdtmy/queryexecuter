# -*-conding utf-8-*-

import logging
import configparser
import os
import argparse
import json
import collections
import yaml
import datetime
from QueryEngine import QueryEngine

formatter = '%(asctime)s : %(levelname)s : %(name)s :  %(message)s'
logging.basicConfig(level=logging.INFO, format=formatter)
logger = logging.getLogger('queryexecuter')
logpath = './log/'


def main(batchfile, dryrun=False, log=False):

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

    if log:
        if not os.path.exists(logpath):
            os.makedirs(logpath)

        now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
        logname = logpath + 'qe_' + now + '.log'
        loggerfh = logging.FileHandler(logname)
        loggerfh.setLevel(logging.INFO)
        loggerfh.setFormatter(logging.Formatter(formatter))
        logger.addHandler(loggerfh)
        logger.info('Logfile Name : %s', logname)

    logger.info('LOGGINIG : %s', log)
    logger.info('BATCH FILE : %s', batchfile)
    logger.info('DRYRUNMODE : %s', dryrun)

    defautsetting, executes = _parse_batchfile(batchfile)

    for step, execute in executes.items():
        logger.info('QUERYSTEP : %i', int(step))
        setting = _overwritesetting(defautsetting, execute)
        dbinfofile = setting["DatabaseInfoFile"]
        engine = setting["QueryEngine"]
        dbinfo = setting["DatabaseInfo"]
        sqlfile = setting["SQLFile"]

        logger.info('DBinfofile : %s', dbinfofile)
        logger.info('Engine : %s', engine)
        logger.info('DBInfo : %s', dbinfo)
        logger.info('SQLFile : %s', sqlfile)

        parameter = setting["Parameter"] if "Parameter" in setting.keys(
        ) else None
        option = setting["Option"] if "Option" in setting.keys() else None

        logger.info('ParameterFile : %s', parameter)
        logger.info('Option : %s', option)

        targetdbdict = _get_targetdbinfo(dbinfofile, dbinfo)

        qe = QueryEngine(engine, targetdbdict)

        query = _get_query_from_file(sqlfile)

        if parameter != None:
            query = _parse_query(query, parameter)

        logger.info('Execute Query')
        logger.info('\n' + query)

        # Skip connection & execute if dryrun mode.
        if dryrun:
            continue

        try:
            qe.connect()
        except Exception as e:
            logger.error(e)
            break

        try:
            result = qe.execute(query)
        except Exception as e:
            logger.error(e)
            break

        logger.info('RESULT : %s', result)

    logger.info("END")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument('--dryrun', '-d', action='store_true', default=False)
    parser.add_argument('--batchfile', '-f')
    parser.add_argument('--log', '-l', action='store_true', default=False)
    args = parser.parse_args()
    main(batchfile=args.batchfile, dryrun=args.dryrun, log=args.log)
