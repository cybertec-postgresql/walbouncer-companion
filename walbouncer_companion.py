#!/usr/bin/env python2

import argparse
import logging
import os
import shlex
import time
from datetime import datetime
from subprocess import Popen
import psycopg2
import psycopg2.extras
import yaml

TEMP_XLOGS_DIR = 'temp_xlogs'

config = None
pg_connection = None
args = None


def pg_init_connection(**args):
    global pg_connection
    logging.info('connecting to master Postgres DB...')
    pg_connection = psycopg2.connect(**args)
    pg_connection.autocommit = True


def pg_exec(sql, params=None):
    cur = pg_connection.cursor(cursor_factory=psycopg2.extras.RealDictCursor)
    cur.execute(sql, params)
    data = cur.fetchall()
    cur.close()
    return data


def load_config_file(config_file_path):
    global config
    config = yaml.load(open(config_file_path))


def get_included_and_excluded_tablespaces_from_config(application_name):
    logging.info('getting include/exclude tablespace info for %s', application_name)
    for c in config.get('configurations', []):
        for config_name, config_dict in c.items():
            if config_name == application_name:
                return config_dict.get('filter', {}).get('include_tablespaces', []), config_dict.get('filter', {}).get('exclude_tablespaces', [])
    return [], []


def get_included_and_excluded_databases_from_config(application_name):
    logging.info('getting include/exclude database info for %s', application_name)
    for c in config.get('configurations', []):
        for config_name, config_dict in c.items():
            if config_name == application_name:
                return config_dict.get('filter', {}).get('include_databases', []), config_dict.get('filter', {}).get('exclude_databases', [])
    return [], []


def get_tablespace_paths_by_oids(oids):
    if not oids:
        return []
    sql = """
        SELECT pg_catalog.pg_tablespace_location(u) from unnest(%s) u
    """
    d = pg_exec(sql, (oids,))
    return [x['pg_tablespace_location'] for x in d if x['pg_tablespace_location']]


def get_oids_for_tablespaces(tablespaces):
    if not tablespaces:
        return []
    sql = """
        SELECT DISTINCT oid FROM pg_tablespace WHERE spcname = ANY(%s)
    """
    d = pg_exec(sql, (tablespaces,))
    return [x['oid'] for x in d]


def get_oids_for_all_tablespaces():
    sql = """
        SELECT oid FROM pg_tablespace
    """
    d = pg_exec(sql)
    return [x['oid'] for x in d]


def get_oids_and_tablespaces_for_databases(database_names):
    if not database_names:
        return []
    sql = """
        SELECT DISTINCT oid, dattablespace, datname FROM pg_database WHERE datname = ANY(%s)
    """
    return pg_exec(sql, (database_names,))


def get_all_db_oids():
    sql = """SELECT oid FROM pg_database"""
    d = pg_exec(sql)
    return [x['oid'] for x in d]


def run_shell(cmd_line_params):
    if type(cmd_line_params) != list:
        raise Exception('run_shell() expects list input!')
    p = Popen(cmd_line_params)
    p.wait()
    # print "done"


def run_shell_returning_process(cmd_line_params):
    if type(cmd_line_params) != list:
        raise Exception('run_shell() expects list input!')
    p = Popen(cmd_line_params)
    return p
    # print "done"

# run_shell(['ls', '-l', ';;', 'sleep', '10'])
# run_shell(['ls', '-l'])


def pg_start_backup():
    label = str(datetime.now().date())
    sql = """
        select pg_start_backup(%s, true)
    """
    logging.info('running pg_start_backup...')
    if args.dry_run:
        return
    pg_exec(sql, (label,))


def pg_stop_backup():
    sql = """
        select pg_stop_backup()
    """
    logging.info('running pg_stop_backup...')
    if args.dry_run:
        return
    pg_exec(sql)


def get_master_data_diretory():
    sql = """show data_directory"""
    data = pg_exec(sql)
    return data[0]['data_directory']


def do_basebackup(host, user, master_pgdata, slave_pgdata, ts_excl_oids=None, db_excl_oids=None, tablespace_paths_to_copy=None):   # TODO check for tblspace symlinks
    excludes = ''

    for oid in db_excl_oids:
            excludes += " --exclude base/{}".format(oid)
            excludes += " --exclude pg_tblspc/*/*/{}".format(oid)

    for oid in max(ts_excl_oids, []):
        excludes += " --exclude pg_tblspc/{}".format(oid)

    cmd_t = "rsync {dry_run} -a --keep-dirlinks --exclude pg_xlog --exclude postmaster.pid {excludes} {user}@{host}:{master_pgdata}/ {slave_pgdata}"
    cmd = cmd_t.format(host=host, user=user, master_pgdata=master_pgdata, slave_pgdata=slave_pgdata, excludes=excludes,
                       dry_run=('-n' if args.dry_run else ''))
    cmd_as_args = shlex.split(cmd)
    logging.info("starting main file copy with: %s", cmd)

    if not args.dry_run:
        run_shell(cmd_as_args)

    for tblspc in tablespace_paths_to_copy:  # TODO in parallel
        cmd_t = "rsync {dry_run} -a {user}@{host}:{tblspc}/ {tblspc}"
        cmd = cmd_t.format(host=host, user=user, tblspc=tblspc, dry_run=('-n' if args.dry_run else ''))
        cmd_as_args = shlex.split(cmd)
        logging.info("starting tablespace path copy with: %s", cmd)

        if not args.dry_run:
            run_shell(cmd_as_args)


def ensure_dir(dir):    # relative path
    logging.info('creating dir "%s" if needed...', dir)
    if os.path.exists(dir):
        if os.listdir(dir):
            raise Exception('path "{}" exists and not empty!'.format(dir))
    else:
        os.mkdir(dir)


def rename_temp_to_xlog(datadir, temp_xlog_dir):    # relative paths
    logging.info('moving temp dir "%s" to "%s/pg_xlog"...', temp_xlog_dir, datadir)
    if not args.dry_run:
        run_shell(['mv', temp_xlog_dir, os.path.join(datadir, 'pg_xlog')])


def start_xlog_streaming(connect_params):     # TODO slots
    connect_params['datadir'] = TEMP_XLOGS_DIR

    cmd_t = "pg_receivexlog -h {host} -p {port} -U {user} -D {datadir}"
    cmd = cmd_t.format(**connect_params)
    cmd_as_args = shlex.split(cmd)

    logging.info('executing pg_receivexlog: %s', cmd)

    if args.dry_run:
        return
    return run_shell_returning_process(cmd_as_args)


def stop_xlog_streaming(process):
    logging.info('stopping pg_receivexlog process after 10s [in hope all XLOGS have been transferred. might need adjustment for very busy instances] ')

    if args.dry_run:
        return

    # just "hoping" this time is enough to fetch last xlog needed by the backup
    time.sleep(10)  # TODO check with archive if archiving enabled

    if process:
        process.terminate()
    else:
        logging.warning('invalid pg_receivexlog process, nothing to stop')


def main():
    argp = argparse.ArgumentParser(
        description='Creates a selective Postgres replica based on an Walbouncer config')
    argp.add_argument('-c', '--config', required=True, help='walbouncer yaml format config file')
    argp.add_argument('-r', '--replica-name', help='name of replica to be built (application_name from the config)', required=True)
    argp.add_argument('-u', '--user', help='user for rsync copy from master', default=os.getenv('USER'))
    argp.add_argument('-n', '--dry-run', action='store_true', help='just show what would be executed')
    argp.add_argument('-q', '--quiet', action='store_true', default=False)
    argp.add_argument('-D', '--pgdata', required=True)

    global args
    args = argp.parse_args()

    # basic validations
    if os.path.exists(args.pgdata) and os.listdir(args.pgdata):
        print 'ERROR, pgdata directory "', args.pgdata, '"must be empty or nonexisting'
        exit(1)

    logging.basicConfig(format='[%(asctime)s] %(message)s', level=(logging.ERROR if args.quiet else logging.INFO))

    logging.info(args)

    logging.info('loading config file from %s...', args.config)
    load_config_file(args.config)
    logging.info('config: %s', config)

    connect_config = config['master']
    connect_config['user'] = args.user
    pg_init_connection(**connect_config)

    ts_inc, ts_excl = get_included_and_excluded_tablespaces_from_config(args.replica_name)
    db_inc, db_excl = get_included_and_excluded_databases_from_config(args.replica_name)
    ts_excl_oids = []
    db_excl_oids = []

    logging.info('include_tablespaces: %s, exclude_tablespaces: %s', ts_inc, ts_excl)
    logging.info('include_databases: %s, exclude_databases: %s', db_inc, db_excl)

    if (ts_excl and ts_inc) or (db_excl and db_inc):
        logging.error('only exclude or include listings allowed per db/tblspc!')

    ts_all_oids = get_oids_for_all_tablespaces()
    if ts_excl:
        ts_excl_oids = get_oids_for_tablespaces(ts_excl)
    elif ts_inc:
        ts_inc_oids = get_oids_for_tablespaces(ts_inc + ['pg_default', 'pg_global'])
        ts_excl_oids = set(ts_all_oids) - set(ts_inc_oids)

    tablespace_paths_to_copy = get_tablespace_paths_by_oids(list(set(ts_all_oids) - set(ts_excl_oids)))
    logging.info('tablespace_paths_to_copy: %s', tablespace_paths_to_copy)

    if db_excl:
        db_excl_infos = get_oids_and_tablespaces_for_databases(db_excl)
        db_excl_oids = [x['oid'] for x in db_excl_infos]
    elif db_inc:
        db_inc_infos = get_oids_and_tablespaces_for_databases(db_inc + ['template0', 'template1'])
        db_inc_oids = [x['oid'] for x in db_inc_infos]
        db_all_oids = get_all_db_oids()
        db_excl_oids = set(db_all_oids) - set(db_inc_oids)

    logging.info('excluded tablespaces oids: %s', ts_excl_oids)
    logging.info('excluded database oids: %s', db_excl_oids)

    # if args.dry_run:
    master_datadir = get_master_data_diretory()

    ensure_dir(TEMP_XLOGS_DIR)

    t1 = time.time()

    p = start_xlog_streaming(connect_config)

    # just "hoping" this time is enough to start xlog streaming
    logging.info('waiting 10s for XLOG streaming to start...[in hope it''s enough]')
    if not args.dry_run:
        time.sleep(10)  # make into a param?

    success = False
    try:

        pg_start_backup()

        do_basebackup(config['master']['host'], args.user, master_datadir, args.pgdata,
                      ts_excl_oids=ts_excl_oids, db_excl_oids=db_excl_oids, tablespace_paths_to_copy=tablespace_paths_to_copy)
        success = True
    finally:

        try:
            pg_stop_backup()
        except:
            logging.error('WARNING! failure on pg_stop_backup(). manual call of pg_stop_backup() needed on master!')

        try:
            stop_xlog_streaming(p)
        except:
            logging.error('WARNING! failure on stop_xlog_streaming(). manual termination of process needed!')

        if success:
            rename_temp_to_xlog(args.pgdata, TEMP_XLOGS_DIR)

    logging.info('finished in %s s', round(time.time() - t1))


if __name__ == '__main__':
    try:
        main()
    except KeyboardInterrupt:
        logging.error('User interrupt. Exiting.')
