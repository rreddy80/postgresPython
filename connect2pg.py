"""
run a query against a given environment:
******************************************
python connect2pg.py <<environment>> <<database-name>> <<query-to-run>>

to simply list all databases:
******************************
python connect2pg.py <<environment>> list_all_databases

to simply list all tables in a database:
*****************************************
python connect2pg.py <<environment>> <<database-name>> list_all_tables

to simply get all data in a table:
***********************************

"""
import sys
import json
import argparse
from time import strftime, gmtime
import csv
from datetime import datetime
import psycopg2
from sshtunnel import SSHTunnelForwarder


def connect_db_server(ssh_user, ssh_pass, db_server='10.123.45.6', db_port=5432, ssh_server='10.234.5.67', ssh_port=22):
    """create a DB connection using SSH SSHTunnelForwarder"""
    server = SSHTunnelForwarder(
        (ssh_server, ssh_port),
        ssh_password=ssh_pass,
        ssh_username=ssh_user,
        remote_bind_address=(db_server, db_port),
        local_bind_address=('0.0.0.0', 10022))
    server = start_server(server)
    return server


def log(given_message):
    """log a given message in a particular format"""
    print "{0} {1}".format(get_formatted_time(), given_message)
    with open('pylog.log', 'a') as new_file:
        new_file.write("{0} {1}\n".format(get_formatted_time(), given_message))


def get_formatted_time():
    """get a time format"""
    return strftime("%Y-%m-%d %H:%M:%S ", gmtime())


def start_server(server):
    """start the server"""
    try:
        log(server.local_bind_port)
    except:
        server.start()
        log("started server at port: {0}".format(server.local_bind_port))
    return server


def print_columns(given_desc):
    """ After retriving results of query, first print column names """
    log(tuple([desc[0] for desc in given_desc]))


def format_query(given_query):
    """ Process a query based on the user input """
    query_list = given_query.lower().split('_')
    try:
        if query_list[0] == 'list' and query_list[1] == 'tables':
            return "select table_name from information_schema.tables where table_schema='public'"
        elif query_list[0] == 'list' and query_list[1] == 'databases':
            return "SELECT datname FROM pg_database WHERE datistemplate = false"
        elif query_list[0] == 'get' and query_list[1] == 'all':
            return "SELECT * FROM {0}".format('_'.join(query_list[2:]))
        elif query_list[0] == 'get' and query_list[1].isdigit():
            return "SELECT * FROM {1} limit {0}".format(query_list[1], '_'.join(query_list[2:]))
        else:
            return given_query
    except KeyError:
        return given_query


def run_command(args, server_port):
    """main function to run a sql command against a given environment"""
    try:
        conn = psycopg2.connect(
            database=args.database,
            user=str(PARMS['db_user']),
            password=str(PARMS['db_password']),
            port=server_port
        )
        curs = conn.cursor()
        if args.query.lower().startswith('backup_'):
            curs.execute("select * from {0}".format(args.query.split('backup_')[-1]))
            cur_time = datetime.strftime(datetime.now(), '%Y%m%d%H%M%S')
            with open("{0}_{1}_{2}.csv".format(
                args.database,
                args.query.split('backup_')[-1],
                cur_time
                ), 'wb') as given_file:
                writer = csv.writer(given_file)
                writer.writerow([head[0] for head in curs.description])
                for each_row in curs.fetchall():
                    writer.writerow(each_row)
        else:
            curs.execute("""{0}""".format(args.query))
            if format_query(args.query).lower().startswith("select"):
                print_columns(curs.description)
                rows = curs.fetchall()
                for each_row in rows:
                    log(each_row)
            if args.query.lower().startswith("delete") or args.query.lower().startswith("update"):
                conn.commit()
        curs.close()
        conn.close()
    except (NameError, ValueError) as ex:
        log(ex)
        log(sys.exc_info()[1])
    except psycopg2.ProgrammingError as ex:
        log("{0} can't be run. please check the tables in the selected DB (below)".format(ex))
        #x = printTables(curs)
    except:
        log(sys.exc_info()[1])
    else:
        if curs.closed == 0:
            curs.close()
        if conn.closed == 0:
            conn.close()


if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description=\
    'Connect to a Postgres and Run Queries against it via SSH Tunnels')
    PARSER.set_defaults(method=run_command)
    PARSER.add_argument('env', type=str, help=\
    'Environment Name where you want to run these queries?')
    PARSER.add_argument('database', type=str, help=\
    'Database name against which we run the query')
    PARSER.add_argument('query', type=str, help=\
    'query to execute')

    ARGS = PARSER.parse_args()

    SERVER = None
    with open(ARGS.env + '-Config.json') as f:
        PARMS = json.load(f)
        if PARMS['db_server'] != 'localhost':
            SERVER = connect_db_server(
                str(PARMS['ssh_user']),
                str(PARMS['ssh_pass']),
                str(PARMS['db_server']),
                int(PARMS['db_port']),
                str(PARMS['ssh_server']),
                22)
            GIVEN_PORT = SERVER.local_bind_port
        else:
            GIVEN_PORT = int(PARMS['db_port'])

    log(PARMS['db_server'])
    log(PARMS['db_port'])
    ARGS.query = format_query(ARGS.query)
    log("{0}: {1}".format(ARGS.database, ARGS.query))
    ARGS.method(ARGS, GIVEN_PORT)

    if SERVER is not None:
        SERVER.stop()
