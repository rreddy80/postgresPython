"""
backup a database table:
*************************
python connect2pg.py <<environment>>

run a query against a given environment:
******************************************
python connect2pg.py <<environment>> <<database-name>> <<query-to-run>>

to simply list all databases:
******************************
python connect2pg.py <<environment>> list_databases

to simply list all tables in a database:
*****************************************
python connect2pg.py <<environment>> <<database-name>> list_tables

to simply get all data in a table:
***********************************
python connect2pg.py <<environment>> <<database-name>> list_all_<<table_name>>

to simply get only certain # of rows from a table:
***********************************
python connect2pg.py <<environment>> <<database-name>> list_<<number>>_<<table_name>>

"""
import json
import argparse

from connection import ConnectDB

def format_query(given_query):
    """ Process a query based on the user input """
    query_list = given_query.lower().split('_')
    try:
        if query_list[0] == 'list' and query_list[1] == 'tables':
            return "select table_name from information_schema.tables \
            where table_schema='public'"
        elif query_list[0] == 'list' and query_list[1] == 'databases':
            return "SELECT datname FROM pg_database WHERE datistemplate = false"
        elif query_list[0] == 'list' and query_list[1] == 'all':
            return "SELECT * FROM {0}".format('_'.join(query_list[2:]))
        elif query_list[0] == 'list' and query_list[1].isdigit():
            return "SELECT * FROM {1} limit {0}".format(
                query_list[1], '_'.join(query_list[2:])
                )
        else:
            return given_query
    except KeyError:
        return given_query

if __name__ == "__main__":
    PARSER = argparse.ArgumentParser(description=\
    'Connect to a Postgres and Run Queries against it via SSH Tunnels')
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

        DB_CONNECTION = ConnectDB(**PARMS)
        if PARMS['db_server'] != 'localhost':
            SERVER = DB_CONNECTION.connect_db_server(**PARMS)
            GIVEN_PORT = SERVER.local_bind_port
        else:
            GIVEN_PORT = int(PARMS['db_port'])
            print "connecting locally forwarding traffic at port: {0}".format(GIVEN_PORT)

    DB_CONNECTION.log(PARMS['db_server'])
    DB_CONNECTION.log(PARMS['db_port'])
    ARGS.query = format_query(ARGS.query)
    DB_CONNECTION.log("{0}: {1}".format(ARGS.database, ARGS.query))
    DB_CONNECTION.run_command(ARGS, GIVEN_PORT)

    if SERVER is not None:
        SERVER.stop()
