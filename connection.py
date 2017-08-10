"""
Connection object
"""
from datetime import datetime
from time import strftime, gmtime
import sys
import csv
import psycopg2
from sshtunnel import SSHTunnelForwarder


class ConnectDB(object):
    """
    Create a class that can run a query against a DB and return results
    """
    __COMMIT_REQUIRED = ['delete', 'update', 'insert']

    def __init__(self, **kwargs):
        """ instantiate the class variables (and other variables) """
        self._db_user = kwargs['db_user']
        self._db_password = kwargs['db_password']
        self._db_server = kwargs['db_server']
        self._db_port = kwargs['db_port']
        self._default_db = kwargs['default_database']

    def connect_db_server(self, **kwargs):
        """create a DB connection using SSH SSHTunnelForwarder"""
        server = SSHTunnelForwarder(
            (kwargs['ssh_server'], 22),
            ssh_password=kwargs['ssh_pass'],
            ssh_username=kwargs['ssh_user'],
            remote_bind_address=(kwargs['db_server'], kwargs['db_port']),
            local_bind_address=('0.0.0.0', 10022))
        return self.start_server(server)

    def start_server(self, server):
        """start the server"""
        try:
            self.log(server.local_bind_port)
        except:
            server.start()
            self.log("started server at port: {0}".format(server.local_bind_port))
        return server

    def log(self, given_message):
        """log a given message in a particular format"""
        print "{0} {1}".format(self.get_formatted_time(), given_message)
        with open('pylog.log', 'a') as new_file:
            new_file.write("{0} {1}\n".format(self.get_formatted_time(), given_message))


    def get_formatted_time(self):
        """get a time format"""
        return strftime("%Y-%m-%d %H:%M:%S ", gmtime())


    def print_columns(self, given_desc):
        """ After retriving results of query, first print column names """
        self.log(tuple([desc[0] for desc in given_desc]))


    def task_backup(self, _cursor, _table_name, _file_name):
        """ run a delete query """
        _cursor.execute("select * from {0}".format(_table_name))
        with open(_file_name, 'wb') as given_file:
            writer = csv.writer(given_file)
            writer.writerow([head[0] for head in _cursor.description])
            for each_row in _cursor.fetchall():
                writer.writerow(each_row)

    def get_backup_filename(self, args):
        """ return a unique filename using timestamp """
        return "{0}_{1}_{2}.csv".format(
            args.database,
            args.query.lower().split('backup_')[1],
            datetime.strftime(datetime.now(), '%Y%m%d%H%M%S'))

    def run_command(self, args, server_port):
        """main function to run a sql command against a given environment"""
        try:
            conn = psycopg2.connect(
                database=args.database,
                user=self._db_user,
                password=self._db_password,
                port=server_port
            )
            curs = conn.cursor()
            if args.query.lower().startswith('backup_'):
                self.task_backup(
                    curs,
                    args.query.lower().split('backup_')[1],
                    self.get_backup_filename(args)
                    )
            else:
                curs.execute("""{0}""".format(args.query))
                _operation = args.query.lower().split()[0]
                if _operation == "select":
                    self.print_columns(curs.description)
                    rows = curs.fetchall()
                    for each_row in rows:
                        self.log(each_row)
                if _operation in self.__COMMIT_REQUIRED > 0:
                    conn.commit()
            curs.close()
            conn.close()
        except (NameError, ValueError) as ex:
            self.log(ex)
            self.log(sys.exc_info()[1])
        except psycopg2.ProgrammingError as ex:
            self.log("""{0} can't be run.
            please check the tables in the selected DB (below)""".format(ex))
        except:
            self.log(sys.exc_info()[1])
        else:
            if curs.closed == 0:
                curs.close()
            if conn.closed == 0:
                conn.close()
