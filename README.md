# postgresPython
Connect to a Database behind a jump server from command line and Run queries against it

To connect to a database, update env-Config.json to replace 

1. ssh_server: IP address of the jump server thru which you can connect to the database
2. ssh_user: username for the above jump server
3. ssh_pass: password for above user to log-in to the jump server
4. db_server: database server IP
5. db_port: port on which the database is running (default: 5432 for postgres)
6. default_database: default database to log-in to (for ex: postgres)
7. db_user: username, if any, to log-in to the database
8. db_password: password, if any, to log-in the database

If you have multiple hops to reach the Db server, probably its a better idea to use local port forwarding. In this case config file would rather be a simple like below

{
"db_server": "localhost",
"db_port": 6432,
"default_database": "postgres",
"db_user": "pguser",
"db_password": "pgpass"
}

where localhost:6432 is actually pointing at your Db server. As you can see you don't need to give SSH_Server details as you have tackled this separately by doing local port forwarding.

# Running queries against a database/ server

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