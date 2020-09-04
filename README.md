# Convert select query results into JSON.

Generating a SQL in a JSON file in a few steps:
- Defining a connection to specific databases.
- Add SQL files to a directory (sqls) belonging to a specific database.
- Run the program and expect the JSON generated files.


Examples of parameters for connection to specific databases
```ini
[ibm]
DATABASE = db2
HOSTNAME = ip
PORT     = 3700
PROTOCOL = TCPIP
UID      = user
PWD      = pass

[mssql]
SERVER   = ip
USER     = user
PASSWORD = pass
DATABASE = db
```

