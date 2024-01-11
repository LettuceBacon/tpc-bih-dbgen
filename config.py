# Properties
from pathlib import Path
from subprocess import run as runCmd

# REQUIRED
# database where TPC-BiH data is generated
dbname = "" 
# user who performs the generation
user = "" 
# password of user
password = "" 
# host of database's connection
host = "localhost" 
# port of database's connection
port = "5432" 
# Path of tpch tables and tpch-dbgen folder
tpchTblPath = str(Path.cwd() / "tpch-dbgen")
# Path of psql
psqlPath = "psql"
# Destination of generated data and history
destPath = str(Path.cwd())
# Only generate V1data or generate V1data and history
v1Only = True
# Numbers of update operations
updateTimes = 10000

# # OPTION

# dbname = "tpchtestground" # database where TPC-BiH data is generated
# user = "mfj" # user who performs the generation
# password = "" # password of user
# host = "" # host of database's connection
# port = "" # port of database's connection

# v1Only = False # whether do a V1data generation or do V1data and history generation

# psqlPath = "/usr/bin/psql" # Path of psql

def config():
    assert type(v1Only) == bool
    assert updateTimes > 0
    assert len(psqlPath) > 0
    assert runCmd([psqlPath, "-V"]).returncode == 0
    # Ensure all tpch tables have been generated
    tableNames = ("nation", "region", "part", "supplier", 
                  "partsupp", "customer", "orders", "lineitem")
    for tblName in tableNames:
        tblPath = Path(tpchTblPath) / (tblName + ".tbl")
        assert tblPath.exists() == True