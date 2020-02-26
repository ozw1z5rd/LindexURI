# LindexURI
BigData URI utility

The idea is that every information stored is addressable using a URI, in this case we like to work with HDFS and HIVE. There

LindexURI.isValid( uri ) : returns true if the URI is valid, it's a static method and can be used in a quick way.

###luri = LindexURI(uri) 
###luri.isPartitioned()
returns true if the HIVE uri is defining a partitioned table

if uri == "hive://databasename/tablename?dt=201212" luri.isPartitioned returns True.

###luri.getPartitions() 
returns a dictionary that describes the HIVE partition

if uri == "hive://databasename/tablename?dt=201212" luri.getPartitions() returns

OrderedDict( 'dt': '201212' ) 

###luri.getDatabase()
gets the database name from the HIVE uri ( this can be modified to work also with HDFS paths )

if uri == "hive://databasename/tablename?dt=201212" luri.getDatabase() returns 'databasename'

##luri.getTable()
gets the table name from HIVE uri, can be modified to work also with HDFS paths

if uri == "hive://databasename/tablename?dt=201212" luri.getDatabase() returns 'tablename'

###luri.getHDFSHostName()
gets the HDFS hostname 

if uri == "hdfs://hdfs-prod/warehouse/databasename.db/tablename.db/dt=201212" luri.getHDFSHostName returns 'hdfs-prod'

###luri.getHDFSPath()
gets the path from the HDFS uri 

if uri == "hdfs://hdfs-prod/warehouse/databasename.db/tablename.db/dt=201212" luri.getHDFSPath() returns 'warehouse/databasename.db/tablename.db/dt=201212'

###luri.getSchema()
gets the schema 

if uri == "hdfs://hdfs-prod/warehouse/databasename.db/tablename.db/dt=201212" luri.getSchema() returns 'hdfs'

###luri.getPartitionsAsHDFSPath()
converts the partition coordinates into an HDFS path

p = OrderedDict( 'dt' : '201212', 'country': 'AU' ) 
dt=201212&country=AU

###luri.getHDFSPathAsPartition()
converts the HDFS path into a partition coordinates dictionary

       'hdfs://hdfs-production/Vault/Docomodigital/Production/Newton/events/prod/year=2018/month=08/day=07/hour=09'

        root path : "/Vault/Docomodigital/Production/Newton/events/prod/"

        partitions : {
            "year" : "2018",
            "month" : "08",
            "day" : "07",
            "hour" : "09"
        }
        


###luri.looksPartitioned()
returns true if the HDFS path can define a partition

