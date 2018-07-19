from pyspark.sql.types import ArrayType
from pyspark.sql.types import StringType
from pyspark.sql.functions import udf
from pyspark.sql.functions import explode
from pyspark.sql import SparkSession
import datetime

spark=SparkSession.builder.getOrCreate()
curr_date = str(datetime.date.today())

############ Config ############
# This should be the HDFS_PATH location set in the first script
HDFS_PATH="/tmp/fsimage/"

# The database name
dbName = "default"
tblName = "fsimage_tbl"
################################

tsvFilePath = HDFS_PATH + "fsimage_"+curr_date+".tsv"
tsvDF = spark.read.option("header","true").csv(tsvFilePath,sep='\t')
tsvDF = tsvDF.select("Path", "Replication", "PreferredBlockSize", "BlocksCount", "FileSize").filter("BlocksCount!=0")

# Split the paths such that eg: /tmp/tables/tbl1 is split to /, /tmp, /tmp/tables, /tmp/tables/tbl1
def splitPaths(str):
  index= 1
  paths = []
  while (index > 0):
    paths.append(str[:index])
    index = str.find("/", index+1)
  return paths
  
splitPathsUDF = udf(splitPaths, ArrayType(StringType(),False))
explodedPaths = tsvDF.withColumn("Path", explode(splitPathsUDF(tsvDF["Path"])))
explodedPaths.createOrReplaceTempView("explodedpaths")

smallBlocksListDF = spark.sql("SELECT Path, sum(FileSize)/sum(BlocksCount)/1048576 as avgblocksize, sum(FileSize)/1048576 as TotalSize, sum(BlocksCount) as totalblocks, " + 
  " sum(FileSize)/avg(PreferredBlockSize) as idealblocks, " +
  " sum(BlocksCount)-sum(FileSize)/avg(PreferredBlockSize) as blockreduction, " +
  " cast(current_date as string) as extract_dt " +
  " from explodedpaths GROUP BY path ORDER BY blockreduction DESC")
  
# Filter out paths that you would like to be excluded from the final table (eg: Oozie, tmp, solr, hive warehouse, etc)
filteredPaths = smallBlocksListDF.filter("path not like '/user/oozie%'").filter("path not like '/solr%'").filter("path not like '/hbase%'").filter("path not like '/tmp%'").filter("path not like '/user/hive/warehouse%'")

filteredPaths.repartition(1).write.mode('append').format('parquet').saveAsTable(dbName+"."+tblName, partitionBy='extract_dt', compression = 'snappy')
