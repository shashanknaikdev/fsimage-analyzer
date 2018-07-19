# fsimage-analyzer

These scripts are based on code written by **Peter Ebert** for the fsimage analyzer

The script has been migrated to Pyspark with features to create HDFS directories and tables through the shell script to make it convenient to track fsimage periodically.

*Note: This requires HDFS admin priviledges*

The **Step1_FetchFsimage.sh** shell script will roll the edits log, and fetch the latest fsimage to the node the script is being run on. It then uses the OIV tool to convert the fsimage into a text delimited format and moves it to the HDFS path defined.

The **Step2_FsimagePyspark.py** file is a PySpark script that reads the text format fsimage and loads it into a DataFrame. Only the features "Path", "Replication", "PreferredBlockSize", "BlocksCount" and "FileSize" are used.

The UDF splitPaths(str) processes each Path in the DataFrame and splits the strings such that */tmp/tables/tbl1* is split to */, /tmp, /tmp/tables, /tmp/tables/tbl1*

Once the paths are split, we generate the columns TotalSize as sum(FileSize), totalblocks as sum(BlocksCount), avgblocksize as sum(FileSize)/sum(BlocksCount), idealblocks as sum(FileSize)/avg(PreferredBlockSize), blockreduction as sum(BlocksCount)-sum(FileSize)/avg(PreferredBlockSize). A new field extract_dt is added which is used as the partitioning column, the date is automatically fetched as the current date.

**TotalSize**: Total Filesize at the Path location

**totalblocks**: Total Number of blocks at the Path location

**avgblocksize**: Average block size at the Path location

**idealblocks**: The ideal number of blocks that at the Path location in best case scenario

**blockreduction**: The potential for block reduction at the Path location, higher means more small files

In the next step, it filters out paths that you would like to be excluded from the final table (eg: Oozie, tmp, solr, hive warehouse, etc) - You can change this if needed.

### Usage:

1) Keep both files in the same directory on an edge node

2) Change configuration settings, if needed:

  In **Step1_FetchFsimage.sh**
    
    HADOOP_CLIENT_OPTS="-Xmx10240m"
    HADOOP_OPTS="-Xmx10240m"
    HDFS_PATH="/tmp/fsimage/"
    SPARK_DRIVER_MEM="4G"
    SPARK_EXECUTOR_MEM="6G"

  In **Step2_FsimagePyspark.py**

    HDFS_PATH="/tmp/fsimage/" - This should be same as the HDFS_PATH in Step1_FetchFsimage.sh, will add a common config file in the next version
    dbName = "default"
    tblName = "fsimage_tbl"

3) Run: sh Step1_FetchFsimage.sh

### Analysis:

Once the fsimage is loaded in the table you can run queries like:

    select path,
    round(avgblocksize,2) as avgblocksize_MB,
    round(TotalSize,2) as sum_filesize_MB,
    totalblocks,
    round(idealblocks,2) as idealblocks,
    round(blockreduction,2) as blockreduction
    from fsimage_tbl
    where path like ("%/genericTablePath/%") 
          and LENGTH(regexp_replace(path,'[^/]',''))=5 
          and extract_dt='2018-05-10'
    order by blockreduction desc 
    limit 20;

This lets you drill down into path at different levels with the **LENGTH(regexp_replace(path,'[^/]',''))=5**. You can change the level of depth you want to check, even filtering for specific table locations in the where clause.
