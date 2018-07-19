# fsimage-analyzer

These scripts are based on code written by Peter Ebert for the fsimage analyzer

The script has been migrated to Pyspark with features to create HDFS directories and tables through the shell script to make it convenient to track fsimage periodically

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

    HDFS_PATH="/tmp/fsimage/" - This should be same as the HDFS_PATH in Step1_FetchFsimage.sh, will add a common file in the       next version
    dbName = "default"
    tblName = "fsimage_tbl"

3) Run: sh Step1_FetchFsimage.sh
