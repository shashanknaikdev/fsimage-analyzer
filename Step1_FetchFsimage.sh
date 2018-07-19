############ Config ############

# Adjust the client jvm heap
export HADOOP_CLIENT_OPTS="-Xmx10240m"
export HADOOP_OPTS="-Xmx10240m"

# Set the HDFS_PATH to copy the text format fsimage to
HDFS_PATH="/tmp/fsimage/"

# Adjust Spark Driver and Executor memory
SPARK_DRIVER_MEM="4G"
SPARK_EXECUTOR_MEM="6G"

################################


DATE=`date +"%Y-%m-%d"`

# You will need to run the dfsadmin commands as an hdfs admin:
hdfs dfsadmin -rollEdits
hdfs dfsadmin -fetchImage ./fsimage_$DATE
hdfs oiv -i ./fsimage_$DATE -o ./fsimage_$DATE.tsv -p Delimited

# Then put the converted fsimage onto the HDFS_PATH
hdfs dfs -put -f fsimage_$DATE.tsv $HDFS_PATH

rm -f ./fsimage_$DATE
# rm -f ./fsimage_$DATE.tsv

# Specify/modify any additional options i.e. --num-executors, --executor-cores, --driver-memory, --driver-cores, etc
spark2-submit --executor-memory $SPARK_EXECUTOR_MEM --driver-memory $SPARK_DRIVER_MEM 2_fsImagePyspark.py
