# list all the file in hadoop or hdfs
FILE_LIST=`hadoop fs -ls -R ${HDFS_BASE_PATH}/abc/* |rev|cut -d '/' -f1-2|rev`

# deleting all files in a directory from HDFS or hadoop file system
hadoop fs -rm -r -skipTrash  ${HDFS_BASE_PATH}/${schema_name}/${table_name}/${part_name}=${partition}

# deleting specific file from HDFS or Hadoop
hadoop fs -rm -skipTrash  ${HDFS_BASE_PATH}/${schema_name}/${table_name}/${part_name}=${partition}/abc.txt

# to delete files from another cluster of HDFS or hadoop
ip_addr="111.222.333.444"
hadoop fs -Dfs.default.name=${ip_addr} -rm -r -skipTrash  ${ip_addr}${HDFS_BASE_PATH}/${schema_name}/${table_name}/${part_name}=${partition}

# copying files or directory from one cluster to another cluster
JOBQUEUE="default"
ip_addr="111.222.333.444"
hadoop distcp -Dmapreduce.job.queuename=${JOBQUEUE} 
${HDFS_BASE_PATH}/${schema_name}/${table_name}/${part_name}=${partition} ${ip_addr}${HDFS_BASE_PATH}/${schema_name}/${table_name}/

# making a directory in HDFS or hadoop
hadoop fs -mkdir -p ${HDFS_BASE_PATH}/${SCHEMA}/abc

# copying files within HDFS
hadoop fs -cp -f ${HDFS_SOURCE_PATH}/${SCHEMA}/${partition_col}=${partition} ${HDFS_DSTN_PATH}/${SCHEMA}/"

# copy to loacl unix path
hdfs dfs -copyToLocal ${HDFS_SOURCE_PATH}/${SCHEMA}/${partition_col}=${partition}/abc.txt ${UNIX_PATH}/${SCHEMA}/

# copy from unic to hdfs
hdfs dfs -copyFromLocal ${UNIX_PATH}/${SCHEMA}/abc.txt ${HDFS_PATH}/${SCHEMA}/

# reading sequence file in text format
hdfs dfs -text ${UNIX_PATH}/${SCHEMA}/part-00001

# find row count of hdfs file
hdfs dfs -cat ${UNIX_PATH}/${SCHEMA}/part-00001 | wc -l

