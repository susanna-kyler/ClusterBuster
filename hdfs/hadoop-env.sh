#!/bin/bash
export JAVA_HOME=/usr/local/jdk1.8.0_51
export HADOOP_HOME=/usr/local/hadoop
export HADOOP_COMMON_HOME=${HADOOP_HOME}
export HADOOP_HDFS_HOME=${HADOOP_HOME}
export HADOOP_MAPRED_HOME=${HADOOP_HOME}
export YARN_HOME=${HADOOP_HOME}
export YARN_CONF_DIR=${HADOOP_CONF_DIR}
export HADOOP_LOG_DIR=/s/${HOSTNAME}/a/tmp/${USER}/hadoop-logs
export YARN_LOG_DIR=/s/${HOSTNAME}/a/tmp/${USER}/yarn-logs

export HADOOP_OPTS="-Dhadoop.tmp.dir=/s/${HOSTNAME}/a/nobackup/cs455/${USER}"
