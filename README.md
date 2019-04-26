# ClusterBuster
Cs455 Term project


## Submitting a Job
You do not need to modify your Spark configuration to submit a job. Submitting a job can be done as normal, but use honolulu:58128 as the master-url.

The Spark UI is available at [honolulu:58129].

## HDFS
Interfacing with HDFS is only necessary if you would like to directly execute filesystem commands.  The Spark cluster is already configured to use it.  To interface with HDFS you'll need to set your `HADOOP_CONF_DIR` to the hdfs dir in this repository.  After than you can issue commands as usual, using `$HADOOP_HOME/bin/hadoop fs`.

The HDFS UI is available at [honolulu:50128].
