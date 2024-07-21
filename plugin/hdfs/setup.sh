# shellcheck disable=SC2148
# Copyright (c) Facebook, Inc. and its affiliates. All Rights Reserved.
export JAVA_HOME="/usr/lib/jvm/java-11-openjdk-arm64/"
export HADOOP_HOME="/home/yzk/hadoop-3.4.0"
export LD_LIBRARY_PATH=$LD_LIBRARY_PATH:$JAVA_HOME/lib/server:$JAVA_HOME/lib:$HADOOP_HOME/lib/native

export CLASSPATH=`$HADOOP_HOME/bin/hadoop classpath --glob`
for f in `find $HADOOP_HOME/share/hadoop/hdfs | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
for f in `find $HADOOP_HOME/share/hadoop | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
for f in `find $HADOOP_HOME/share/hadoop/client | grep jar`; do export CLASSPATH=$CLASSPATH:$f; done
