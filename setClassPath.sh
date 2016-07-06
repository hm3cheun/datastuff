#!/bin/bash
HADOOP_HOME=/usr/lib/hadoop
HIVE_HOME=/usr/lib/hive
 
echo -e '1\x01foo' > /tmp/a.txt
echo -e '2\x01bar' >> /tmp/a.txt
 
HADOOP_CORE=$(ls $HADOOP_HOME/hadoop-common*.jar)
CLASSPATH=.:$HIVE_HOME/conf:$(hadoop classpath)
 
for i in ${HIVE_HOME}/lib/*.jar ; do
    CLASSPATH=$CLASSPATH:$i
done

javac -cp $CLASSPATH HiveJdbcClient.java
java -cp $CLASSPATH HiveJdbcClient

