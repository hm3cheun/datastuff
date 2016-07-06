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

javac -cp $CLASSPATH FileWriteToHDFS.java
java -cp $CLASSPATH FileWriteToHDFS /root/Reddit/top25 /tmp/data/Reddit/top25
java -cp $CLASSPATH FileWriteToHDFS /root/Reddit/top25_1 /tmp/data/Reddit/top25_1
java -cp $CLASSPATH FileWriteToHDFS /root/Reddit/top25_2 /tmp/data/Reddit/top25_2
java -cp $CLASSPATH FileWriteToHDFS /root/Reddit/top25_3 /tmp/data/Reddit/top25_3
java -cp $CLASSPATH FileWriteToHDFS /root/4square/4square_checkin /tmp/data/4square/4square_checkin
java -cp $CLASSPATH FileWriteToHDFS /root/4square/4sq_cities /tmp/data/4square/4sq_cities
java -cp $CLASSPATH FileWriteToHDFS /root/4square/4sq_poi /tmp/data/4square/4sq_poi
java -cp $CLASSPATH FileWriteToHDFS /root/4square/4square_checkin_1 /tmp/data/4square/4square_checkin_1
java -cp $CLASSPATH FileWriteToHDFS /root/4square/4square_checkin_2 /tmp/data/4square/4square_checkin_2
java -cp $CLASSPATH FileWriteToHDFS /root/4square/4square_checkin_3 /tmp/data/4square/4square_checkin_3
java -cp $CLASSPATH FileWriteToHDFS /root/Wikipedia/wiki_clickstream_temp1 /tmp/data/Wikipedia/wiki_clickstream_temp1


