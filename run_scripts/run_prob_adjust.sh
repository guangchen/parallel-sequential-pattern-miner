#!/bin/sh

if [ $# -ne 4 ]; then
    echo "Usage: <binary file of frequent patterns> <num map tasks> <partition file> <property file>"
    exit -1
fi

cp=$TWISTER_HOME/bin:.

for i in ${TWISTER_HOME}/lib/*.jar; do
	cp=$i:${cp}
done

for i in ${TWISTER_HOME}/apps/*.jar; do
	cp=$i:${cp}
done

java -Xmx1024m -Xms512m -XX:SurvivorRatio=10 -classpath $cp edu.indiana.d2i.seqmining.adjust.ProbAdjustDriver $1 $2 $3 $4
