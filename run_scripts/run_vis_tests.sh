#!/bin/sh

LOCALCLASSPATH=`/bin/sh $PWD/classpath.sh run`

java -cp $LOCALCLASSPATH edu.indiana.d2i.seqmining.test.VisTester
