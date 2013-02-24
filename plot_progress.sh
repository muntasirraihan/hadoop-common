#!/bin/bash
clear

VERSION=0.23.3

#HADOOP_SOURCE=/proj/ISS/scheduling
HADOOP_SOURCE="$HOME/natjam/hadoop-common"
JOB_OUTPUT_DIR=$HADOOP_SOURCE/workload/scripts/workGenLogs

echo $JOB_OUTPUT_DIR

rm -rf $JOB_OUTPUT_DIR$/progress*

FILE_COUNT=`ls -1 $JOB_OUTPUT_DIR | wc -l`

echo $FILE_COUNT


#rm -rf $JOB_OUTPUT_DIR$/progress-plot-job-*.txt

for (( i = 0; i < $FILE_COUNT; i++ ))
do
    #rm -rf $JOB_OUTPUT_DIR/progress-plot-job-*.txt
    cat $JOB_OUTPUT_DIR/job-$i.txt | grep 'INFO mapreduce.Job:  map' | awk '{printf "%s\t%s\t%s\n", $2, $6, $8}' | sed "s/%//g" > $JOB_OUTPUT_DIR/progress-plot-job-$i.txt
done

touch $JOB_OUTPUT_DIR/progress.plt

echo "set terminal png nocrop medium size 640,480" >> $JOB_OUTPUT_DIR/progress.plt
echo "set output '$JOB_OUTPUT_DIR/progress.png'" >> $JOB_OUTPUT_DIR/progress.plt
echo "set xdata time" >> $JOB_OUTPUT_DIR/progress.plt
echo "set autoscale" >> $JOB_OUTPUT_DIR/progress.plt
echo "set key right bottom" >> $JOB_OUTPUT_DIR/progress.plt
echo "set timefmt \"%H:%M:%S\"" >> $JOB_OUTPUT_DIR/progress.plt
#echo "plot '$JOB_OUT" >> $JOB_OUTPUT_DIR/progress.plt


GNUPLOT_COMMAND="plot '$JOB_OUTPUT_DIR/progress-plot-job-0.txt' using 1:2 title 'j$i m' with linespoints, '$JOB_OUTPUT_DIR/progress-plot-job-0.txt' using 1:3 title 'j0 r' with linespoints"

for ((i = 1; i < $FILE_COUNT; i++ ))
do
    GNUPLOT_COMMAND=$GNUPLOT_COMMAND", '$JOB_OUTPUT_DIR/progress-plot-job-$i.txt' using 1:2 title 'j$i m' with linespoints, '$JOB_OUTPUT_DIR/progress-plot-job-$i.txt' using 1:3 title 'j$i r' with linespoints" >> $JOB_OUTPUT_DIR/progress.plt
done

echo $GNUPLOT_COMMAND >> $JOB_OUTPUT_DIR/progress.plt

gnuplot $JOB_OUTPUT_DIR/progress.plt
