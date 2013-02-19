#!/usr/bin/env bash

CURRDIR="`pwd`"
SCRIPTDIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"
#defaults
ARGS="jar ../WorkGenYarn.jar org.apache.hadoop.examples.WorkGenYarn -conf ../workGenKeyValue_conf.xsl -libjars ../WorkGenYarn.jar"
BINDIR="../../bin"
OUTDIR="workGenLogs"

#parse args
arg_type="none"

while (( "$#" )); do
  case $1 in
    --args)
      arg_type="args"
      ARGS=""
      ;;
    --bindir)
      arg_type="bindir"
      ;;
    --outdir)
      arg_type="outdir"
      ;;
    *)
      if [ "$arg_type" = "args" ] ; then
        ARGS="$ARGS $1"
      elif [ "$arg_type" = "bindir" ] ; then
        BINDIR="$1"
      elif [ "$arg_type" = "outdir" ] ; then
        OUTDIR="$1"
      else
        echo "Unrecognized $arg_type"
      fi
      ;;
  esac
  shift
done

#run
if [ -d "$OUTDIR" ]; then rm -r "$OUTDIR"; fi
mkdir "$OUTDIR"

cd $SCRIPTDIR
./run-jobs-script.sh \
 --bindir $BINDIR --outdir $OUTDIR \
 --args $ARGS \
 --redratio-distribution uniform --suspend-strategy shortest \
 --hdfs-input workGenInputSmall \
 --interval 1 --duration 8 \
 --verbose true \
 --hdfs-input-num 100 \
 --sleep 0 \
 --queue low --deadline 40 --mapratio 0.00 --redratio 0.00 --nummaps 1 --numreduces 1 \
 --jobs 0\
 --sleep 10 \
 --queue low --deadline 30 --mapratio 0.06 --redratio 0.02 --nummaps 1 --numreduces 1 \
 --jobs 1\
 --sleep 0 \
 --queue low --deadline 20--mapratio 0.01 --redratio 0.00 --nummaps 1 --numreduces 1 \
 --jobs 2\
 --sleep 0 \
 --queue low --deadline 10 --mapratio 0.10 --redratio 0.35 --nummaps 1 --numreduces 1 \
 --jobs 3\
 --sleep 0 \

cd $CURRDIR
