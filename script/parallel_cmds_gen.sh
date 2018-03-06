#!/bin/bash
PYCMD=$1
SCRIPT=$2
INDIR=$3
OUTDIR=$4
LOOKUP=$5
NUMPROC=$6

COUNTER=0
for f in $INDIR*; do
  if [[ -d $f ]]; then
    # $f is a directory
    #echo $f

    if [ "$COUNTER" == "$NUMPROC" ]
    then
      break
    fi

    dirname=${f#$INDIR};
    proc_outdir=$OUTDIR$dirname;

    #%s %s -in %s -out %s -lookup %s
    echo $PYCMD $SCRIPT -in $f -out $proc_outdir -lookup $LOOKUP
    let COUNTER=COUNTER+1
  fi
done
