#!/bin/bash

orig_fn=$1
if [[ -z ${orig_fn} ]] || [[ ! -f ${orig_fn} ]];then
  echo "Cannot find input file ${orig_fn}"
  exit 1
fi

cp ${orig_fn} ${orig_fn}._original_
cat ${orig_fn}._original_ |\
perl -0pe "s/comment\s*=\s*\"[^\"]+\"//ig" |\
perl -0pe "s/(nth_value\([^\)]+\))\s*from\s*(first|last)/\$1/ig" \
> ${orig_fn}