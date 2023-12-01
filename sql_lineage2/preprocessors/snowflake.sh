#!/bin/bash

# This script pre-processes known Snowflake SQL to make batch parsing more standard
# e.g. Snowflake official doc says comments can only be double quotes, some versions
# also supported single quotes
# Test cases under test_cases/parsing_only/snowflake
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