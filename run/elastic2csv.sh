#!/bin/bash
domain(){
  if [ "$1" = "bi" ]; then
    echo ${BI};
  elif [ "$1" = "search" ]; then
    echo ${SEARCH};
  elif [ "$1" = "ARCHIVE" ]; then
    echo ${ARCHIVE}
  else echo $1;
  fi
}

usage(){
cat << EOF
elastic2csv - automated process for exporting elastic queries to csv files

Usage: ${0##*/} [options] [-q QUERY] <json file> [-d DOMAIN] <hostname>

   -h                      Display this help and exit
   -q <json file>          Input json request file which contains elasticsearch query
   -c <integer>            No of columns to show on output. This script will grab the first n keys found in query json
   -d <hostname>           The hostname of the elasticserach SERVER
   -o <output file>        File or dir to save the csv response
   -i <index name>         Elasticsearch index name or pattern to search from
   -s <server connection>  Remote server name,port, user where elasticsearch is hosted on

EOF
}

check_mandatory_fields(){
  if [[ -z $QUERY_REQUEST  ||  -z $ESDOMAIN || -z $TOTAL_COLS ]]
  then
    echo "ERROR: -q <query> and -d <hostname> -c <columns> are mandatory arguments. See usage: \n";
    usage;
    exit 1;
  fi
}

# export .env files
export $(grep -v '^#' ../.env | xargs)

QUERY_REQUEST=""
ESDOMAIN=""
OUTPUT="./out/exportedData.csv"
INDEX=""
TOTAL_COLS=""
OPTIND=1
# Resetting OPTIND is necessary if getopts was used previously in the script.
# It is a good idea to make OPTIND local if you process options in a function.
while getopts "hq:o:i:d:s:c:" opt; do
       case $opt in
           h) usage
              exit 0 ;;
           q)  QUERY_REQUEST=$OPTARG ;;
           o)  OUTPUT=$OPTARG ;;
           i)  INDEX=$OPTARG ;;
           d)  ESDOMAIN=$OPTARG ;;
           s)  SERVER=$OPTARG ;;
           c)  TOTAL_COLS=$OPTARG ;;
           *)  usage >&2
               exit 1 ;;
       esac
   done


check_mandatory_fields;

shift "$((OPTIND-1))"   # Discard the options and sentinel --

# Port forward remote elasticsearch server
echo ${ESDOMAIN}
echo $( domain ${ESDOMAIN} )

# kill ssh tunnel already using this port
kill $( ps aux | grep '[9]201:' | awk '{print $2}' )
ssh -f -N -q -L "9201:"$( domain ${ESDOMAIN} ) ${SERVER}

#run python script to extract data from elasticsearch
python3 main.py ${QUERY_REQUEST} ${INDEX}

COLUMNS=$(cat ${QUERY_REQUEST} | jq --arg v $TOTAL_COLS '.aggs.two.aggs | keys_unsorted | .[:$v|tonumber] | ["\(.[:$v|tonumber] | .[]).value"] |.[:$v|tonumber] |= . + ["doc_count"] | .[:0] |= . + ["key.split"] | [ .[] | split(".")]')
# extract useful values from json to csv
cat tmp_dump.json | jq --argjson v "$COLUMNS" '.[] | [ getpath( $v[]) ]'
#cat tmp_dump.json | jq -r '["url","unique sessions","revenue", "total sessions"], (.[] | [.key.urls,.one.value,.three.value,.doc_count]) | @csv' > $3
#cat tmp_dump.json | jq -r '.[] | [.key.split,.three.value,.one.value,.five.value,.six.value,.doc_count] | @csv' > $3

#python3 ./core/cleanLPs.py

# remove all unnecesary double quotes
#sed -i '' 's/"//g' $3


# Get all rows with <= 5 commas
#grep -xE '([^,]*,){0,$3}[^,]*' $3 > tmp.csv
#rm $3
#mv tmp.csv $3
