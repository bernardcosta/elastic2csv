#!/bin/sh
# export .env files
export $(grep -v '^#' ../.env | xargs)
main(){
  QUERY_REQUEST=""
  ESDOMAIN=""
  INDEX=""
  SERVER=""
  OUTPUT="./out/exportedData.csv"


  OPTIND=1
  # Resetting OPTIND is necessary if getopts was used previously in the script.
  # It is a good idea to make OPTIND local if you process options in a function.
  while getopts "hq:o:i:d:s:" opt; do
         case $opt in
             h) usage
                exit 0 ;;
             q)  QUERY_REQUEST="$OPTARG" ;;
             o)  OUTPUT=$OPTARG ;;
             i)  INDEX=$OPTARG ;;
             d)  ESDOMAIN="$OPTARG" ;;
             s)  SERVER=$OPTARG ;;
             *)  usage >&2
                 exit 1 ;;
         esac
     done


  if [[ -z $QUERY_REQUEST  ||  -z $ESDOMAIN ]]
  then
    echo "ERROR: -q <query> and -d <hostname> are mandatory arguments. See usage: \n";
    usage;
  fi

  shift "$((OPTIND-1))"   # Discard the options and sentinel --

  # Port forward remote elasticsearch server
  ssh -f -N -q -L ${ESDOMAIN} ${SERVER}


  # run python script to extract data from elasticsearch
  #python3 main.py ${QUERY_REQUEST} ${OUTPUT}


  # run python script to extract data from elasticsearch
  #python3 main.py $2 $3

  # extract useful values from json to csv
  #cat tmp_dump.json | jq -r '["url","unique sessions","revenue", "total sessions"], (.[] | [.key.urls,.one.value,.three.value,.doc_count]) | @csv' > $3
  #cat tmp_dump.json | jq -r '.[] | [.key.split,.three.value,.one.value,.five.value,.six.value,.doc_count] | @csv' > $3

  #python3 ./core/cleanLPs.py

  # remove all unnecesary double quotes
  #sed -i '' 's/"//g' $3


  # Get all rows with <= 5 commas
  #grep -xE '([^,]*,){0,$3}[^,]*' $3 > tmp.csv
  #rm $3
  #mv tmp.csv $3

}

usage(){
cat << EOF
elastic2csv - automated process for exporting elastic queries to csv files

Usage: ${0##*/} [options] [-q QUERY] <json file> [-d DOMAIN] <hostname>

   -h                      Display this help and exit
   -q <json file>          Input json request file which contains elasticsearch query
   -o <output file>        File or dir to save the csv response
   -i <index name>         Elasticsearch index name or pattern to search from
   -d <hostname>           The hostname of the elasticserach SERVER
   -s <server connection>  Remote server name,port, user where elasticsearch is hosted on
EOF
}

main "$@"; exit
