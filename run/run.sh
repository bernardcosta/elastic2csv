python3 ./core/compositeexport.py $1
# extract useful values from json to csv
cat elasticexport/tmp_dump.json | jq -r '.[] | [.key.urls,.doc_count,.three.value] | @csv' > $2
