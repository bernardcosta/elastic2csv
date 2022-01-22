docker compose build python
docker compose up python

# extract useful values from json to csv
cat elastic2csv/tmp_dump.json | jq -r '.[] | [.key.urls,.doc_count,.three.value] | @csv' > output.csv
