#!/bin/bash

# Run Python scripts one by one
sleep 30 #to wait for kafka broker and couchserver to be up and running
python -u ./app/producer.py &
# seq 4 | parallel --linebuffer -j 4 python -u ./app/changes_consumer.py &
wait