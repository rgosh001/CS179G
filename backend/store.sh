#!/bin/bash

if [ "$#" -ne 1 ]; then
    echo "Proper usage: ./submit.sh [name]"
    exit 1;
fi

./submit.sh  "setup/"$1".py" "csv/"$1".csv"
