#!/bin/sh

if [ ! -f "$DATA_FOLDER/complete_data.csv.zip" ]
then
    kaggle datasets download muhammadkaleemullah/imdb-data -f complete_data.csv -p $DATA_FOLDER
else
    echo "Data file already exist."
fi
