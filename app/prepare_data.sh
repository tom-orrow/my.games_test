#!/bin/sh

if [ ! -f "$DATA_FOLDER/complete_data.csv" ]
then
    kaggle datasets download muhammadkaleemullah/imdb-data -f complete_data.csv -p $DATA_FOLDER
    unzip $DATA_FOLDER/complete_data.csv -d $DATA_FOLDER
else
    echo "Data file already exist."
fi