#!/bin/sh
wget=/usr/bin/wget
CURRENT_DIR= $PWD
echo "pwd is - "
echo $PWD
SCRIPTS_DIR=$PWD"/Scripts"
START="$(date --date="2 days ago" +%F-%H-%M-%S)"
END="$(date --date="1 day ago" +%F-%H-%M-%S)"
echo "Script dir is  --------"
echo $SCRIPTS_DIR
cd $SCRIPTS_DIR
echo "start time and end time is -"
echo $START
echo $END

python getRequests.py $START $END
python topAsn.py
python getUrls.py $START $END

while IFS='\n' read path;do
    echo "My path var is === "
    echo $path
    $wget $path
    filename=$(basename $path .bz2)
    ext=".bz2"
    bzip2 -d $filename$ext
    python parseUpdate.py $filename

done < urlFile
rm -rf update*
python write_to_csv.py $START
python write_to_json.py $START
mv FinalStability FinalStability_$START
