START="$(date --date="2 days ago" +%F-%H-%M-%S)"
END="$(date --date="1 day ago" +%F-%H-%M-%S)"
echo "In startup"
cd "/home/mthatte/refactored_routeflow-bgp-analysis/routeflow-bgp-analysis/Data/Scripts/"
echo "pwd is -"
echo $PWD 
echo $START
echo $END
python extract_data.py $START $END
