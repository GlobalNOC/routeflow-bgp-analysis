# routeflow-bgp-analysis
Uses Route Views data to determine routing events for top talkers for IRNC networks

## Getting started

Build the project.

Install bgpReport_source library - 

```
cd bgpReport_source
sudo pip install .
```
## How to run the script - 

## Running through command line - 
```
bgp-report-run 
```
Typing this command will start the script with optional datetime parameters and config file location


```
bgp-report-run [START_TIME] [END_TIME] [path to config.json file]
```

Example command - 
```
bgp-report-run 2017-11-15-16-58-39 2017-11-15-17-19-39 /home/mthatte
```

## Running as a python Script - 
navigate to parent directory - /routeflow-bgp-analysis

run the python script test_pip.py as -

```
sudo python test_pip.py
```

Route view events data will be written to files - **Analysis.csv** and **Analysis.json**

File - **status.json** logs status of current execution of test_pip script.
