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
bgp-report-run [path to config.json file] [START_TIME] [END_TIME]
```

Example commands - 
```
bgp-report-run /home/mthatte 2017-11-15-16-58-39 2017-11-15-17-19-39 
```
Above command runs the script with 'config.json' file at location '/home/mthatte' and the passed datetime parameters

```
bgp-report-run 2017-11-15-16-58-39 2017-11-15-17-19-39
```
Above command runs the script with passed datetime parameters

```
bgp-report-run /home/mthatte
```
Above command takes the location of 'config.json' files as an argument and runs the script with default datetime parameters = current_date - 2 and current_date - 1

```
bgp-report-run
```
When no parameters are specified, the script will look for 'config.json' file in the current directory and run the script with default datetime parameters.

## Running as a python Script - 
navigate to parent directory - /routeflow-bgp-analysis

run the python script test_pip.py as -

```
sudo python test_pip.py
```

Route view events data will be written to files - **Analysis.csv** and **Analysis.json**

File - **status.json** logs status of current execution of test_pip script.
