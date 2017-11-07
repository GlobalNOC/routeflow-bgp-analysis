# routeflow-bgp-analysis
Uses Route Views data to determine routing events for top talkers for IRNC networks

## Getting started

Build the project.

Install bgpReport_source library - 

```
cd bgpReport_source
sudo pip install .
```

navigate to parent directory - /routeflow-bgp-analysis

run the python script 'test_pip.py' as -

```
sudo python test_pip.py
```

Route view events data will be written to files - **Analysis.csv** and **Analysis.json**

File - **status.json** logs status of current execution of test_pip script.
