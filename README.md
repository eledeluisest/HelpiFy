# Helpify

## Contents.
- data/: Source Data for Data Simulation
 and data structure for dashboard creation.
- 1_data_to_cloudant.py: Transformation from .csv to .json that
we will use to feed manualy our cloudant.
- 1_1_DataSimulation.ipynb: Data generation to feed the cloudant
 as an emergency was taking place.
- 2_statistics.py: Data Processing to load a DB2 database.
- demo_architecture.pptx: Shows PoC conceptual architecture
 and describes next step real-time architechture on IBM cloud
## Conections and Sources.
- 1_data_to_cloudant.py: .csv files in data/ directory.
- 1_1_DataSimulation.ipynb: Connection to cloudant.
- 2_statistics.py: Connection to cloudant and DB2.

## Dependencies & versions.
- 1_data_to_cloudant.py: Anaconda3. Python 3.6.4
- 1_1_DataSimulation.ipynb: IBM Watson Python Notebook.
 Python 3.6  + Spark 2
- 2_statistics.py: Anaconda3. Python 3.6.4  
> Any of the .py files work incrusted in .ipynb notebooks of IBM Watson Studio. 
Anyway given processing time limitations we used .py run locally
## Dashboard
- 2_statistics.py: Populates a DB2 that is the source for the following
cognos dashboard: https://eu-gb.dataplatform.cloud.ibm.com/dashboards/f090b7a0-3b4d-4c29-a072-6983b07e65b5/view/5b1fd40238b232cc50c3d4e407cd7d042e622d55bbbbd00484847b490a652397a8381097c87e4f5c88195366f3bf1a5fcc
