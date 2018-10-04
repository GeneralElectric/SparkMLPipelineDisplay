SparkMLPipelineDisplay
# Spark ML Pipeline Display Function
----------------------------------

This function was originally created by Peter Knight from GE Aviaiton. 
It is designed to create an interactive Bokeh plot of Spark ML Pipelines. 
It has an on hover display that shows the Parameters and values for each stage.

The repository contains code and example notebooks (each of which builds the example pipline from https://spark.apache.org/docs/2.2.0/ml-pipeline.html and displays them):
* display_pipeline.py - python file defining the function. it has code for both Zepplin and Databricks so needs editing befure use
* Databricks Pipeline Display.ipynb - Databricks Jupyter Notebook
* Databricks Pipeline Display Demo Export.html - An export of the Databricks Jupyter Notebook to a html file
* Zeppelin ML Pipeline Display Demo.json - Zeppelin Notebook example
