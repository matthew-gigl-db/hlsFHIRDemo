# How to Set Up the FHIR Workshop
***

1. Turn on all previews
1. Change the display name of the workspace to "<customer's name> FHIR Ingestion Workshop"
1. Turn on the following developer settings:
    - New Charts
    - Tabbed Notebooks
    - New ETL Multi File Editor 
    - Open Queries in new SQL Editor 
1. Link your GitHub Account (Optional)
1. Update the host URL in the databricks.yml for the course workspace
1. Update other workspace target variables as necessary
1. Use DAB in the Workspace interface to deploy the workflows
1. Execute the following in order:  
    - "Workshop Lab Workspace Setup"
    - "Run Synthea on DBX" 
    - File Arrival Trigger Will Kickoff "Synthea FHIR Ingestion" 
1. Turn on Data Classification for the fhir_workshop catalog
1. Turn on Anomaly Detection for the synthea schema
1. Download DBPriorAuthExample.json from ~/fixtures
1. Upload DBPriorAuthExample.json to /Volumes/fhir_workshop/synthea/synthetic_files_raw/output/fhir/

Optional: 
1. Run "Run Synthea on DBX for the synthea_65K schema with a "run now with different parameters", also changing the max records to 66000 and the min records to 65000
    - Then run the "Synthea FHIR Ingestion" with a run now with the schema set to synthea_65.  
1. Set up Mosaic Model Serving and SQL UDF for Redox API
