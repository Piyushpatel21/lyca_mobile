# lycamobile-etl-movements

This repository contains all the scripts used to create the database objects, load data into different layers of datamart and queries to generate the business reports.
The repository is divided into folders as mentioned below:

 **redshift_admin_scripts:**
    This folder contains scripts to create Redshift users, groups, databases, schemas and grant the necessary permissions.

**stage_scripts:**
    This folder contains scripts to create stg tables, copy commands to load data into stage from s3 bucket etc.
    
**datamart_scripts:** 
    This folder contains scripts to create datamart tables, load data into the datamart and queries to generate reports.
    This folder has subfolders:
    
*  Procedure Scripts: Contains the procedure scripts to load data into the datamarts.
*  Table_Creation_scripts: Contains the table definition scripts in the datamart.
*  Dimension_load_scripts: Contains scripts to load the dimension tables in datamart.
*  reporting_scripts: Contains queries used for reporting.
    

