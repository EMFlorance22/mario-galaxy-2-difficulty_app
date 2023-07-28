# mario-galaxy-2-difficulty_app
Personal project to predict difficulties of Mario Galaxy 2 Levels. Collection, processing, and machine learning code of data made from scratch. Steps are below:

1. Collected all the data for the levels on one Microsoft Excel sheet
2. Used PL/SQL to create separate tables from the single Excel sheet (Stars, Bosses, Enemies, Traps, Powerups, Techniques) and populated those tables using PL/SQL Procedures and Loops
3. Exported the filled PLSQL/Oracle tables into separate CSV files
4. Created MySQL tables and loaded data in the separate CSV files into the tables
5. Used SQL to perform some basic EDA on the data now in the appropriate tables then performed more complex EDA by connecting to the MySQL Database in Python
6. Loaded the CSV data into Amazon S3
7. Connected the S3 data to the Glue Data Catalog in order to perform ETL using Glue
8. Used PySpark to perform ETL on the data on-premises and in the cloud using S3 and Glue
9. Saved the processed data into another S3 bucket then copied it over to a Redshift database via the COPY cmd
10. Connected the Redshift Database to Jupyter Notebook to use Python's sklearn library to predict the difficulty of Mario Galaxy 2 levels using ML
11. Connected the Redshift Database to PowerBI to build a basic dashboard of level data

Analysis showed that there was not enough data in order to accurately predict difficulty using any ML technique (although KNN was best) --> will use Mario Galaxy 1 data as well
EDA visualized important factors that correlate with difficulty such as enemy, trap, and powerup data as well as if the level was a prankster comet or boss level

Overall this was a successful data lifecycle project using PL/SQL, Python, Amazon Glue, Amazon Redshift, and PowerBI to explore a seemingly complex dataset
    Manual Data Collection --> EDA using SQL and Python --> Data storage using S3 and MySQL --> ETL/Data Engineering with PySpark and Glue --> 
    Data Warehouse storing of processed data with Redshift --> Analysis/ML and prediction with Python --> Report/Dashboard building with PowerBI
