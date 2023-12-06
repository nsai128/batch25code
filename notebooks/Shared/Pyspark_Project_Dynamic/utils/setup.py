# Databricks notebook source
# MAGIC %md
# MAGIC ### Step By Step process to execute this code in `DATABRICKS COMMUNITY EDITION`

# COMMAND ----------

# MAGIC %md
# MAGIC * __`STEP 1`__ :  Run below notebook for Creating __`/mnt/landing/sales`__ directory and Downloading datafiles from github.
# MAGIC * *  __`Script Location :`__ `/Shared/Pyspark_Project_Dynamic/utils/community_edition_files_setup`
# MAGIC * __`STEP 2`__ : Run below notebook for Creating Databases and tables for different stages 
# MAGIC * * __`Script Location : `__  `/Shared/Pyspark_Project_Dynamic/utils/database_ddl_scripts`
# MAGIC * * it will create below list of databases 
# MAGIC * *  * staging zone - `stg_sales`
# MAGIC * *  * curation zone - `curation_sales`
# MAGIC * *  * data warehouse zone - `dw_sales`
# MAGIC * *  * log database - `log_db`
# MAGIC * __`STEP 3`__ : Rune below notebook to execute End to End load in Databricks Community Edition. 
# MAGIC * * __`Script Location : `__  `/Shared/Pyspark_Project_Dynamic/NB_Final_Manual_Run`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Main Scripts information :
# MAGIC * __`Notebook 1: NB_sales_load_dynamic`__
# MAGIC * __`Single Notebook For Dynamic Load For All Dimensions `__  `/Shared/Pyspark_Project_Dynamic/NB_sales_load_dynamic`
# MAGIC *  Single notebook we an use for procesing all dimensions dynamically passing arguments. 
# MAGIC * * * `Customers`
# MAGIC * * * `product`
# MAGIC * * * `times`
# MAGIC * * * `channels`
# MAGIC * * * `promotions`
# MAGIC * * * `countries`
# MAGIC
# MAGIC * __`Notebook 2: NB_sales_fact_load`__
# MAGIC * __`This Notebook we are using for processing sales Fact table in all stages `__  `/Shared/Pyspark_Project_Dynamic/NB_sales_fact_load`
# MAGIC *  This notebook we are using for process Sales Fact Table in all databases. 
# MAGIC * * * `sales Transaction Fact`
# MAGIC * __`Notebook 3: NB_costs_fact_load`__
# MAGIC * __`This Notebook we are using for processing costs Fact table in all stages `__  `/Shared/Pyspark_Project_Dynamic/NB_costs_fact_load`
# MAGIC *  This notebook we are using for process costs Fact Table in all databases. 
# MAGIC * * * `costs Transaction Fact`
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC ### Common Utility Notebooks for Metadata , SQL DDL's and Functions
# MAGIC * __`SQL DDL's`__ : Notebook for all Databases creation and Tables creation scripts .`Shared/Pyspark_Project_Dynamic/utils/database_ddl_scripts`
# MAGIC * __`Python Functions`__ : Notebook for all common python reusable functions. `/Shared/Pyspark_Project_Dynamic/utils/functions`
# MAGIC * __`Pyspark Schema`__ : Notebook for all pyspark schema for all tables. `/Shared/Pyspark_Project_Dynamic/utils/pyspark_schema`

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### List of Databases
# MAGIC |List Databases| Comments |
# MAGIC |------ | --- |
# MAGIC |**stg_sales** | `Delta tables ` - staging tables - daily truncate load|
# MAGIC |**curation_sales** |`Delta tables ` -  curation zone - all historica data for advanced analytics team |
# MAGIC |**dw_sales** |`Delta tables ` - final target for storing dimension and fact tables |
# MAGIC |**log_db** | `Delta tables ` - storing logs and bad data | 
# MAGIC |**jobs** | `Delta table ` - storing ETL Job information | 
# MAGIC ### List of tables at each stage
# MAGIC
# MAGIC
# MAGIC Staging Tables | Curation Tables | Warehouse Tables
# MAGIC ---:|:---:| ---
# MAGIC **stg_customer** | curation_customer | _dim_customer_
# MAGIC **stg_product** | curation_product | _dim_product_
# MAGIC **stg_channels** | curation_channels | _dim_channels_
# MAGIC **stg_promotions** | curation_promotions | _dim_promotions_
# MAGIC **stg_times** | curation_times | _dim_times_
# MAGIC **stg_countries** | curation_countries | _dim_countries_
# MAGIC **stg_sales_transaction** | curation_sales_transactions | _fact_sales_transaction_
# MAGIC **stg_costs_transaction** | curation_costs_transactions | _fact_costs_transaction_
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC #### Creating `mount-point` to access datalake in azure databricks

# COMMAND ----------


dbutils.fs.mount(
  source = "wasbs://<container-name>@<storage-account-name>.blob.core.windows.net",
  mount_point = "/mnt/sales",
  extra_configs = {"fs.azure.account.key.<storage-account-name>.blob.core.windows.net":dbutils.secrets.get(scope = "<scope-name>", key = "<key-name>")})

# COMMAND ----------

dbutils.secrets.get(scope="batch19-adlsgen2-scope",key="batch19-adlsgen2-account-key")

# COMMAND ----------

# HDFS /  DBFS / GFS / DFS / WASBS - windows azure storage blob service / AWS S3  - simple storage Service / DBFS (Databricks file system)
dbutils.fs.mount(
  source = "wasbs://datalake@adlsgen2batch25.blob.core.windows.net/",
  mount_point = "/mnt/datalake/",
  extra_configs = {"fs.azure.account.key.adlsgen2batch25.blob.core.windows.net":"kgepxiehrOjWQl67yFptnVifh7/GcpuK7ctOPoVIS1MyQL7GloWRVt+AStpTvKuQ=="})

# COMMAND ----------

# MAGIC %fs ls /mnt/datalake/

# COMMAND ----------

# HDFS /  DBFS / GFS / DFS / WASBS - windows azure storage blob service / AWS S3  - simple storage Service / DBFS (Databricks file system)
dbutils.fs.mount(
  source = "wasbs://batch19@adlsgen2batch19.blob.core.windows.net/target/",
  mount_point = "/mnt/target",
  extra_configs = {"fs.azure.account.key.adlsgen2batch19.blob.core.windows.net":dbutils.secrets.get(scope="batch19-adlsgen2-scope",key="batch19-adlsgen2-account-key")})

# COMMAND ----------

dbutils.fs.mounts()

# COMMAND ----------

# MAGIC %fs ls /mnt/landing/sales

# COMMAND ----------

df= spark.read.csv("dbfs:/mnt/landing/sales/dept.csv",header=True)
display(df)