# Guided Capstone - Equity Market Data Analysis

### Environment

- java 11
- pipenv
- pipenv install pyspark==3.1.2
- pipenv install pytest==6.2.4

### Additional changes to pySpark for wasb

From SPARK_HOME:
/home/alan/.local/share/virtualenvs/guided-capstone-WVET3RpX/lib/python3.8/site-packages/pyspark

- mkdir hadoop
- Create hadoop/core-site.xml with following contents:
```
<property>
<name>fs.azure.account.key.guidedcapstonesa.blob.core.windows.net</name>
<value><ACCOUNT_KEY></value>
</property>
```

- Modify bin/load-spark-env.sh to export HADOOP_CONF_DIR
``` 
SPARK_ENV_SH="spark-env.sh"
if [ -z "$SPARK_ENV_LOADED" ]; then
export SPARK_ENV_LOADED=1

export SPARK_CONF_DIR="${SPARK_CONF_DIR:-"${SPARK_HOME}"/conf}"
# modification for Azure wasb
export HADOOP_CONF_DIR=${SPARK_HOME}/hadoop
```
* Download following jars from mvnrepository.com to SPARK_HOME/jars

  * jetty-util-ajax-10.0.0.jar
  * jetty-util-6.0.1.jar
  * azure-storage-8.6.6.jar
  * hadoop-azure-3.1.2.jar

## Step One -  Design and Setup

- Dataflow diagram: dataflow.png

## Step Two -  Data Ingestion

### Summary:  

Use Apache Spark RDD and dataframe APIs to read trade and quote data from csv and json sources, conform them to a common schema, and write the output to parquet.


### Usage instructions:

```
pipenv shell
python step2.py
```

### Results persisted as parquet in output/staging

![output](./images/VirtualBox_pySpark_12_07_2021_21_17_10.png)

<br>

![output](./images/VirtualBox_pySpark_12_07_2021_21_17_21.png)

## Step Three: End-of-Day (EOD) Data Load

### Summary:

Recreate Quote and Trade dataframes, filter out-of-date records, and write to cloud storage.

### Usage instructions (following step 2):

```
pipenv shell
python step3.py
```

### Results persisted as parquet in output/latest

![output](./images/VirtualBox_pySpark_12_07_2021_21_17_38.png)

<br>

![output](./images/VirtualBox_pySpark_12_07_2021_21_17_50.png)


## Step Four: Analytical ETL

### Summary:

Derive three metrics for each quote.

Latest trade price before the quote.
Latest 30-minute moving average trade price, before the quote.
The bid/ask price movement from previous day’s closing price.

Metrics are calculated per symbol per exchange

### Usage instructions (following step 3):

```
pipenv shell
python step4.py
```

### Results persisted as parquet in output/analytics

![output](./images/VirtualBox_pySpark_12_07_2021_21_18_18.png)

<br>

![output](./images/VirtualBox_pySpark_12_07_2021_21_18_30.png)

