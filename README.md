
# Big Data Pipeline: 
# Prediction Of Football Matches Results

This is a pipeline that uses many Big Data technologies to predict results of football match.

## Requirements
Pull this image docker please. It is an Ubuntu image with **Hadoop** (2.7.2), **Spark** (2.2.1), **Kafka** (2.11-1.0.2) and **HBase** (1.4.8) 
```bash
   docker pull liliasfaxi/spark-hadoop:hv-2.7.2
``` 

## Data
- We took the date from Kaggle: **Matches in LaLiga**.
- The table consists of **37147** lines.
- Each line represents a football match and the number of goals for each team.

## Architecture
![Architecture](https://user-images.githubusercontent.com/62619786/168388800-fbf15de1-cc8a-4fe3-98ad-c15d9763d567.PNG)

**1.** Took the dataset from Kaggle which contains results from football matches in LaLiga and done some preprocessing.

**2.** Launched a mapReduce job to calculate the average goals scored by each team.

**3.** Sent the generated output through Kafka to be stored in HBase.

**4.** Used Spark to extract that data from HBase and use it to predict the result of a football match between two teams mentioned in the users requests.
 



