# Clicks_fraud_detection_with_Kafka_and_Flink
Detection of suspicious clicks on ads through a data stream analysis.  
Data stream the was read from kafka and processed on Flink  

## Analysis_and_outputs folder contains: 
__Pre_analysis.ipynb__ : the upstream data analysis to define 3 suspicious patterns  
__Patterns_analysis.ipynb__ : the analysis of the patterns outputted  
__The output files__ of the streaming job  

## <font color='green'> The scala Streaming job file: </font>  
[located here](https://github.com/Nada-S/Clicks_fraud_detection_with_Kafka_and_Flink/blob/master/src/main/scala/org/apache/flink/StreamingJob.scala)
