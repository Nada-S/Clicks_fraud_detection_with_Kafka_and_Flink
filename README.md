# Clicks_fraud_detection_with_Kafka_and_Flink
Detection of suspicious clicks on ads through a data stream analysis.  
The data stream was read from kafka and processed on Flink  

## The scala Streaming job file: 
[located here](https://github.com/Nada-S/Clicks_fraud_detection_with_Kafka_and_Flink/blob/master/src/main/scala/org/apache/flink/StreamingJob.scala)

## Analysis_and_outputs folder contains: 
[__Pre_analysis.ipynb__](https://github.com/Nada-S/Clicks_fraud_detection_with_Kafka_and_Flink/blob/master/Analysis_and_outputs/Pre_analysis.ipynb) : the upstream analysis of a sample of data to define 3 suspicious patterns  
[__Patterns_analysis.ipynb__](https://github.com/Nada-S/Clicks_fraud_detection_with_Kafka_and_Flink/blob/master/Analysis_and_outputs/Patterns_analysis.ipynb) : analysis of the outputted patterns     
__The output files__ of the streaming job  
