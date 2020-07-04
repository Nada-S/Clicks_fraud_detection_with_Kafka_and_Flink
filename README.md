# Clicks_fraud_detection_with_Kafka_and_Flink
Detection of suspicious clicks on ads through a data stream analysis.  
Data stream the was read from kafka and processed on Flink  

## <font color='red'> Analysis_and_outputs folder contains: </font>
Pre_analysis.ipynb : the upstream data analysis to define 3 suspicious patterns
Patterns_analysis.ipynb : the analysis of the patterns outputted
The output files of the streaming job 

## <font color='red'> The scala Streaming job file: </font>
located : Clicks_fraud_detection_with_Kafka_and_Flink\src\main\scala\org\apache\flink
