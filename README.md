## Project Overview
Pinterest crunches billions of data points every day to decide how to provide more value to their users.
 This is a imitation of Pinterest's system's using the AWS Cloud.

### Dependencies
apache-airflow==2.10.5
boto3==1.35.93
PyYAML==6.0.2
PyYAML==6.0.2
Requests==2.32.3
SQLAlchemy==2.0.36

### How To Run
To run batch processing run user_posting_emulation.py after generating the disierd amout of data run BatchDB in databricks
To Run Stream prossesing run user_posting_emulation_streaming.py while generating data run StreamingDB in databricks