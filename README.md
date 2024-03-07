# Pintrest Data Pipeline
What is the tasK?
What are the steps?
What resources/tools are used?
Outcome?


# Installation instructions
Download with user_posting_emulation.py
Access, clean and analyse with s3_bucket_to_databricks.ipynb

Upload to AWS for sheduling 129076a9eaf9_dag.py
Send data to streams with user_posting_emulation_streaming.py
Access and clean data with dataframes_to_delta_tables.ipynb

# Usage instructions
You will first need to create a key pair file locally, which is a file ending in the .pem extension. This file will ultimately allow you to connect to your EC2 instance. To do this, first navigate to Parameter Store in your AWS account.
Using your KeyPairId (you can locate this information within the email containing your AWS login credentials) find the specific key pair associated with your EC2 instance. Select this key pair and under the Value field select Show.This will reveal the content of your key pair. Copy its entire value (including the BEGIN and END header) and paste it in the .pem file in VSCode.
Step 2:
Navigate to the EC2 console and identify the instance with your unique UserId. Select this instance, and under the Details section find the Key pair name and make a note of this. Save the previously created file in the VSCode using the following format: Key pair name.pem.

-ssh to install packages
get data from AWS

Download the Confluent.io Amazon S3 Connector and configure to bucket

Now that you have set up the Kafka REST Proxy integration for your API, you need to set up the Kafka REST Proxy on your EC2 client machine.

Install the Confluent package for the Kafka REST Proxy on your EC2 client machine.
Start the REST proxy on the EC2 client machine.

Run Databricks

----

If yours AWS account  HAS NOT already been provided with access to a MWAA environment Databricks-Airflow-env and to its S3 bucket mwaa-dags-bucket, you will be required to create an API token in Databricks to connect to your AWS account, to set up the MWAA-Databricks connection or to create the requirements.txt file.

This DAG should be uploaded to the dags folder in the mwaa-dags-bucket.
Manually trigger the DAG you have uploaded in the previous step and check it runs successfully.



This is the ARN you should be using when setting up the Execution role for the integration point of all the methods you will create.
Your API should be able to invoke the following actions:
List streams in Kinesis
Create, describe and delete streams in Kinesis
Add records to streams in Kinesis

Run  user_posting_emulation_streaming.py
Run Databricks
Once the streaming data has been cleaned, you should save each stream in a Delta Table.



# License information
MIT License

Copyright (c) [2024] [Daniel White]

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.