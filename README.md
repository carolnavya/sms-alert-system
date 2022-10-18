# sms-alert-system

## Problem Statement
The objective is to simulate sending a large number of SMS alerts, like for an emergency alert service. The simulation consists of three parts:
1.	A **Producer** that that generates a configurable number of messages (default 1000) to random phone number. Each message contains up to 100 random characters.
2.	A **Sender**, who pickups up messages from the producer and simulates sending messages by waiting a random period time distributed around a configurable mean. The sender also has a configurable failure rate.
3.	A **Progress Monitor** that displays the following and updates it every N seconds (configurable):\
a.	Number of messages sent so far\
b.	Number of messages failed so far\
c.	Average time per message so far

## Technology Stack
- Python
- AWS DynamoDB
- AWS SQS
- AWS Lambda

## System overview
![undefined drawio (3)](https://user-images.githubusercontent.com/36771418/196522174-daf8e616-54a0-41d2-bed8-6539b46f783d.png)


## How to run
Read the Cloud Engineer test documentation pdf for the project setup instructions.
To run producer:
```
python3 producer.py -m=<max-messages-generated> -l=<length-of-each-message>
```
To run Sender:
```
python3 sender.py -w=<wait-time> -f=<failure-rate>
```
To run activity Monitor
```
python3 python3 activityMonitor.py -t=<time(secs)-to-casually-overlook-stats>
```
## References
- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/sqs.html
- https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/dynamodb.html#DynamoDB.Client.query
- https://www.thelambdablog.com/getting-dynamodb-data-changes-with-streams-and-processing-them-with-lambdas-using-python/
- https://dev.to/aws-builders/select-count-from-dynamodb-group-by-pk1-sk1-with-streams-43dj
