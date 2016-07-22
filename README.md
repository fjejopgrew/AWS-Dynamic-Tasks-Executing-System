AWS_Dynamic_Tasks_Executing_System
==================================
•	This is a multiple-client dynamic tasks executing system based on AWS.

•	It Uses AWS SQS as tasks queue to submit and retrieve tasks, used AWS DynamoDB to solve duplication problems, and achieved data persistence with AWS S3.

•	Based on the SQS length to achieve dynamic provision and automatic termination of worker nodes.


How to Use
==================================
If you want to use it, you need to set up you default aws account and credentials at '.aws' folder. What's more, replace the SQS url, EC2 ip address and so on.
