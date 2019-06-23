# hit_data_app

This repository contains all code files to parse a clickstream data file and generate relevant reports. Backend infrastructure for this application is on AWS


## How to install the app on AWS:

a)	Create a new s3 bucket with any name of your choice.

b)	Add a new Lambda Function with name ‘processHitLevelData’ and select python 3.6 as a run time environment. Assign and create full Lambda execution role to his function using AWS IAM.

c)	Add a s3 put trigger (bucket from (a)) to the lambda function.

d)	Add a new layer in Lambda function and upload the ‘python.zip’ file to it. Link the layer to the function you just created. This layer adds the dependencies for python packages used in the ‘lambda_function.py’ file.

e)	Copy paste the full ‘lambda_function.py’ file from this repo to the newly created lambda function on AWS.

f)	Assign a memory (512 MB) and give value of 3 mins to the timeout field.

 
## How to run the app on AWS:
a)	Drop the (data.sql) file in the s3 bucket created before to run the app.

b)	A new directory ‘result’ will be created with a report inside the same bucket.
