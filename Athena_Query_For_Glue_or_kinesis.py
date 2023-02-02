from datetime import datetime, timedelta, timezone
from email import encoders
from email.mime.base import MIMEBase
from email.mime.application import MIMEApplication
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from io import StringIO
import json
import time
import boto3

now = datetime.now(timezone.utc)
now = datetime.now(timezone.utc)-timedelta(hours=1)
dt = now.strftime("%Y%m%d")
hr_str = "%02d" % now.hour

cquery = "CREATE Query "

squery = "SELECT Query"

dquery = "DROP TABLE Table_Name;"

DATABASE = 'default'
output='s3://Bucket_Name/Folder_Name/'
csvoutput='s3://Bucket_Name/Folder_Name/'

def lambda_handler(event, context):
    cquery = "CREATE Query"
    
    squery = "SELECT Query"

    dquery = "DROP TABLE Table_Name;"
    
    msg = MIMEMultipart()
    msg["Subject"] = "Subject_Line!"
    msg["From"] = "contact@santoshbatulla.com"
    msg["To"] = "jenkins@santoshbatulla.com"
    
    client = boto3.client('athena')
    # Execution
    response = client.start_query_execution(
        QueryString=cquery,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': output,
        }
    )

    cqueryId = response['QueryExecutionId']
    time.sleep(30)

    results = client.start_query_execution(
        QueryString=squery,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': csvoutput,
        }
    )

    squeryId = results['QueryExecutionId']
    time.sleep(90)
    
    s3 = boto3.resource('s3')
    s3.Bucket('Bucket_Name').download_file("Folder_Name/" + squeryId + ".csv", "/tmp/data-" + dt + "-" + hr_str + ".csv")

    
    body = MIMEText("Mail_Body_Message.", "plain")
    msg.attach(body)

    filename = "/tmp/data-" + dt + "-" + hr_str + ".csv"

    with open(filename, "rb") as attachment:
        part = MIMEApplication(attachment.read())
        part.add_header("Content-Disposition",
                        "attachment",
                        filename=filename)
    msg.attach(part)

    ses_client = boto3.client("ses", region_name="us-east-1")
    rmail = ses_client.send_raw_email(
        Source="contact@santoshbatulla.com",
        Destinations=["jenkins@santoshbatulla.com"],
        RawMessage={"Data": msg.as_string()}
    )

    drop = client.start_query_execution(
        QueryString=dquery,
        QueryExecutionContext={
            'Database': DATABASE
        },
        ResultConfiguration={
            'OutputLocation': output,
        }
    )
    return response 
    return results
    return rmail
    return drop
    return
