#!/usr/bin/env python2.7
from __future__ import print_function
import json
import os
try:
    from http.client import HTTPSConnection
    from urllib.parse import urlparse
except ImportError:
    from httplib import HTTPSConnection
    from urlparse import urlparse

def lambda_handler(event, context):
    '''
    '''
    parsed = urlparse(os.environ['SLACK_URL'])
    conn = HTTPSConnection(parsed.hostname)
    
    for (subject, message) in summarize_messages(event):
        if subject == message:
            body = json.dumps(dict(text=subject))
        else:
            body = json.dumps(dict(text=subject, attachments=[dict(text=message)]))
        conn.request('POST', parsed.path, body)
        resp = conn.getresponse()

        print('Sent:', body)
        print('HTTP {} from {}'.format(resp.status, parsed.hostname))

def summarize_messages(event):
    '''
    '''
    records = event.get('Records', [])
    messages = []
    
    for (index, record) in enumerate(records):
        print('Record {}:'.format(index), json.dumps(record))
    
        if 'Sns' in record:
            try:
                msgjson = json.loads(record['Sns']['Message'])
            except ValueError:
                # Send the raw message, which couldn't be parsed as JSON.
                message = record['Sns']['Message']
            else:
                # Look for known values in the message JSON.
                if 'Cause' in msgjson:
                    # Autoscale events have a cause.
                    message = msgjson['Cause']
                elif 'NewStateReason' in msgjson:
                    # Cloudwatch alarms have a reason.
                    message = msgjson['NewStateReason']
                else:
                    # Send the raw JSON, which didn't have a recognized value.
                    message = json.dumps(msgjson)
            messages.append((record['Sns']['Subject'], message))
        else:
            print('Unknown record type:', record)
            messages.append(('Mysterious message from {}'.format(record.get('EventSource', '???')), None))
    
    return messages

if __name__ == '__main__':
    summaries = summarize_messages(json.loads('''
        {
          "Records": [
            {
              "EventVersion": "1.0",
              "EventSource": "aws:sns",
              "EventSubscriptionArn": "arn:aws:sns:us-east-1:847904970422:CI-Events:380ff93e-d662-4028-8659-c5fb820673cd",
              "Sns": {
                "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:847904970422:CI-Events:380ff93e-d662-4028-8659-c5fb820673cd",
                "Type": "Notification",
                "SignatureVersion": "1",
                "Timestamp": "2017-02-09T00:31:48.572Z",
                "Signature": "bT6VrGYf/O+GwELg62I27uTHwymYnSPSUGvRwJbaYQO1StIo9r2gOPu6qvuC7u75jSl7gdxsM2xJ/bJAgLP2BV15Eiv8S0bx7pInSZLiQXbe+40Tk3K1ydcKUkW5XMu8PP2OdRSktz2PB65Tkau+37R1VlID4C6fCcTd3oiD0xQGW1dsjzeJmGW8mONh/G3fUpnx8jbKOZdRGlaqzkhEZWA4zuGgBqXXoLShWokIPzS15WJYL2NUJ5EmE9G6dTibd8rkbRWIJh5Qhta/QBQ67ipNXZG5eqv+0alMef11/Ww92cMdS4z22p/oR+yWgw3OGzh+W8YQBOn45zr2PbXJEA==",
                "Message": "{\\"Progress\\":50,\\"AccountId\\":\\"847904970422\\",\\"Description\\":\\"Launching a new EC2 instance: i-0154e7d1fba186b38\\",\\"RequestId\\":\\"510cf431-7cab-42e8-9ce9-91d7ebc8cb33\\",\\"EndTime\\":\\"2017-02-09T00:31:48.519Z\\",\\"AutoScalingGroupARN\\":\\"arn:aws:autoscaling:us-east-1:847904970422:autoScalingGroup:2d9cc8fc-b822-4266-afa1-c40ba5f0863c:autoScalingGroupName/CI Crontab 4.x\\",\\"ActivityId\\":\\"510cf431-7cab-42e8-9ce9-91d7ebc8cb33\\",\\"StartTime\\":\\"2017-02-09T00:31:16.159Z\\",\\"Service\\":\\"AWS Auto Scaling\\",\\"Time\\":\\"2017-02-09T00:31:48.519Z\\",\\"EC2InstanceId\\":\\"i-0154e7d1fba186b38\\",\\"StatusCode\\":\\"InProgress\\",\\"StatusMessage\\":\\"\\",\\"Details\\":{\\"Subnet ID\\":\\"subnet-35d87242\\",\\"Availability Zone\\":\\"us-east-1b\\"},\\"AutoScalingGroupName\\":\\"CI Crontab 4.x\\",\\"Cause\\":\\"At 2017-02-09T00:31:13Z an instance was started in response to a difference between desired and actual capacity, increasing the capacity from 0 to 1.\\",\\"Event\\":\\"autoscaling:EC2_INSTANCE_LAUNCH\\"}",
                "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-b95095beb82e8f6a046b3aafc7f4149a.pem",
                "MessageAttributes": {},
                "Subject": "Auto Scaling: launch for group \\"CI Crontab 4.x\\"",
                "TopicArn": "arn:aws:sns:us-east-1:847904970422:CI-Events",
                "MessageId": "90cced31-040b-58a6-9538-bc24bebf2c8b"
              }
            },
            {
              "EventVersion": "1.0", 
              "EventSource": "aws:sns", 
              "EventSubscriptionArn": "arn:aws:sns:us-east-1:847904970422:CI-Events:543efcac-0802-4fdd-9eb1-d6d6c8f76799", 
              "Sns": {
                "MessageId": "d0f9ec0a-e542-5c24-863e-bd1f2993ce6b", 
                "Signature": "mTD5mrUzok2eE1UmJR7Le/D0eOveczZ39wXC7bxxg8IMOchSNwa6+KtKV4D+oD26uC4WSmCH5z92b09hX6vaTTpdc7G1DPywInUiwrLXYgrPgFKVG1Tj1JJZqTp+14JH/XiaaQ5WQ9sxPSQ7u1Iczd86jHtdkdOs7LBmWgzuFjFAJrkJz41JpgYuiEhDR0K07Syz/EKBtao3hd3QlG2CvJNNqhgxaYvn98GMDsfbtO0OZcoZX4TiAHkQslpyj3v0B/7IpuRKsmIC7wXL0NMNH2S8TetLqVLPpssezOYOHJj/Lu53ojeYJ3y3hwdQGBHL8yH6gOuUT7G/x6wFNDdKKQ==", 
                "Type": "Notification", 
                "TopicArn": "arn:aws:sns:us-east-1:847904970422:CI-Events", 
                "MessageAttributes": {}, 
                "SignatureVersion": "1", 
                "Timestamp": "2017-02-09T01:49:05.702Z", 
                "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-b95095beb82e8f6a046b3aafc7f4149a.pem", 
                "Message": "And this is the test message", 
                "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:847904970422:CI-Events:543efcac-0802-4fdd-9eb1-d6d6c8f76799", 
                "Subject": "Test Subject"
              }
            },
            {
              "EventVersion": "1.0", 
              "EventSource": "aws:sns", 
              "EventSubscriptionArn": "arn:aws:sns:us-east-1:847904970422:CI-Events:543efcac-0802-4fdd-9eb1-d6d6c8f76799", 
              "Sns": {
                "MessageId": "d0f9ec0a-e542-5c24-863e-bd1f2993ce6b", 
                "Signature": "mTD5mrUzok2eE1UmJR7Le/D0eOveczZ39wXC7bxxg8IMOchSNwa6+KtKV4D+oD26uC4WSmCH5z92b09hX6vaTTpdc7G1DPywInUiwrLXYgrPgFKVG1Tj1JJZqTp+14JH/XiaaQ5WQ9sxPSQ7u1Iczd86jHtdkdOs7LBmWgzuFjFAJrkJz41JpgYuiEhDR0K07Syz/EKBtao3hd3QlG2CvJNNqhgxaYvn98GMDsfbtO0OZcoZX4TiAHkQslpyj3v0B/7IpuRKsmIC7wXL0NMNH2S8TetLqVLPpssezOYOHJj/Lu53ojeYJ3y3hwdQGBHL8yH6gOuUT7G/x6wFNDdKKQ==", 
                "Type": "Notification", 
                "TopicArn": "arn:aws:sns:us-east-1:847904970422:CI-Events", 
                "MessageAttributes": {}, 
                "SignatureVersion": "1", 
                "Timestamp": "2017-02-09T01:49:05.702Z", 
                "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-b95095beb82e8f6a046b3aafc7f4149a.pem", 
                "Message": "[1,2,3]", 
                "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:847904970422:CI-Events:543efcac-0802-4fdd-9eb1-d6d6c8f76799", 
                "Subject": "Test Subject"
              }
            },
            {
                "EventVersion": "1.0",
                "EventSubscriptionArn": "arn:aws:sns:us-east-1:847904970422:CI-Events:543efcac-0802-4fdd-9eb1-d6d6c8f76799",
                "EventSource": "aws:sns",
                "Sns": {
                    "SignatureVersion": "1",
                    "Timestamp": "2017-02-15T23:42:53.523Z",
                    "Signature": "YxmEX0nUkDLlg33oGx4KMdrRH4QPl44pA5cuD0YY+qgSTs2+6n9A11esHySDoGZhrJ6EhHeFECtTcSxfa93Kk82H31pqulgEXpfPPnddz8rNgTtSgJQmeu7E3fYTj6t1tbmE+u2wc9UIzps/2KS0tEdXd56ZKsKs8avA0iAlGbZtf4lNLUvUFJlyR+VB7Zb96lxjnyo6HNP6se2y29IpvYxQrA0na89+4w+m6BU4hxpQT5SmqaCool4K7ezzJ3tkm4e20JQViQdtvm5AYx0/9nl6JOSCOuk7cIR/atx09VsaCw6zGrt36Vqqi4ZshXlzxshGCICE7Wk6sB1kwREWHQ==",
                    "SigningCertUrl": "https://sns.us-east-1.amazonaws.com/SimpleNotificationService-b95095beb82e8f6a046b3aafc7f4149a.pem",
                    "MessageId": "c250d0c2-0bd1-5241-b5dc-81dce26c043f",
                    "Message": "{\\"AlarmName\\":\\"Machine RDS Low CPU Credits\\",\\"AlarmDescription\\":null,\\"AWSAccountId\\":\\"847904970422\\",\\"NewStateValue\\":\\"ALARM\\",\\"NewStateReason\\":\\"Threshold Crossed: 1 datapoint (79.85) was less than the threshold (80.0).\\",\\"StateChangeTime\\":\\"2017-02-15T23:42:53.475+0000\\",\\"Region\\":\\"US East - N. Virginia\\",\\"OldStateValue\\":\\"OK\\",\\"Trigger\\":{\\"MetricName\\":\\"CPUCreditBalance\\",\\"Namespace\\":\\"AWS/RDS\\",\\"StatisticType\\":\\"Statistic\\",\\"Statistic\\":\\"AVERAGE\\",\\"Unit\\":null,\\"Dimensions\\":[{\\"name\\":\\"DBInstanceIdentifier\\",\\"value\\":\\"machine\\"}],\\"Period\\":300,\\"EvaluationPeriods\\":1,\\"ComparisonOperator\\":\\"LessThanThreshold\\",\\"Threshold\\":80.0,\\"TreatMissingData\\":\\"\\",\\"EvaluateLowSampleCountPercentile\\":\\"\\"}}",
                    "MessageAttributes": {},
                    "Type": "Notification",
                    "UnsubscribeUrl": "https://sns.us-east-1.amazonaws.com/?Action=Unsubscribe&SubscriptionArn=arn:aws:sns:us-east-1:847904970422:CI-Events:543efcac-0802-4fdd-9eb1-d6d6c8f76799",
                    "TopicArn": "arn:aws:sns:us-east-1:847904970422:CI-Events",
                    "Subject": "ALARM: \\"Machine RDS Low CPU Credits\\" in US East - N. Virginia"
                }
            }
          ]
        }
        '''))
    
    assert summaries[0] == ('Auto Scaling: launch for group "CI Crontab 4.x"',
    'At 2017-02-09T00:31:13Z an instance was started in response to a difference '
    'between desired and actual capacity, increasing the capacity from 0 to 1.')
    assert summaries[1] == ('Test Subject', 'And this is the test message')
    assert summaries[2] == ('Test Subject', '[1, 2, 3]')
    assert summaries[3] == ('ALARM: "Machine RDS Low CPU Credits" in US East - N. Virginia',
    'Threshold Crossed: 1 datapoint (79.85) was less than the threshold (80.0).')

    from pprint import pprint
    pprint(summaries)
