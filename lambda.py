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
    
    for message in summarize_messages(event):
        body = json.dumps(dict(text=message))
        conn.request('POST', parsed.path, body)
        resp = conn.getresponse()

        print('HTTP {} for message {}'.format(resp.status, message))

def summarize_messages(event):
    '''
    '''
    records = event.get('Records', [])
    messages = []
    
    for record in records:
        if 'Sns' in record:
            try:
                message = json.loads(record['Sns']['Message'])['Cause']
            except:
                message = record['Sns']['Message']
            messages.append('*{}*\n```\n{}\n```'.format(record['Sns']['Subject'], message))
        else:
            print('Unknown record type:', record)
            messages.append('Mysterious message from {}'.format(record.get('EventSource', '???')))
    
    return messages

if __name__ == '__main__':
    print(summarize_messages(json.loads('''
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
            }
          ]
        }
        ''')))
