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
            message = json.loads(record['Sns']['Message'])
            messages.append('*{}*\n{}'.format(record['Sns']['Subject'], message['Cause']))
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
            }
          ]
        }
        ''')))
