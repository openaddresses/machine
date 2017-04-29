#!/usr/bin/env python3
import boto3, json, sys

COLLECT_RULE = 'OA-Collect-Extracts'
COLLECT_RULE_TARGET_ID = 'OA-EC2-Run-Task'

def main():
    client = boto3.client('events')
    
    print('Updating rule', COLLECT_RULE, 'with target', COLLECT_RULE_TARGET_ID, '...', file=sys.stderr)
    rule = client.describe_rule(Name=COLLECT_RULE)

    client.put_rule(
        Name = COLLECT_RULE,
        Description = 'Archive collection, every other day at 11am UTC (4am PDT)',
        ScheduleExpression = 'cron(0 11 */2 * ? *)', State = 'ENABLED',
        )
    
    client.put_targets(
        Rule = COLLECT_RULE,
        Targets = [{
            'Id': COLLECT_RULE_TARGET_ID,
            'Arn': 'arn:aws:lambda:us-east-1:847904970422:function:OA-EC2-Run-Task',
            'Input': json.dumps({
                "command": ["openaddr-collect-extracts"], 
                "hours": 18, "bucket": "data.openaddresses.io",
                "sns-arn": "arn:aws:sns:us-east-1:847904970422:CI-Events"
                })
            }]
        )

if __name__ == '__main__':
    exit(main())
