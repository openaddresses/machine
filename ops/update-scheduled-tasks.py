#!/usr/bin/env python3
''' Update descriptions, timing, and targets for OpenAddresses scheduled tasks.

Periodic tasks in OpenAddresses are implemented as a series of event rules in
AWS Cloudwatch. They invoke a single EC2 task instance runner in AWS Lambda,
which instantiates a fresh EC2 instance that lasts only for the duration of the
task. This script modifies the description, schedule expression, and Lambda
target for each event rule.
'''
import boto3, json, sys
from os.path import join, dirname, exists

ENQUEUE_RULE = 'OA-Enqueue-Sources'
COLLECT_RULE = 'OA-Collect-Extracts'
CALCULATE_RULE = 'OA-Calculate-Coverage'
DOTMAP_RULE = 'OA-Update-Dotmap'
TILEINDEX_RULE = 'OA-Index-Tiles'
EC2_RUN_TARGET_ID = 'OA-EC2-Run-Task'
EC2_RUN_TARGET_ARN = 'arn:aws:lambda:us-east-1:847904970422:function:OA-EC2-Run-Task'
LOG_BUCKET = "data.openaddresses.io"
SNS_ARN = "arn:aws:sns:us-east-1:847904970422:CI-Events"

version_paths = ['../openaddr/VERSION', 'VERSION']

def first_file(paths):
    for path in paths:
        if exists(join(dirname(__file__), path)):
            return join(dirname(__file__), path)

def main():
    with open(first_file(version_paths)) as file:
        version = file.read().strip()
    
    print('Found version', version)

    rules = {
        ENQUEUE_RULE: dict(
            cron = 'cron(0 23 ? * fri *)',
            description = 'Enqueue sources, Fridays 11pm UTC (4pm PDT)',
            input = {
                "command": ["openaddr-enqueue-sources"],
                "hours": 60, "instance-type": "t2.nano",
                "bucket": LOG_BUCKET, "sns-arn": SNS_ARN, "version": version
                }),
        COLLECT_RULE: dict(
            cron = 'cron(0 11 */2 * ? *)',
            description = 'Archive collection, every other day at 11am UTC (4am PDT)',
            input = {
                "command": ["openaddr-collect-extracts"], "hours": 18,
                "bucket": LOG_BUCKET, "sns-arn": SNS_ARN, "version": version
                }),
        CALCULATE_RULE: dict(
            cron = 'cron(0 11 */3 * ? *)',
            description = 'Update coverage page data, every third day at 11am UTC (4am PDT)',
            input = {
                "command": ["openaddr-calculate-coverage"],
                "hours": 3, "instance-type": "t2.nano",
                "bucket": LOG_BUCKET, "sns-arn": SNS_ARN, "version": version
                }),
        DOTMAP_RULE: dict(
            cron = 'cron(0 11 */5 * ? *)',
            description = 'Generate OpenAddresses dot map, every fifth day at 11am UTC (4am PDT)',
            input = {
                "command": ["openaddr-update-dotmap"],
                "hours": 16, "instance-type": "r3.large", "temp-size": 256,
                "bucket": LOG_BUCKET, "sns-arn": SNS_ARN, "version": version
                }),
        TILEINDEX_RULE: dict(
            cron = 'cron(0 11 */7 * ? *)',
            description = 'Index into tiles, every seventh day at 11am UTC (4am PDT)',
            input = {
                "command": ["openaddr-index-tiles"], "hours": 16,
                "bucket": LOG_BUCKET, "sns-arn": SNS_ARN, "version": version
                }),
        }
    
    client = boto3.client('events')
    
    for (rule_name, details) in rules.items():
        print('Updating rule', rule_name, 'with target', EC2_RUN_TARGET_ID, '...', file=sys.stderr)
        rule = client.describe_rule(Name=rule_name)

        client.put_rule(
            Name = rule_name,
            Description = details['description'],
            ScheduleExpression = details['cron'], State = 'ENABLED',
            )
    
        client.put_targets(
            Rule = rule_name,
            Targets = [dict(
                Id = EC2_RUN_TARGET_ID,
                Arn = EC2_RUN_TARGET_ARN,
                Input = json.dumps(details['input'])
                )]
            )

if __name__ == '__main__':
    exit(main())
