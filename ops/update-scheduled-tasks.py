#!/usr/bin/env python3
import boto3, json, sys

COLLECT_RULE = 'OA-Collect-Extracts'
CALCULATE_RULE = 'OA-Calculate-Coverage'
DOTMAP_RULE = 'OA-Update-Dotmap'
TILEINDEX_RULE = 'OA-Index-Tiles'
EC2_RUN_TARGET_ID = 'OA-EC2-Run-Task'
EC2_RUN_TARGET_ARN = 'arn:aws:lambda:us-east-1:847904970422:function:OA-EC2-Run-Task'
SNS_ARN = "arn:aws:sns:us-east-1:847904970422:CI-Events"

def main():
    rules = {
        COLLECT_RULE: dict(
            cron = 'cron(0 11 */2 * ? *)',
            description = 'Archive collection, every other day at 11am UTC (4am PDT)',
            input = {
                "command": ["openaddr-collect-extracts"], "hours": 18,
                "bucket": "data.openaddresses.io", "sns-arn": SNS_ARN
                }),
        CALCULATE_RULE: dict(
            cron = 'cron(0 11 */3 * ? *)',
            description = 'Update coverage page data, every third day at 11am UTC (4am PDT)',
            input = {
                "command": ["openaddr-calculate-coverage"],
                "hours": 3, "instance-type": "t2.nano",
                "bucket": "data.openaddresses.io", "sns-arn": SNS_ARN
                }),
        DOTMAP_RULE: dict(
            cron = 'cron(0 11 */5 * ? *)',
            description = 'Generate OpenAddresses dot map, every fifth day at 11am UTC (4am PDT)',
            input = {
                "command": ["openaddr-update-dotmap"],
                "hours": 16, "instance-type": "r3.large", "temp-size": 256,
                "bucket": "data.openaddresses.io", "sns-arn": SNS_ARN
                }),
        TILEINDEX_RULE: dict(
            cron = 'cron(0 11 */7 * ? *)',
            description = 'Index into tiles, every seventh day at 11am UTC (4am PDT)',
            input = {
                "command": ["openaddr-index-tiles"],
                "hours": 16,
                "bucket": "data.openaddresses.io", "sns-arn": SNS_ARN
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
