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
    client = boto3.client('events')
    
    print('Updating rule', COLLECT_RULE, 'with target', EC2_RUN_TARGET_ID, '...', file=sys.stderr)
    rule = client.describe_rule(Name=COLLECT_RULE)

    client.put_rule(
        Name = COLLECT_RULE,
        Description = 'Archive collection, every other day at 11am UTC (4am PDT)',
        ScheduleExpression = 'cron(0 11 */2 * ? *)', State = 'ENABLED',
        )
    
    client.put_targets(
        Rule = COLLECT_RULE,
        Targets = [dict(
            Id = EC2_RUN_TARGET_ID,
            Arn = EC2_RUN_TARGET_ARN,
            Input = json.dumps({
                "command": ["openaddr-collect-extracts"], "hours": 18,
                "bucket": "data.openaddresses.io", "sns-arn": SNS_ARN
                })
            )]
        )
    
    print('Updating rule', CALCULATE_RULE, 'with target', EC2_RUN_TARGET_ID, '...', file=sys.stderr)
    rule = client.describe_rule(Name=CALCULATE_RULE)

    client.put_rule(
        Name = CALCULATE_RULE,
        Description = 'Update coverage page data, every third day at 11am UTC (4am PDT)',
        ScheduleExpression = 'cron(0 11 */3 * ? *)', State = 'ENABLED',
        )
    
    client.put_targets(
        Rule = CALCULATE_RULE,
        Targets = [dict(
            Id = EC2_RUN_TARGET_ID,
            Arn = EC2_RUN_TARGET_ARN,
            Input = json.dumps({
                "command": ["openaddr-calculate-coverage"],
                "hours": 3, "instance-type": "t2.nano",
                "bucket": "data.openaddresses.io", "sns-arn": SNS_ARN
                })
            )]
        )

    print('Updating rule', DOTMAP_RULE, 'with target', EC2_RUN_TARGET_ID, '...', file=sys.stderr)
    rule = client.describe_rule(Name=DOTMAP_RULE)

    client.put_rule(
        Name = DOTMAP_RULE,
        Description = 'Generate OpenAddresses dot map, every fifth day at 11am UTC (4am PDT)',
        ScheduleExpression = 'cron(0 11 */5 * ? *)', State = 'ENABLED',
        )
    
    client.put_targets(
        Rule = DOTMAP_RULE,
        Targets = [dict(
            Id = EC2_RUN_TARGET_ID,
            Arn = EC2_RUN_TARGET_ARN,
            Input = json.dumps({
                "command": ["openaddr-update-dotmap"],
                "hours": 16, "instance-type": "r3.large", "temp-size": 256,
                "bucket": "data.openaddresses.io", "sns-arn": SNS_ARN
                })
            )]
        )

    print('Updating rule', TILEINDEX_RULE, 'with target', EC2_RUN_TARGET_ID, '...', file=sys.stderr)
    rule = client.describe_rule(Name=TILEINDEX_RULE)

    client.put_rule(
        Name = TILEINDEX_RULE,
        Description = 'Index into tiles, every seventh day at 11am UTC (4am PDT)',
        ScheduleExpression = 'cron(0 11 */7 * ? *)', State = 'ENABLED',
        )
    
    client.put_targets(
        Rule = TILEINDEX_RULE,
        Targets = [dict(
            Id = EC2_RUN_TARGET_ID,
            Arn = EC2_RUN_TARGET_ARN,
            Input = json.dumps({
                "command": ["openaddr-index-tiles"],
                "hours": 16,
                "bucket": "data.openaddresses.io", "sns-arn": SNS_ARN
                })
            )]
        )

if __name__ == '__main__':
    exit(main())
