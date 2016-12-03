local = data_bag_item('data', 'local')
cname = local['cname']
username = local['username']

db_user = local['db_user']
db_pass = local['db_pass']
db_host = local['db_host']
db_name = local['db_name']
aws_access_id = local['aws_access_id']
aws_secret_key = local['aws_secret_key']
aws_s3_bucket = local['aws_s3_bucket']
aws_sns_arn = local['aws_sns_arn']
github_token = local['github_token']
mapbox_key = local['mapbox_key']
slack_url = local['slack_url']

database_url = "postgres://#{db_user}:#{db_pass}@#{db_host}/#{db_name}?sslmode=require"

#
# Ensure configuration exists.
#
rotation = <<-ROTATION
{
	copytruncate
	rotate 4
	weekly
	missingok
	notifempty
	compress
	delaycompress
	endscript
}
ROTATION

file "/etc/logrotate.d/openaddr_crontab-collect-extracts" do
    content "/var/log/openaddr_crontab/collect-extracts.log\n#{rotation}\n"
end

file "/etc/logrotate.d/openaddr_crontab-index-tiles" do
    content "/var/log/openaddr_crontab/index-tiles.log\n#{rotation}\n"
end

file "/etc/logrotate.d/openaddr_crontab-dotmap" do
    content "/var/log/openaddr_crontab/dotmap.log\n#{rotation}\n"
end

file "/etc/logrotate.d/openaddr_crontab-enqueue-sources" do
    content "/var/log/openaddr_crontab/enqueue-sources.log\n#{rotation}\n"
end

#
# Place crontab scripts.
#
directory '/etc/cron.d'

directory "/var/log/openaddr_crontab" do
  owner username
  mode "0755"
end

file "/etc/cron.d/openaddr_crontab-collect-extracts" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
LC_ALL=C.UTF-8
# Archive collection, every other day at 11am UTC (4am PDT)
0 11	*/2 * *	#{username}	\
  openaddr-run-ec2-command \
  -a "#{aws_access_id}" \
  -s "#{aws_secret_key}" \
  -b "#{aws_s3_bucket}" \
  --sns-arn "#{aws_sns_arn}" \
  --slack-url "#{slack_url}" \
  --verbose \
  -- \
    openaddr-collect-extracts \
    -d "#{database_url}" \
    -b "#{aws_s3_bucket}" \
    --sns-arn "#{aws_sns_arn}" \
    --verbose \
  >> /var/log/openaddr_crontab/collect-extracts.log 2>&1
CRONTAB
end

file "/etc/cron.d/openaddr_crontab-index-tiles" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
LC_ALL=C.UTF-8
# Index into tiles, every third day at 11am UTC (4am PDT)
0 11	*/3 * *	#{username}	\
  openaddr-run-ec2-command \
  --hours 9 \
  -a "#{aws_access_id}" \
  -s "#{aws_secret_key}" \
  -b "#{aws_s3_bucket}" \
  --sns-arn "#{aws_sns_arn}" \
  --slack-url "#{slack_url}" \
  --verbose \
  -- \
    openaddr-index-tiles \
    -d "#{database_url}" \
    -a "#{aws_access_id}" \
    -s "#{aws_secret_key}" \
    -b "#{aws_s3_bucket}" \
    --sns-arn "#{aws_sns_arn}" \
    --verbose \
  >> /var/log/openaddr_crontab/index-tiles.log 2>&1
CRONTAB
end

file "/etc/cron.d/openaddr_crontab-dotmap" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
LC_ALL=C.UTF-8
# Generate OpenAddresses dot map, every fifth day at 11am UTC (4am PDT)
0 11	*/5 * *	#{username}	\
  openaddr-run-ec2-command \
  --role dotmap \
  --hours 16 \
  --instance-type r3.large \
  -a "#{aws_access_id}" \
  -s "#{aws_secret_key}" \
  -b "#{aws_s3_bucket}" \
  --sns-arn "#{aws_sns_arn}" \
  --slack-url "#{slack_url}" \
  --verbose \
  -- \
    openaddr-update-dotmap \
    -d "#{database_url}" \
    -m "#{mapbox_key}" \
    -a "#{aws_access_id}" \
    -s "#{aws_secret_key}" \
    --sns-arn "#{aws_sns_arn}" \
  >> /var/log/openaddr_crontab/dotmap.log 2>&1
CRONTAB
end

file "/etc/cron.d/openaddr_crontab-enqueue-sources" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
SLACK_URL=#{slack_url}
LC_ALL=C.UTF-8
# Enqueue sources, Fridays 11pm UTC (4pm PDT)
0 23	* * fri	#{username}	( \
  curl -X POST -d '{"text": "Starting new batch run..."}' $SLACK_URL -s ; \
  openaddr-enqueue-sources \
  -d "#{database_url}" \
  -t "#{github_token}" \
  -a "#{aws_access_id}" \
  -s "#{aws_secret_key}" \
  -b "#{aws_s3_bucket}" \
  --sns-arn "#{aws_sns_arn}" \
  && curl -X POST -d '{"text": "Completed <https://#{cname}/latest/set|new batch run>."}' $SLACK_URL -s \
  || curl -X POST -d '{"text": "Failed to complete new batch run."}' $SLACK_URL -s ) \
  >> /var/log/openaddr_crontab/enqueue-sources.log 2>&1
CRONTAB
end

file "/etc/cron.d/openaddr_crontab-cleanup-tempdir" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# Clean up week-old contents of /tmp
0 0	* * *	#{username}	find /tmp -depth -user #{username} -mtime +7 -delete
CRONTAB
end
