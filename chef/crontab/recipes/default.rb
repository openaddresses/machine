local = data_bag_item('data', 'local')
cname = local['cname']
username = local['username']

db_user = local['db_user']
db_pass = local['db_pass']
db_host = local['db_host']
db_name = local['db_name']
aws_s3_bucket = local['aws_s3_bucket']
aws_sns_arn = local['aws_sns_arn']
aws_cloudwatch_ns = local['aws_cloudwatch_ns']
github_token = local['github_token']
mapbox_key = local['mapbox_key']

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

file "/etc/logrotate.d/openaddr_crontab-index-tiles" do
    content "/var/log/openaddr_crontab/index-tiles.log\n#{rotation}\n"
end

file "/etc/logrotate.d/openaddr_crontab-dotmap" do
    content "/var/log/openaddr_crontab/dotmap.log\n#{rotation}\n"
end

file "/etc/logrotate.d/openaddr_crontab-enqueue-sources" do
    content "/var/log/openaddr_crontab/enqueue-sources.log\n#{rotation}\n"
end

file "/etc/logrotate.d/openaddr_crontab-sum-up-data" do
    content "/var/log/openaddr_crontab/sum-up-data.log\n#{rotation}\n"
end

#
# Place crontab scripts.
#
directory '/etc/cron.d'

directory "/var/log/openaddr_crontab" do
  owner username
  mode "0755"
end

file "/etc/cron.d/openaddr_crontab-index-tiles" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
LC_ALL=C.UTF-8
# Index into tiles, every seventh day at 11am UTC (4am PDT)
0 11	*/7 * *	#{username}	\
  openaddr-run-ec2-command \
  --hours 16 \
  -b "#{aws_s3_bucket}" \
  --sns-arn "#{aws_sns_arn}" \
  --verbose \
  -- \
    openaddr-index-tiles \
    -d "#{database_url}" \
    -b "#{aws_s3_bucket}" \
    --sns-arn "#{aws_sns_arn}" \
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
  --temp-size 256 \
  -b "#{aws_s3_bucket}" \
  --sns-arn "#{aws_sns_arn}" \
  --verbose \
  -- \
    openaddr-update-dotmap \
    -d "#{database_url}" \
    -m "#{mapbox_key}" \
    --sns-arn "#{aws_sns_arn}" \
  >> /var/log/openaddr_crontab/dotmap.log 2>&1
CRONTAB
end

file "/etc/cron.d/openaddr_crontab-enqueue-sources" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
LC_ALL=C.UTF-8
# Enqueue sources, Fridays 11pm UTC (4pm PDT)
0 23	* * fri	#{username}	\
  openaddr-run-ec2-command \
  --hours 60 \
  --instance-type t2.nano \
  -b "#{aws_s3_bucket}" \
  --sns-arn "#{aws_sns_arn}" \
  --verbose \
  -- \
    openaddr-enqueue-sources \
    -d "#{database_url}" \
    -t "#{github_token}" \
    -b "#{aws_s3_bucket}" \
    --sns-arn "#{aws_sns_arn}" \
    --cloudwatch-ns "#{aws_cloudwatch_ns}" \
  >> /var/log/openaddr_crontab/enqueue-sources.log 2>&1
CRONTAB
end

file "/etc/cron.d/openaddr_crontab-sum-up-data" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin
LC_ALL=C.UTF-8
# Sum up current data, hourly
0 *	* * *	#{username}	\
  openaddr-sum-up-data \
    -d "#{database_url}" \
    -b "#{aws_s3_bucket}" \
    --sns-arn "#{aws_sns_arn}" \
    --quiet \
  >> /var/log/openaddr_crontab/sum-up-data.log 2>&1
CRONTAB
end
