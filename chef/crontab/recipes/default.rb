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
