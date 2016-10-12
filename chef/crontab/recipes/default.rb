cname = node[:cname]
username = node[:username]

db_user = node[:db_user]
db_pass = node[:db_pass]
db_host = node[:db_host]
db_name = node[:db_name]
aws_access_id = node[:aws_access_id]
aws_secret_key = node[:aws_secret_key]
aws_sns_arn = node[:aws_sns_arn]
github_token = node['github_token']
mapbox_key = node[:mapbox_key]
slack_url = node[:slack_url]

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
  owner name
  mode "0755"
end

file "/etc/cron.d/openaddr_crontab-collect-extracts" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# Archive collection, every other day at 5am UTC (10pm PDT)
0 5	*/2 * *	#{username}	( \
  curl -X POST -d '{"text": "Starting new collection zips..."}' "#{slack_url}" -s ; \
  openaddr-run-ec2-command \
  -a "#{aws_access_id}" \
  -s "#{aws_secret_key}" \
  --sns-arn "#{aws_sns_arn}" \
  --verbose \
  -- \
    openaddr-collect-extracts \
    -d "#{database_url}" \
    -a "#{aws_access_id}" \
    -s "#{aws_secret_key}" \
    --sns-arn "#{aws_sns_arn}" \
    --verbose \
  && curl -X POST -d '{"text": "Completed <https://#{cname}|new collection zips>."}' "#{slack_url}" -s \
  || curl -X POST -d '{"text": "Failed to complete new collection zips."}' "#{slack_url}" -s ) \
  >> /var/log/openaddr_crontab/collect-extracts.log 2>&1
CRONTAB
end

file "/etc/cron.d/openaddr_crontab-dotmap" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# Generate OpenAddresses dot map, every third day midnight UTC (5pm PDT)
0 0	*/3 * *	#{username}	( \
  curl -X POST -d '{"text": "Starting new dot map..."}' "#{slack_url}" -s ; \
  openaddr-run-ec2-command \
  --role dotmap \
  --instance-type r3.large \
  -a "#{aws_access_id}" \
  -s "#{aws_secret_key}" \
  --sns-arn "#{aws_sns_arn}" \
  --verbose \
  -- \
    openaddr-update-dotmap \
    -d "#{database_url}" \
    -m "#{mapbox_key}" \
    -a "#{aws_access_id}" \
    -s "#{aws_secret_key}" \
    --sns-arn "#{aws_sns_arn}" \
  && curl -X POST -d '{"text": "Completed <https://openaddresses.io|new dot map>."}' "#{slack_url}" -s \
  || curl -X POST -d '{"text": "Failed to complete new dot map."}' "#{slack_url}" -s ) \
  >> /var/log/openaddr_crontab/dotmap.log 2>&1
CRONTAB
end

file "/etc/cron.d/openaddr_crontab-enqueue-sources" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# Enqueue sources, Fridays 11pm UTC (4pm PDT)
0 23	* * fri	#{username}	( \
  curl -X POST -d '{"text": "Starting new batch run..."}' "#{slack_url}" -s ; \
  openaddr-enqueue-sources \
  -d "#{database_url}" \
  -a "#{aws_access_id}" \
  -s "#{aws_secret_key}" \
  --sns-arn "#{aws_sns_arn}" \
  && curl -X POST -d '{"text": "Completed <https://#{cname}/latest/set|new batch run>."}' "#{slack_url}" -s \
  || curl -X POST -d '{"text": "Failed to complete new batch run."}' "#{slack_url}" -s ) \
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
