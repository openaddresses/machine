username = node[:username]
app_name = 'openaddr_crontab'

db_user = node[:db_user]
db_pass = node[:db_pass]
db_host = node[:db_host]
db_name = node[:db_name]
aws_access_id = node[:aws_access_id]
aws_secret_key = node[:aws_secret_key]
aws_sns_arn = node[:aws_sns_arn]
github_token = node['github_token']
mapbox_key = node[:mapbox_key]

database_url = "postgres://#{db_user}:#{db_pass}@#{db_host}/#{db_name}?sslmode=require"

env_file = "/etc/#{app_name}.conf"

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

file "/etc/logrotate.d/#{app_name}-collect-extracts" do
    content "/var/log/#{app_name}-collect-extracts.log\n#{rotation}\n"
end

file "/etc/logrotate.d/#{app_name}-dotmap" do
    content "/var/log/#{app_name}-dotmap.log\n#{rotation}\n"
end

#
#
#
file "/etc/crontab.d/#{app_name}-collect-extracts" do
    content <<-CRONTAB
# Archive collection, every other day at 5am UTC (10pm PDT)
0 5	*/2 * *	openaddr	( openaddr-run-ec2-command \
	--verbose \
	-- \
	openaddr-collect-extracts \
		-d "#{database_url}" \
		-a "#{aws_access_id}" \
		-s "#{aws_secret_key}" \
		--sns-arn "#{aws_sns_arn}" \
		--verbose ) \
		 >> /var/log/#{app_name}-collect-extracts.log 2>&1
CRONTAB
end
