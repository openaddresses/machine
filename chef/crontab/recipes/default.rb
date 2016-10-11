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
# Ensure upstart job exists.
#
file env_file do
  content <<-CONF
DATABASE_URL=#{database_url}
AWS_ACCESS_KEY_ID=#{aws_access_id}
AWS_SECRET_ACCESS_KEY=#{aws_secret_key}
AWS_SNS_ARN=#{aws_sns_arn}
GITHUB_TOKEN=#{github_token}
MAPBOX_KEY=#{mapbox_key}
CONF
end

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
