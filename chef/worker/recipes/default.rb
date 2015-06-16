username = node[:username]
app_name = 'openaddr_worker'

db_user = node[:db_user]
db_pass = node[:db_pass]
db_host = node[:db_host]
db_name = node[:db_name]
aws_access_id = node[:aws_access_id]
aws_secret_key = node[:aws_secret_key]
aws_sns_arn = node[:aws_sns_arn]

gag_github_status = node['gag_github_status']
database_url = "postgres://#{db_user}:#{db_pass}@#{db_host}/#{db_name}?sslmode=require"
github_token = node['github_token']
web_docroot = node['web_docroot']

env_file = "/tmp/#{app_name}.conf"
procfile = File.join(File.dirname(__FILE__), '..', '..', 'Procfile-worker')

execute 'pip install honcho[export]'

#
# Ensure upstart job exists.
#
file env_file do
  content <<-CONF
WEB_DOCROOT=#{web_docroot}
DATABASE_URL=#{database_url}
GITHUB_TOKEN=#{github_token}
GAG_GITHUB_STATUS=#{gag_github_status}
AWS_ACCESS_KEY_ID=#{aws_access_id}
AWS_SECRET_ACCESS_KEY=#{aws_secret_key}
AWS_SNS_ARN=#{aws_sns_arn}
CONF
end

execute "honcho export upstart /etc/init" do
  command "honcho -e #{env_file} -f #{procfile} export -u #{username} -a #{app_name} upstart /etc/init"
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

file "/etc/logrotate.d/#{app_name}-worker-1" do
    content "/var/log/#{app_name}/worker-1.log\n#{rotation}\n"
end

#
# Make it go.
#
execute "stop #{app_name}" do
  returns [0, 1]
end

execute "start #{app_name}"
