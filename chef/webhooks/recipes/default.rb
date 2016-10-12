bag = data_bag_item('data', 'local')
username = bag['username']
app_name = 'openaddr_webhook'

db_user = bag['db_user']
db_pass = bag['db_pass']
db_host = bag['db_host']
db_name = bag['db_name']
memcache_server = bag['memcache_server']
aws_access_id = bag['aws_access_id']
aws_secret_key = bag['aws_secret_key']
aws_sns_arn = bag['aws_sns_arn']
webhook_secrets = bag['webhook_secrets']

gag_github_status = bag['gag_github_status']
database_url = "postgres://#{db_user}:#{db_pass}@#{db_host}/#{db_name}?sslmode=require"
github_token = bag['github_token']
github_callback = bag['github_callback']
github_client_id = bag['github_client_id']
github_secret = bag['github_secret']

env_file = "/tmp/#{app_name}.conf"
procfile = File.join(File.dirname(__FILE__), '..', '..', 'Procfile-webhook')

execute 'pip3 install honcho[export]'

#
# Ensure upstart job exists.
#
file env_file do
  content <<-CONF
DATABASE_URL=#{database_url}
MEMCACHE_SERVER=#{memcache_server}
GITHUB_TOKEN=#{github_token}
GITHUB_CALLBACK=#{github_callback}
GITHUB_CLIENT_ID=#{github_client_id}
GITHUB_SECRET=#{github_secret}
GAG_GITHUB_STATUS=#{gag_github_status}
AWS_ACCESS_KEY_ID=#{aws_access_id}
AWS_SECRET_ACCESS_KEY=#{aws_secret_key}
AWS_SNS_ARN=#{aws_sns_arn}
WEBHOOK_SECRETS=#{webhook_secrets}
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

file "/etc/logrotate.d/#{app_name}-web-1" do
    content "/var/log/#{app_name}/web-1.log\n#{rotation}\n"
end

file "/etc/logrotate.d/#{app_name}-dequeue-1" do
    content "/var/log/#{app_name}/dequeue-1.log\n#{rotation}\n"
end

#
# Make it go.
#
execute "stop #{app_name}" do
  returns [0, 1]
end

execute "start #{app_name}"
