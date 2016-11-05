local = data_bag_item('data', 'local')
username = local['username']
app_name = 'openaddr_webhook'

db_user = local['db_user']
db_pass = local['db_pass']
db_host = local['db_host']
db_name = local['db_name']
memcache_server = local['memcache_server']
aws_access_id = local['aws_access_id']
aws_secret_key = local['aws_secret_key']
aws_s3_bucket = local['aws_s3_bucket']
aws_sns_arn = local['aws_sns_arn']
webhook_secrets = local['webhook_secrets']
mapzen_key = local['mapzen_key']

gag_github_status = local['gag_github_status']
database_url = "postgres://#{db_user}:#{db_pass}@#{db_host}/#{db_name}?sslmode=require"
github_token = local['github_token']
github_callback = local['github_callback']
github_client_id = local['github_client_id']
github_secret = local['github_secret']

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
AWS_S3_BUCKET=#{aws_s3_bucket}
WEBHOOK_SECRETS=#{webhook_secrets}
MAPZEN_KEY=#{mapzen_key}
LC_ALL=C.UTF-8
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
