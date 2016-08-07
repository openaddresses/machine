db_user = node[:db_user]
db_pass = node[:db_pass]
db_host = node[:db_host]
db_name = node[:db_name]
aws_access_id = node[:aws_access_id]
aws_secret_key = node[:aws_secret_key]
aws_s3_bucket = node[:aws_s3_bucket]
aws_sns_arn = node[:aws_sns_arn]

gag_github_status = node['gag_github_status']
database_url = "postgres://#{db_user}:#{db_pass}@#{db_host}/#{db_name}?sslmode=require"
github_token = node['github_token']
slack_url = node['slack_url']

execute 'pip install honcho[export]'

#
# Prepare configuration file.
#
file "/etc/openaddr-collector.conf" do
  content <<-CONF
DATABASE_URL=#{database_url}
AWS_ACCESS_KEY_ID=#{aws_access_id}
AWS_SECRET_ACCESS_KEY=#{aws_secret_key}
GITHUB_TOKEN=#{github_token}
CONF
end
