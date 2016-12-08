local = data_bag_item('data', 'local')
username = local['username']
app_name = 'openaddr_worker'

db_user = local['db_user']
db_pass = local['db_pass']
db_host = local['db_host']
db_name = local['db_name']
aws_access_id = local['aws_access_id']
aws_secret_key = local['aws_secret_key']
aws_s3_bucket = local['aws_s3_bucket']
aws_sns_arn = local['aws_sns_arn']

gag_github_status = local['gag_github_status']
database_url = "postgres://#{db_user}:#{db_pass}@#{db_host}/#{db_name}?sslmode=require"
web_docroot = local['web_docroot']
mapzen_key = local['mapzen_key']
slack_url = local['slack_url']

#
# Announce status to the world.
#
file '/etc/init.d/openaddr-worker' do
  mode '0755'
  content <<-INITD
#! /bin/sh
### BEGIN INIT INFO
# Provides:          openaddr-worker
# Required-Start:    $local_fs $network
# Required-Stop:     $local_fs $network
# Should-Start:      $named
# Should-Stop:       $named
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start OpenAddresses worker.
# Description:       Start OpenAddresses worker.
### END INIT INFO
PATH=/sbin:/usr/sbin:/bin:/usr/bin

case "$1" in
  start)
    curl -X POST -d '{"text": "Worker started on `'`hostname --long`'`."}' '#{slack_url}'
    ;;
  stop)
    curl -X POST -d '{"text": "Worker stopped on `'`hostname --long`'`."}' '#{slack_url}'
    ;;
  restart|reload|force-reload)
    echo "Error: argument '$1' not supported" >&2
    exit 3
    ;;
  *)
    echo "Usage: $0 start|stop" >&2
    exit 3
    ;;
esac
INITD
end

execute "update-rc.d openaddr-worker defaults"
execute "/etc/init.d/openaddr-worker start"

env_file = "/tmp/#{app_name}.conf"
procfile = File.join(File.dirname(__FILE__), '..', '..', 'Procfile-worker')

execute 'pip3 install "honcho[export] == 0.7.1"' # note Jinja2 version dep.

#
# Ensure upstart job exists.
#
file env_file do
  content <<-CONF
WEB_DOCROOT=#{web_docroot}
DATABASE_URL=#{database_url}
GAG_GITHUB_STATUS=#{gag_github_status}
AWS_ACCESS_KEY_ID=#{aws_access_id}
AWS_SECRET_ACCESS_KEY=#{aws_secret_key}
AWS_SNS_ARN=#{aws_sns_arn}
AWS_S3_BUCKET=#{aws_s3_bucket}
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
