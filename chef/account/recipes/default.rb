local = data_bag_item('data', 'local')
name = local['username']

group name
user name do
  gid name
  home "/home/#{name}"
end

directory "/home/#{name}" do
  owner name
  group name
  mode "0755"
end

file "/etc/cron.d/openaddr_account-cleanup-tempdir" do
    content <<-CRONTAB
PATH=/usr/local/sbin:/usr/local/bin:/sbin:/bin:/usr/sbin:/usr/bin

# Clean up week-old contents of /tmp
0 0	* * *	#{name}	find /tmp -depth -user #{name} -mtime +7 -delete
CRONTAB
end
