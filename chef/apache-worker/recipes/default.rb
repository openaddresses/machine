package 'apache2'

directory '/var/www/html/oa-runone' do
  owner node[:username]
  mode '0755'
end

hostname = Socket.gethostbyname(Socket.gethostname).first

file '/etc/apache2/sites-available/worker.conf' do
  content <<-CONF
<VirtualHost *:80>
    ServerName #{hostname}
    DocumentRoot /var/www/html
    ErrorLog ${APACHE_LOG_DIR}/worker-error.log
    CustomLog ${APACHE_LOG_DIR}/worker-access.log combined
</VirtualHost>
CONF
end

execute 'a2ensite worker'
execute 'a2dissite 000-default'

execute 'service apache2 reload'
