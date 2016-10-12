package 'apache2'

local = data_bag_item('data', 'local')
hostname = local['cname']

file '/etc/apache2/sites-available/webhook.conf' do
  content <<-CONF
<VirtualHost *:80>
    ServerName #{hostname}
    ErrorLog ${APACHE_LOG_DIR}/webhooks-error.log
    CustomLog ${APACHE_LOG_DIR}/webhooks-access.log combined

    <Location />
        ProxyPass http://127.0.0.1:5000/
        ProxyPassReverse http://127.0.0.1:5000/
    </Location>
    <Proxy http://127.0.0.1:5000/*>
        Allow from all
    </Proxy>
</VirtualHost>
CONF
end

execute 'a2enmod proxy'
execute 'a2enmod proxy_http'

execute 'a2ensite webhook'
execute 'a2dissite 000-default'

execute 'service apache2 reload'
