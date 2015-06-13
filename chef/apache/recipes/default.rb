package 'apache2'

hostname = node[:cname]

file '/etc/apache2/sites-available/webhook.conf' do
  content <<-CONF
<VirtualHost *:80>
    ServerName #{hostname}
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
