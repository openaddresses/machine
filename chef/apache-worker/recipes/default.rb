package 'apache2'

directory '/var/www/html/oa-runone' do
  owner node[:username]
  mode '0755'
end
