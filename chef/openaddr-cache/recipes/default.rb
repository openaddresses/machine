include_recipe 'node'

git '/var/opt/openaddresses-cache' do
  repository 'https://github.com/openaddresses/openaddresses-cache.git'
  reference 'machine-ready'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses-cache'
  environment({'HOME' => '/', 'PATH' => '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'})
end
