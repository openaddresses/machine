include_recipe 'node'

git '/var/opt/openaddresses' do
  repository 'https://github.com/openaddresses/openaddresses.git'
  reference 'master'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses'
  environment({'HOME' => '/', 'PATH' => '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'})
end
