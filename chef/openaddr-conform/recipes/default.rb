package 'gdal-bin'
include_recipe 'node'

git '/var/opt/openaddresses-conform' do
  repository 'https://github.com/openaddresses/openaddresses-conform.git'
  reference 'machine-ready'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses-conform'
  environment({'HOME' => '/', 'PATH' => '/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin'})
end
