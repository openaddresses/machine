include_recipe 'node'

git '/var/opt/openaddresses-cache' do
  repository 'https://github.com/openaddresses/openaddresses-cache.git'
  reference 'machine-ready'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses-cache'
end
