git '/var/opt/openaddresses-cache' do
  repository 'https://github.com/openaddresses/openaddresses-cache.git'
  reference 'pending-fixes'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses-cache'
end
