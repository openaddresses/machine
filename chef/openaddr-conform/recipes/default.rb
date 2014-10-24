git '/var/opt/openaddresses-conform' do
  repository 'https://github.com/openaddresses/openaddresses-conform.git'
  reference 'master'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses-conform'
end
