git '/var/opt/openaddresses-conform' do
  repository 'https://github.com/openaddresses/openaddresses-conform.git'
  reference 'fix-cache-truncation'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses-conform'
end
