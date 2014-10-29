git '/var/opt/openaddresses' do
  repository 'https://github.com/openaddresses/openaddresses.git'
  reference 'limited-sources'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses'
end
