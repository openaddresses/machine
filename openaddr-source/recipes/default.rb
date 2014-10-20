git '/var/opt/openaddresses' do
  repository 'https://github.com/openaddresses/openaddresses.git'
  reference 'master'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses'
end
