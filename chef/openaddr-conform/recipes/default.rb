package 'nodejs'
package 'npm'

git '/var/opt/openaddresses-conform' do
  repository 'https://github.com/openaddresses/openaddresses-conform.git'
  reference 'ubuntu-bugfixes'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses-conform'
end
