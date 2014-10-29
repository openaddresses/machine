package 'gdal-bin'
package 'nodejs'
package 'npm'

git '/var/opt/openaddresses-conform' do
  repository 'https://github.com/openaddresses/openaddresses-conform.git'
  reference 'ubuntu-bugfixes'
end

# One package used here is tetchy about node vs. nodejs
link '/usr/bin/node' do
  to '/usr/bin/nodejs'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses-conform'
end
