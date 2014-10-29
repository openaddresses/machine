package 'nodejs'
package 'npm'

git '/var/opt/openaddresses-cache' do
  repository 'https://github.com/openaddresses/openaddresses-cache.git'
  reference 'a4521621af838a0d359fe76ef78ede643f0a769c'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses-cache'
end
