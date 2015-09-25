package 'nodejs'
package 'npm'

git '/tmp/mapbox-upload' do
  repository 'https://github.com/mapbox/mapbox-upload.git'
  revision 'v4.2.0'
end

version = `npm list -g mapbox-upload | grep '└' | cut -d@ -f 2`.rstrip()

execute 'npm install -g' do
  cwd '/tmp/mapbox-upload'
  not_if { version == '4.2.0' }
  creates '/usr/local/lib/node_modules/mapbox-upload/bin/upload.js'
end
