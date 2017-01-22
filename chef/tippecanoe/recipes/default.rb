package 'git'
package 'build-essential'
package 'libsqlite3-dev'
package 'protobuf-compiler'
package 'libprotobuf-dev'

git '/tmp/tippecanoe' do
  repository 'https://github.com/mapbox/tippecanoe.git'
  revision '1.15.1' # c68d553
end

built = `tippecanoe -v 2>&1 | cut -d' ' -f 2`.rstrip()

execute 'make' do
  cwd '/tmp/tippecanoe'
  not_if { built == 'v1.15.1' }
  # creates '/tmp/tippecanoe/tippecanoe'
end

installed = `tippecanoe -v 2>&1 | cut -d' ' -f 2`.rstrip()

execute 'make install' do
  cwd '/tmp/tippecanoe'
  environment({'PREFIX' => '/usr/local'})
  not_if { installed == 'v1.15.1' }
  # creates '/usr/local/bin/tippecanoe'
end
