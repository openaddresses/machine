package 'python-cairo'
package 'python-pip'
package 'python-dev'
package 'libpq-dev'
package 'memcached'
package 'libffi-dev'
package 'build-essential'
package 'python-all-dev'

bash 'install_latest_gdal' do
  code <<-EOH
    curl -L 'https://github.com/mapbox/mason/archive/8ad789e39d5cf4f0e9fc351f06d7689a69758462.zip' > ~/mason.zip
    unzip ~/mason.zip -d /.mason
    cd ~/.mason
    ln -s ~/.mason/mason /usr/local/bin/mason
    mason instal gdal 1.11.2
    EOH
end

execute "pip install -U ." do
  cwd File.join(File.dirname(__FILE__), '..', '..', '..')
end

