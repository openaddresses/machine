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
    wget 'http://download.osgeo.org/gdal/2.1.0/gdal-2.1.0.tar.gz'
    tar xvfz gdal-2.1.0.tar.gz
    cd gdal-2.1.0
    ./configure --with-python
    make
    make install
    cd ..
    ldconfig
    EOH
end

execute "pip install -U ." do
  cwd File.join(File.dirname(__FILE__), '..', '..', '..')
end

