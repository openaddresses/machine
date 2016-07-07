package 'python-cairo'
package 'python-pip'
package 'python-dev'
package 'libpq-dev'
package 'memcached'
package 'libffi-dev'
package 'build-essential'
package 'python-all-dev'

execute "pip install -U ." do
  cwd File.join(File.dirname(__FILE__), '..', '..', '..')
end

bash 'install_latest_gdal' do
  code <<-EOH
    wget 'http://download.osgeo.org/gdal/1.11.3/gdal-1.11.3.tar.gz'
    tar xvfz gdal-1.11.3.tar.gz
    cd gdal-1.11.3
    ./configure --with-python
    make
    sudo make install
    cd ..
    EOH
end
