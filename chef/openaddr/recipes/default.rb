package 'python-cairo'
package 'python-gdal'
package 'python-pip'
package 'python-dev'

execute "pip install -U ." do
  cwd File.join(File.dirname(__FILE__), '..', '..', '..')
end
