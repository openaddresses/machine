package 'python-cairo'
package 'python-gdal'

execute "pip install -U ." do
  cwd File.join(File.dirname(__FILE__), '..', '..', '..')
end
