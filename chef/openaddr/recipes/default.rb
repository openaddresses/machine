package 'python-cairo'
package 'python-gdal'
package 'python-pip'
package 'python-dev'
package 'libpq-dev'
package 'memcached'
package 'libffi-dev'
package 'gdal-bin'
package 'libgdal-dev'

# Skip pip install under Circle CI, since it will be ignored
# in favor of virtualenv-specific install later on.
if node['circleci-environment'] != true then
  execute "pip install -U ." do
    cwd File.join(File.dirname(__FILE__), '..', '..', '..')
  end
end

