package 'python-pip'
package 'python-dev'
package 'libpq-dev'

execute "pip install -U ." do
  cwd File.join(File.dirname(__FILE__), '..', '..', '..')
end
