package 'nodejs'
package 'npm'

execute 'npm install -g openaddresses-download' do
  not_if 'which openaddresses-download'
end
