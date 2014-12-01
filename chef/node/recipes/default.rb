execute 'install node' do
  command 'curl -s http://nodejs.org/dist/v0.10.33/node-v0.10.33-linux-x64.tar.gz | tar -C /usr --strip-components 1 -xzf -'
  not_if 'which node'
end
