git '/var/opt/openaddresses-conform' do
  repository 'https://github.com/openaddresses/openaddresses-conform.git'
  reference '1bd251363d281c289f2fc67cbba01f86e95e9d17'
end

execute 'npm install' do
  cwd '/var/opt/openaddresses-conform'
end
