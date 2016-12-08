execute 'add-apt-repository -y ppa:openaddresses/ci'
execute 'apt-get update -y'

execute "pip3 install -U 'awscli == 1.11.22' 'botocore == 1.4.79'" do
  environment({'LC_ALL' => "C.UTF-8"}) # ...for correct encoding in python open()
end
