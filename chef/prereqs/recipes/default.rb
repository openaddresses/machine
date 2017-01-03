execute 'add-apt-repository -y ppa:openaddresses/gdal2'
execute 'apt-get update -y'

package 'python3-pip'

execute "pip3 install -U 'awscli == 1.11.22' 'botocore == 1.4.79'" do
  environment({'LC_ALL' => "C.UTF-8"}) # ...for correct encoding in python open()
end
