execute 'add-apt-repository -y ppa:openaddresses/gdal2'
execute 'add-apt-repository -y ppa:openaddresses/postgis2'
execute 'apt-get update -y'

package 'python3-pip'

# Watch for compatibility between awscli, botocore, and boto3.
execute "pip3 install -U 'awscli == 1.11.50' 'botocore == 1.5.14'" do
  environment({'LC_ALL' => "C.UTF-8"}) # ...for correct encoding in python open()
end
