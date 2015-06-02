package 'curl'

bash 'hostname' do
  code <<-CODE
    # What is our public DNS name?
    ipaddr=$(ifconfig eth0 | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{{ print $1}}')
    fullname=`curl -s http://169.254.169.254/latest/meta-data/public-hostname --connect-timeout 5`
    shortname=`echo $fullname | cut -d. -f1`

    # Configure host name for Ubuntu.
    sed -i '/ '$fullname'/ d' /etc/hosts
    echo "$ipaddr $fullname $shortname" >> /etc/hosts
    echo $shortname > /etc/hostname
    hostname -F /etc/hostname
  CODE
  
  # Stop as soon as an error is encountered.
  flags '-e'
end
