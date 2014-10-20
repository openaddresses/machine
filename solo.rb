#
# http://docs.opscode.com/config_rb_solo.html
#
file_cache_path  "/tmp/chef-solo"
cookbook_path    File.dirname(__FILE__)
log_level        :info
log_location     "/var/log/cheflog"
ssl_verify_mode  :verify_none
