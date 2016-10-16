execute "pip3 install -U ." do
  cwd File.join(File.dirname(__FILE__), '..', '..', '..')
  environment({'LC_ALL' => "C.UTF-8"}) # ...for correct encoding in python open()
end

