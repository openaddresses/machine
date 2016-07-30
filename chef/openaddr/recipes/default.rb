execute "pip install -U ." do
  cwd File.join(File.dirname(__FILE__), '..', '..', '..')
  environment({'LC_CTYPE' => "C.UTF-8"})
end

