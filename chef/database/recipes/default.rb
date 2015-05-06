package 'postgresql'

user = node[:db_user]
pass = node[:db_pass]
host = node[:db_host]
name = node[:db_name]

if host == 'localhost' then
  psql = 'psql'
else
  psql = "psql -h '#{host}'"
end

bash "create database" do
  user 'postgres'
  environment({'DATABASE_URL' => "postgres://#{user}:#{pass}@#{host}/#{name}?sslmode=require", 'GITHUB_TOKEN' => ''})
  code <<-CODE
  # #{psql} -c "DROP DATABASE IF EXISTS #{name}";
  # #{psql} -c "DROP USER IF EXISTS #{user}";

    #{psql} -c "CREATE USER #{user} WITH SUPERUSER PASSWORD '#{pass}'";
    #{psql} -c "CREATE DATABASE #{name} WITH OWNER #{user}";

    openaddr-ci-recreate-db
  CODE
  
  # Stop as soon as an error is encountered.
  flags '-e'

  # Assume that exit=1 means the user and database were already in-place.
  returns [0, 1]
end