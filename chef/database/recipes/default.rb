# Specify 9.4 because this recipe tends to be used only in CI.
# CircleCI has 9.4, while installing package 'postgresql' gets conflicting 9.5.
package 'postgresql-9.4'

bag = data_bag_item('data', 'local')
user = bag['db_user']
pass = bag['db_pass']
host = bag['db_host']
name = bag['db_name']
database_url = "postgres://#{user}:#{pass}@#{host}/#{name}?sslmode=require"

if host == 'localhost' then
  args = ''
else
  args = "-h '#{host}'"
end

bash "create database" do
  user 'postgres'
  environment({'DATABASE_URL' => database_url, 'GITHUB_TOKEN' => ''})
  code <<-CODE
  # psql #{args} -c "DROP DATABASE IF EXISTS #{name}";
  # psql #{args} -c "DROP USER IF EXISTS #{user}";
  # psql #{args} -c "DROP USER IF EXISTS dashboard";

    psql #{args} -c "CREATE USER dashboard";
    psql #{args} -c "CREATE USER #{user} WITH SUPERUSER PASSWORD '#{pass}'";
    psql #{args} -c "CREATE DATABASE #{name} WITH OWNER #{user}";
  CODE
  
  # Stop as soon as an error is encountered.
  flags '-e'

  # Assume that exit=1 means the user and database were already in-place.
  returns [0, 1]
end
