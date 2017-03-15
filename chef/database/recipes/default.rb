# CircleCI has 9.3, while installing package 'postgresql' gets conflicting 9.5.
package 'postgresql-9.3'

package 'libsfcgal1' do
  version '1.2.2-1~trusty2'
end

package 'liblwgeom-2.2-5' do
  version '2.2.2+dfsg-2~trusty0'
end

package 'postgresql-9.3-postgis-2.2' do
  version '2.2.2+dfsg-2~trusty0'
end

package 'postgresql-9.3-postgis-scripts' do
  version '2.2.2+dfsg-2~trusty0'
  options '--force-yes'
end

local = data_bag_item('data', 'local')
user = local['db_user']
pass = local['db_pass']
host = local['db_host']
name = local['db_name']
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
    psql #{args} -c "CREATE EXTENSION postgis" -d #{name};
  CODE
  
  # Stop as soon as an error is encountered.
  flags '-e'

  # Assume that exit=1 means the user and database were already in-place.
  returns [0, 1]
end
