run-ec2-command.zip: run-ec2-command.py
	rm -f $@
	zip -j $@ ../openaddr/VERSION run-ec2-command.py \
		../openaddr/util/templates/task-instance-userdata.sh

	rm -rf temp-boto
	pip3 install -t temp-boto 'boto == 2.43.0'
	find temp-boto -name __pycache__ | xargs rm -rf
	cd temp-boto && zip -qr ../$@ boto
