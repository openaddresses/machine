import unittest
from mock import patch, call, Mock

def not_implemented(*args, **kwargs):
    raise NotImplementedError()

class TestEC2 (unittest.TestCase):

    @patch('shutil.rmtree')
    @patch('tempfile.mkdtemp')
    @patch('subprocess.check_call')
    @patch('boto.ec2.EC2Connection')
    @patch('boto.ec2.blockdevicemapping.BlockDeviceType')
    @patch('boto.ec2.blockdevicemapping.BlockDeviceMapping')
    def test_main(self, BlockDeviceMapping, BlockDeviceType, EC2Connection, check_call, mkdtemp, rmtree):
        '''
        '''
        ec2_connection = Mock()
        ec2_connection.get_spot_price_history.return_value = []
        ec2_connection.request_spot_instances.side_effect = not_implemented
    
        EC2Connection.return_value = ec2_connection
        BlockDeviceMapping.return_value = dict()
        BlockDeviceType.return_value = 'fake device type'
        mkdtemp.return_value = '/temp'
    
        from ..run import run_ec2, parser
        
        with self.assertRaises(NotImplementedError) as e:
            argv = '-r', 'http://github/openaddresses/machine', '-b', 'master', 'bucket-name'
            run_ec2(parser.parse_args(argv))
    
        check_call.assert_has_calls([
            call(('git', 'clone', '-q', '-b', 'master', '--bare', 'http://github/openaddresses/machine', '/temp/repo')),
            call(('git', '--git-dir', '/temp/repo', 'archive', 'master', '-o', '/temp/archive.tar')),
            call(('gzip', '/temp/archive.tar')),
            ])
    
        rmtree.assert_called_with('/temp/repo')
    
        ec2_connection.request_spot_instances.assert_called_with(
            1.01, 'ami-4ae27e22', key_name='oa-keypair',
            block_device_map={'/dev/sda1': 'fake device type'},
            security_groups=['default'], instance_type='m3.xlarge',
            user_data="#!/bin/sh -ex\napt-get update -y\nwhile [ ! -f /tmp/machine.tar.gz ]; do sleep 10; done\n\necho 'extracting' > /var/run/machine-state.txt\nmkdir /tmp/machine\ntar -C /tmp/machine -xzf /tmp/machine.tar.gz\n\necho 'installing' > /var/run/machine-state.txt\n/tmp/machine/ec2/swap.sh\n/tmp/machine/chef/run.sh batchmode\n\necho 'processing' > /var/run/machine-state.txt\nopenaddr-process -a None -s None -l log.txt /var/opt/openaddresses/sources bucket-name\n\necho 'terminating' > /var/run/machine-state.txt\nshutdown -h now\n"
            )
