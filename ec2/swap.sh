#!/usr/bin/bash -e

IFS=$'\n'
for line in `cat /proc/mounts`; do
    mount=`echo $line | cut -d' ' -f 2`
    if [ $mount = '/mnt' ]; then
        device=`echo $line | cut -d' ' -f 1`
    fi
done

instance_type=`curl -s http://169.254.169.254/latest/meta-data/instance-type`

if [ -n $device -a $instance_type = 'm3.xlarge' ]; then
    umount /mnt
    mkfs.ext4 $device
    mount $device /mnt

    fallocate -l 36G /mnt/swapfile
    chown root:root /mnt/swapfile
    chmod 600 /mnt/swapfile

    mkswap /mnt/swapfile
    swapon /mnt/swapfile
fi
