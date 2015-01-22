#!/bin/bash -e

function setup_swap() {
    device=$1
    size=$2
    
    # fallocate works with ext4
    umount /mnt
    mkfs.ext4 $device
    mount $device /mnt

    # quickly allocate a large swapfile
    fallocate -l $size /mnt/swapfile
    chown root:root /mnt/swapfile
    chmod 600 /mnt/swapfile

    mkswap /mnt/swapfile
    swapon /mnt/swapfile
}

IFS=$'\n'
for line in `cat /proc/mounts`; do
    mount=`echo $line | cut -d' ' -f 2`
    if [ $mount = '/mnt' ]; then
        device=`echo $line | cut -d' ' -f 1`
    fi
done

if [ -n $device ]; then
    instance_type=`curl -s http://169.254.169.254/latest/meta-data/instance-type`

    if [ $instance_type = 'm3.xlarge' ]; then
        setup_swap $device 36G

    elif [ $instance_type = 'm3.2xlarge' ]; then
        setup_swap $device 72G
    fi
fi
