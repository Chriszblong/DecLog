# umount /mnt/pmem0
# umount /mnt/pmem1
# umount /mnt/pmem2
# umount /mnt/pmem3
# umount /mnt/pmem4
# umount /mnt/pmem5
# umount /mnt/pmem6
# umount /mnt/pmem7

mount -o dax,noatime /dev/pmem0 /mnt/pmem0
mount -o dax,noatime /dev/pmem1 /mnt/pmem1
mount -o dax,noatime /dev/pmem2 /mnt/pmem2
mount -o dax,noatime /dev/pmem3 /mnt/pmem3
mount -o dax,noatime /dev/pmem4 /mnt/pmem4
mount -o dax,noatime /dev/pmem5 /mnt/pmem5
mount -o dax,noatime /dev/pmem6 /mnt/pmem6
mount -o dax,noatime /dev/pmem7 /mnt/pmem7
