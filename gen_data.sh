#========================================================================
#     FileName: gen_data.sh
#         Desc: 
#       Author: hongbin
#        Email: hongbin@actionsky.com
#      Version: 1.0
#   CreateTime: 2012-02-28 17:20
#========================================================================

#!/usr/bin/env bash

source ./log_conf
mysql=`which mysql`
$mysql -S $sock -p$local_passwd -e " create database if not EXISTS testlog; use testlog; create table if not EXISTS tt (i int)"
for i in {1..1000}; do
	$mysql -S $sock -p$local_passwd testlog -e "insert into tt value($i); select * from tt; update tt set i=2000 where i=$i; delete from tt where i=2000" 
	# $mysql -S $sock -p$local_passwd testlog -e "select * from tt"
	# $mysql -S $sock -p$local_passwd testlog -e "update tt set i=2000 where i=$i"
	# $mysql -S $sock -p$local_passwd testlog -e "delete from tt where i=2000"
done
