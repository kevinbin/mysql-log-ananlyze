#========================================================================
#     FileName: log_gather.sh
#         Desc: 
#       Author: hongbin
#        Email: hongbin@actionsky.com
#      Version: 1.0
#   CreateTime: 2012-02-27 11:27
#========================================================================

#!/usr/bin/env bash

source ./log_conf
source_log=source_`date +%Y-%m-%d`.log
select_log=select_`date +%Y-%m-%d`.log
change_log=change_`date +%Y-%m-%d`.log
mysql=`which mysql`
mysqladmin=`which mysqladmin`
logdate=`date "+%Y/%m/%d %H:%M:%S"`
sys_log=$log_dir/shell_log
logmemsage() {
	echo "$logdate: [ INFO ]:$@" >> $sys_log
}

die() {
	echo "$logdate: $@ in $0" | tee -a $sys_log
	exit 2
}

if [ ! -d $log_dir ]; then
	mkdir -p $log_dir
fi
[ ! -x $mysql ] && die "File $mysql does not exists."
[ ! -x $mysqladmin ] && die " File $mysqladmin does not exists."
	
logmemsage "Check target mysql whether is alive"
$mysqladmin -u$user -p$passwd -h$host -P$port ping
if [ $? != 0 ]; then
	die "target mysql host not alive"
fi
logmemsage "Set keyword ....."
keyword="`$mysql -S $sock -p$local_passwd -ss $log_database -e 'select distinct COLUMN_NAME from info_columns where is_checked=1 ' | sed -n '1h;1!H;${g;s/\n/\\\|/g;p;}'`"
sleep 1
logmemsage "Check target mysql database general_log table whether is myisam engine"

engine=`$mysql -ss -u$user -p$passwd -h$host -P$port mysql -e "select engine from information_schema.tables where table_schema='mysql' and table_name='general_log'"`
sleep 1
if [ -n "$keyword" ] && [ -n "$keyuser" ]; then
	if [ $engine == "MyISAM" ]; then
		logmemsage "Export log to $log_dir directory"
		$mysql -ss -u$user -p$passwd -h$host -P$port mysql -e "select event_time,user_host,argument from general_log where command_type='query' and argument regexp '^select.*from|^insert|^update|^delete'; truncate general_log" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' | tee $log_dir/$source_log | sed -n "/select[a-z A-Z \*].*from/p" | sed -n "/$keyword\|$keyuser/p" > $log_dir/$select_log && $mysql -S $sock -p$local_passwd $log_database -e "load data infile '$log_dir/$select_log' into table select_log fields terminated by ',' enclosed by '\"'" &

		# need to converted the tab-delimited output of the query to CSV format
		logmemsage "Import log to $log_database "
		$mysql -S $sock -p$local_passwd $log_database -e "load data infile '$log_dir/$source_log' into table all_log fields terminated by ',' enclosed by '\"';"
		$mysql -S $sock -p$local_passwd -ss $log_database -e "select event_time,user_host,info_statement from all_log where info_statement regexp '^insert|^update|^delete'" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' | sed -n "/$keyword\|$keyuser/p" > $log_dir/$change_log && $mysql -S $sock -p$local_passwd $log_database -e "load data infile '$log_dir/$change_log' into table modify_log fields terminated by ',' enclosed by '\"'" &
		exit $?	
	fi
	die "mysql.general_log table not MyISAM engine, please alter table mysql.general_log engine=myisam"
fi

if [ $engine == "MyISAM" ]; then
	logmemsage "Export log to $log_dir directory"
	$mysql -ss -u$user -p$passwd -h$host -P$port mysql -e "select event_time,user_host,argument from general_log where command_type='query' and argument regexp '^select.*from|^insert|^update|^delete'; truncate general_log" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g' | tee $log_dir/$source_log | sed -n "/select[a-z A-Z \*].*from/p" > $log_dir/$select_log && $mysql -S $sock -p$local_passwd $log_database -e "load data infile '$log_dir/$select_log' into table select_log fields terminated by ',' enclosed by '\"'" &

	# need to converted the tab-delimited output of the query to CSV format
	logmemsage "Import log to $log_database "
	$mysql -S $sock -p$local_passwd $log_database -e "load data infile '$log_dir/$source_log' into table all_log fields terminated by ',' enclosed by '\"';"
	$mysql -S $sock -p$local_passwd -ss $log_database -e "select event_time,user_host,info_statement from all_log where info_statement regexp '^insert|^update|^delete'" | sed 's/\t/","/g;s/^/"/;s/$/"/;s/\n//g'  > $log_dir/$change_log && $mysql -S $sock -p$local_passwd $log_database -e "load data infile '$log_dir/$change_log' into table modify_log fields terminated by ',' enclosed by '\"'" &
	exit $?	
fi

die "mysql.general_log table not MyISAM engine, please alter table mysql.general_log engine=myisam"

