#========================================================================
#     FileName: log_analyze.sh
#         Desc:
#       Author: hongbin
#        Email: hongbin@actionsky.com
#      Version: 1.0
#   CreateTime: 2012-02-27 11:27
#========================================================================
# * 0 * * * sh /path/log_analyze.sh
#!/usr/bin/env bash

# require configure file
source /root/tomcat_log/log_conf

# global variable
backupdate=`date +%Y_%m_%d_%H`
general_log=${backupdate}_general.log
select_log=${backupdate}_select.log
modify_log=${backupdate}_modify.log
keyword_relate=${backupdate}_relate.log
collect_keyword=${backupdate}_collect.log

select_keyword=`mktemp`
modify_keyword=`mktemp`
columns_keyword=`mktemp`
tables_keyword=`mktemp`
relate_keyword=`mktemp`

mysql=`which mysql`
mysqladmin=`which mysqladmin`

# function list
function memsage {
	local logdate=`date "+%Y_%m_%d %H:%M:%S"`
	printf "$logdate \033[32m[ INFO ]\033[0m $@ \033[32m[OK]\033[0m\n" |tee -a $sys_log
}

function die {
	local logdate=`date "+%Y_%m_%d %H:%M:%S"`
	printf "$logdate \033[3m[ ERROR ]\033[0m $@ \033[31m[FAILD]\033[0m\n" |tee -a $sys_log; exit 2
}

function mysql_local_connect {
	$mysql -ss -S $sock -p$local_passwd $log_database -e "$@"
}

function mysql_remote_connect {
	$mysql -ss -u$user -p$passwd -h$desthost -P$port mysql -e "$@"
}

function check_mysql_client {
	[ ! -d $log_dir ] && mkdir -p $log_dir
	[ ! -x $mysql ] && die "File $mysql does not exists."
	[ ! -x $mysqladmin ] && die " File $mysqladmin does not exists."
}

function check_mysql_alive {
	memsage "Check [ $desthost ] mysql is alive ?"
	$mysqladmin -u$user -p$passwd -h$desthost -P$port ping > /dev/null
	[ $? != 0 ] && die "target mysql host not alive"
}

function check_engine {
	memsage "Check [ $desthost ] general_log table is myisam engine ?"
	engine=`$mysql -ss -u$user -p$passwd -h$desthost -P$port mysql -e "select engine from information_schema.tables where table_schema='mysql' and table_name='general_log'"`
	[ $engine != "MyISAM" ] && die "general_log not is myisam engine"
}

function backup_general_log {
	# 备份general_log表为general_log_bak
	memsage "Rename general_log to general_log_bak on [ $desthost ] ... "
	backup_general_sql="drop table if exists general_log_bak;create table if not exists tmp_general_log like general_log;set global general_log=off;rename table general_log to general_log_bak, tmp_general_log to general_log;set global general_log=on"
	mysql_remote_connect "$backup_general_sql"
}

function tar_general_log {
	# 打包general_log_bak
	memsage "Compress [ ${backupdate}_general_log_bak.tar.gz ] on [ $desthost ] ... "
	mysqldata=`$mysql -ss -u$user -p$passwd -h$desthost -P$port -e "select VARIABLE_VALUE from information_schema.global_variables where variable_name='datadir';"`
	ssh -q -tt $os_user@$desthost "cd $mysqldata/mysql/ && tar czf /tmp/${backupdate}_general_log_bak.tar.gz ./general_log_bak*"
}

function remote_copy {
	memsage "Remote copy [ ${backupdate}_general_log_bak.tar.gz ] to local [ $log_dir ] ... "
	scp -q $os_user@$desthost:/tmp/${backupdate}_general_log_bak.tar.gz $log_dir
}

function clean_file {
	memsage "Cleanup [ /tmp/${backupdate}_general_log_bak.tar.gz ] on [ $desthost ] ... "
	ssh -q -tt $os_user@$desthost "rm -f /tmp/${backupdate}_general_log_bak.tar.gz "

	memsage "Cleanup general_log_bak table on [ $desthost ] ... "
	mysql_remote_connect "drop table if exists general_log_bak"
}

function extract_file {
	memsage "Extracting [ ${backupdate}_general_log_bak.tar.gz ] to [ $mysql_datadir/$log_database ] ... "
	tar xf $log_dir/${backupdate}_general_log_bak.tar.gz -C $mysql_datadir/$log_database
	[ ! -e  $mysql_datadir/$log_database/general_log_bak.MYD ] && die "Data file does not exists."
	[ ! -e  $mysql_datadir/$log_database/general_log_bak.frm ] && die "Table strucate file does not exists."
}

function dump_history_log {
	memsage "Dumping [ $log_dir/history/$general_log ] ..."
	gather_sql="select event_time,substring_index(concat_ws('@',substring_index(user_host,'[',1),substring_index(user_host,'[',-1)),']',1),replace(argument,'\`','') from general_log_bak where command_type='query' and argument regexp '^select.*from|^insert|^update|^delete' and user_host not regexp '^log_analyse';"
	mysql_local_connect "$gather_sql" | awk -F '\t' 'BEGIN{IGNORECASE=1;OFS="\t";srand('$RANDOM')}{sub(/@/,"\t")}{if ($4 ~ /^select/){print int(rand()*1000000000),0,$0};if ($4 !~ /^select/){print int(rand()*1000000000),1,$0}}' > $log_dir/history/$general_log
}

function import_history_log_hour {
	memsage "Import [ $log_dir/history/$general_log ] file to [ $log_database.general_log_$backupdate ] ..."
	mysql_local_connect "create table if not exists general_log_$backupdate like general_log;load data infile '$log_dir/history/$general_log' into table general_log_$backupdate"
}
function import_history_log_day {
	memsage "Import [ $log_dir/history/$general_log ] file to [ $log_database.general_log_`date +%Y_%m_%d` ] ..."
	mysql_local_connect "create table if not exists general_log_`date +%Y_%m_%d` like general_log;load data infile '$log_dir/history/$general_log' into table general_log_`date +%Y_%m_%d`"
}

function dump_select_log {
	memsage "Dumping [ $log_dir/$select_log ] ..."
	select_info="select '',event_time,user,host,info_statement from general_log_$backupdate where sql_type = '0' and user != '$webuser';"
	mysql_local_connect "$select_info"  > $log_dir/$select_log
}

function dump_modify_log {
	memsage "Dumping [ $log_dir/$modify_log ] ..."
	modify_info="select '',event_time,user,host,info_statement from general_log_$backupdate where sql_type != '0' and user != '$webuser';"
	mysql_local_connect "$modify_info"  > $log_dir/$modify_log
}

function analyse_table_history_log {
	memsage "Collecting table name ..."
	mysql_local_connect "select DISTINCT(table_name) from info_columns where table_name != ''"  > $tables_keyword

	declare -a tables_array=`awk 'BEGIN{OFS="";ORS=" "}{print $0}' $tables_keyword|awk '{print "("$0")"}'`
	memsage "Analysing history log hit table ..."
	line=`awk 'END{print NR}' $log_dir/history/$general_log`
	cd /tmp
	split -d -l $(($line / $awk_thread)) $log_dir/history/$general_log xxx
	for i in `ls xxx*`; do
	awk -v tables_key="${tables_array[*]}" 'BEGIN{nof = split(tables_key,t);FS="\t";OFS="\t"}{for (i in t){aa="\\<"t[i]"\\>";if ($6 ~ aa){print $1,t[i]}}}' $i > c_$i &
	done
	while true ; do
		pid=`pgrep -f "awk -v tables_key"`
		if [[ -n "$pid" ]]; then
			running_thread=`echo $pid |wc -w`
			memsage "$running_thread thread awk processing "
			sleep 10
		else
			break
		fi
	done
	cat c_xxx* > $log_dir/$keyword_relate
}

function analyse_columns_history_log {
	memsage "Collecting column name ..."
	sort -k2 -u $log_dir/$keyword_relate |awk '{print $2}' > $relate_keyword
	for i in `cat $relate_keyword`; do
		mysql_local_connect "select column_name from info_columns where table_name='$i'" >> /tmp/aa_$$.log
	done
	memsage "Analysing history log hit column ..."
	sort -u /tmp/aa_$$.log > $relate_keyword
	declare -a columns_array=`awk 'BEGIN{OFS="";ORS=" "}{print $0}' $relate_keyword|awk '{print "("$0")"}'`
	cd /tmp
	for i in `ls xxx*`; do
		awk -v columns_key="${columns_array[*]}" 'BEGIN{nof = split(columns_key,c);FS="\t";OFS="\t"}{for (i in c){aa="\\<"c[i]"\\>";if ($6 ~ aa){print $1,c[i]}}}' $i > /tmp/keyword_$i.tmp &
	done
	while true ; do
		pid=`pgrep -f "awk -v columns_key"`
		if [[ -n "$pid" ]]; then
			running_thread=`echo $pid |wc -w`
			memsage "$running_thread thread awk processing "
			sleep 10
		else
			break
		fi
	done
		cat /tmp/keyword_xxx* >> $log_dir/$keyword_relate
}

function import_keyword_relate_log {
	memsage "Import [ $log_dir/$keyword_relate ] file to [ $log_database.keyword_relate_$backupdate ] ..."
	keyword_relate_sql="create table if not exists keyword_relate_`date +%Y_%m_%d` like keyword_relate;load data infile '$log_dir/$keyword_relate' into table keyword_relate_`date +%Y_%m_%d`"
	mysql_local_connect "$keyword_relate_sql" &
}

function map_select_log {
	memsage "Collecting select keyword to [ $select_keyword ] file ..."
	map_select_sql="select GROUP_CONCAT(DISTINCT(column_name)),column_id from attention_columns where select_checked=1 group by column_name"
	mysql_local_connect "$map_select_sql" | awk '{printf("keyword=%s keyid=%s\n", $1,$2)}' > $select_keyword
	memsage "Analysing select statement ..."
	cat $select_keyword |while read line; do
		eval "$line"
		awk -F '\t' 'BEGIN{IGNORECASE=1}{if ($5 ~ /\<'$keyword'\>/){print " '$keyid'",$0} else {print $0}}' $log_dir/$select_log > $log_dir/select_$$.tmp && mv $log_dir/select_$$.tmp $log_dir/$select_log
		keyword=""
		keyid=""
	done
}

function map_modify_log {
	memsage "Collecting modify keyword to [ $modify_keyword ] file ..."
	map_modify_sql="select GROUP_CONCAT(DISTINCT(column_name)),column_id from attention_columns where modify_checked=1 group by column_name"
	mysql_local_connect "$map_modify_sql" | awk '{printf("keyword=%s keyid=%s\n", $1,$2)}' > $modify_keyword
	memsage "Analysing modify statement ..."
	cat $modify_keyword |while read line; do
		eval "$line"
		awk -F '\t' 'BEGIN{IGNORECASE=1}{if ($5 ~ /\<'$keyword'\>/){print " '$keyid'",$0} else {print $0}}' $log_dir/$modify_log > $log_dir/modify_$$.tmp && mv $log_dir/modify_$$.tmp $log_dir/$modify_log
		keyword=""
		keyid=""
	done
}

function reduce_select_log {
	memsage "Reducing [ $log_dir/$select_log ] file ..."
	awk -F '\t' '$1!=""{print "\t"$0}' $log_dir/$select_log > $log_dir/select_$$.tmp && mv $log_dir/select_$$.tmp $log_dir/$select_log
}

function import_select_log {
	memsage "Import [ $log_dir/$select_log ] file ..."
	mysql_local_connect "load data infile '$log_dir/$select_log' into table select_log " &
}

function reduce_modify_log {
	memsage "Reducing [ $log_dir/$modify_log ] file ..."
	awk -F '\t' '$1!=""{print "\t"$0}' $log_dir/$modify_log > $log_dir/modify_$$.tmp && mv $log_dir/modify_$$.tmp $log_dir/$modify_log
}

function import_modify_log {
	memsage "Import [ $log_dir/$modify_log ] file to modify_log table ..."
	mysql_local_connect "load data infile '$log_dir/$modify_log' into table modify_log " &
}

function clean_tmpfile {
	memsage "Cleanup tmp file on localhost ... "
	mysql_local_connect "drop table if exists general_log_bak;drop table if exists general_log_$backupdate"
	rm -f /tmp/tmp.*
	rm -f /tmp/c_xxx*
	rm -f /tmp/xxx*
	rm -f /tmp/keyword_xxx*
}
# function running
start_time=`date +%s`
check_mysql_client
check_mysql_alive
check_engine
backup_general_log
tar_general_log
remote_copy
clean_file
extract_file
dump_history_log
import_history_log_hour
dump_select_log
dump_modify_log
analyse_table_history_log
analyse_columns_history_log
import_keyword_relate_log
map_select_log
reduce_select_log
import_select_log
map_modify_log
reduce_modify_log
import_modify_log
import_history_log_day
clean_tmpfile
end_time=`date +%s`
total_time=$(expr $end_time - $start_time)
memsage "Total running time : $total_time second"
exit 0

