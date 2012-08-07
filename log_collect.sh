#========================================================================
#     FileName: log_collect.sh
#         Desc:
#       Author: hongbin
#        Email: hongbin@actionsky.com
#      Version: 1.0
#   CreateTime: 2012-02-27 11:27
#========================================================================
#!/usr/bin/env bash

# require configure file
source /root/tomcat_log/log_conf
export LANG=en_US.UTF-8
# global variable
backupdate=`date +%Y_%m_%d_%H`
general_log=${backupdate}_general.log
select_log=${backupdate}_select.log
modify_log=${backupdate}_modify.log

select_keyword=`mktemp`
modify_keyword=`mktemp`

mysql=`which mysql`
mysqladmin=`which mysqladmin`


# function list
function memsage() {
  local logdate=`date "+%Y_%m_%d %H:%M:%S"`
  printf "$logdate \033[32m[ INFO ]\033[0m $@ \033[32m[OK]\033[0m\n" |tee -a $sys_log
}

function die() {
  local logdate=`date "+%Y_%m_%d %H:%M:%S"`
  printf "$logdate \033[3m[ ERROR ]\033[0m $@ \033[31m[FAILD]\033[0m\n" |tee -a $sys_log; exit 2
}

function mysql_local_connect() {
  $mysql -ss -S $sock -p$local_passwd $log_database -e "$@"
}

function mysql_remote_connect() {
  $mysql -ss -u$user -p$passwd -h$desthost -P$port mysql -e "$@"
}

function check_mysql_client() {
  [ ! -d $log_dir/history ] && mkdir -p $log_dir/history
  [ ! -x $mysql ] && die "File $mysql does not exists."
  [ ! -x $mysqladmin ] && die " File $mysqladmin does not exists."
}

function check_mysql_alive() {
  memsage "Check [ $desthost ] mysql is alive ?"
  $mysqladmin -u$user -p$passwd -h$desthost -P$port ping > /dev/null
  [ $? != 0 ] && die "target mysql host not alive"
}

function check_engine() {
  memsage "Check [ $desthost ] general_log table is myisam engine ?"
  engine=`$mysql -ss -u$user -p$passwd -h$desthost -P$port mysql -e "select engine from information_schema.tables where table_schema='mysql' and table_name='general_log'"`
  [ $engine != "MyISAM" ] && die "general_log not is myisam engine"
}

function backup_general_log() {
  # 备份general_log表为general_log_bak
  memsage "Rename general_log to general_log_bak on [ $desthost ] ... "
  backup_general_sql="drop table if exists general_log_bak;create table if not exists tmp_general_log like general_log;set global general_log=off;rename table general_log to general_log_bak, tmp_general_log to general_log;set global general_log=on"
  mysql_remote_connect "$backup_general_sql"
}

function tar_general_log() {
  # 打包general_log_bak
  memsage "Compress [ ${backupdate}_general_log_bak.tar.gz ] on [ $desthost ] ... "
  mysqldata=`$mysql -ss -u$user -p$passwd -h$desthost -P$port -e "select VARIABLE_VALUE from information_schema.global_variables where variable_name='datadir';"`
  ssh -q -tt $os_user@$desthost "cd $mysqldata/mysql/ && tar czf /tmp/${backupdate}_general_log_bak.tar.gz ./general_log_bak*"
}

function remote_copy() {
  memsage "Remote copy [ ${backupdate}_general_log_bak.tar.gz ] to local [ $log_dir ] ... "
  scp -q $os_user@$desthost:/tmp/${backupdate}_general_log_bak.tar.gz $log_dir
}

function clean_file() {
  memsage "Cleanup [ /tmp/${backupdate}_general_log_bak.tar.gz ] on [ $desthost ] ... "
  ssh -q -tt $os_user@$desthost "rm -f /tmp/${backupdate}_general_log_bak.tar.gz "

}

function extract_file() {
  mysql_datadir=`$mysql -ss -S $sock -p$local_passwd $log_database -e "select VARIABLE_VALUE from information_schema.global_variables where variable_name='datadir';"`
  memsage "Extracting [ ${backupdate}_general_log_bak.tar.gz ] to [ $mysql_datadir/$log_database ] ... "
  tar xf $log_dir/${backupdate}_general_log_bak.tar.gz -C $mysql_datadir/$log_database
  [ ! -e  $mysql_datadir/$log_database/general_log_bak.MYD ] && die "Data file does not exists."
  [ ! -e  $mysql_datadir/$log_database/general_log_bak.frm ] && die "Table strucate file does not exists."
}

function dump_history_log() {
  memsage "Dumping [ $log_dir/history/$general_log ] ..."
  gather_sql="select event_time,substring_index(concat_ws('@',substring_index(user_host,'[',1),substring_index(user_host,'[',-1)),']',1),replace(argument,'\`','') from general_log_bak where command_type='query' and argument regexp '^select.*from|^insert|^update|^delete' and user_host not regexp '^log_analyse';"
  mysql_local_connect "$gather_sql" | awk -F '\t' 'BEGIN{IGNORECASE=1;OFS="\t"}{sub(/@/,"\t")}{if ($4 ~ /^select/){print "",0,$0};if ($4 !~ /^select/){print "",1,$0}}' > $log_dir/history/$general_log
}


function import_history_log_hour {
  memsage "Import [ $log_dir/history/$general_log ] file to [ $log_database.general_log_$backupdate ] ..."
  mysql_local_connect "create table if not exists general_log_$backupdate like general_log;load data infile '$log_dir/history/$general_log' into table general_log_$backupdate"
}

function import_history_log_day {
  memsage "Import [ $log_dir/history/$general_log ] file to [ $log_database.general_log_`date +%Y_%m_%d` ] ..."
  mysql_local_connect "create table if not exists general_log_`date +%Y_%m_%d` like general_log;load data infile '$log_dir/history/$general_log' into table general_log_`date +%Y_%m_%d`" &
}

function dump_history_select_log() {
  memsage "Dumping [ $log_dir/history/$select_log ] ..."
  select_info="select event_time,user,host,info_statement from general_log_$backupdate where sql_type = '0';"
  [ ! -d $log_dir/select_history ] && mkdir -p $log_dir/select_history
  mysql_local_connect "$select_info"  > $log_dir/select_history/$select_log
}

function dump_history_modify_log() {
  memsage "Dumping [ $log_dir/history/$modify_log ] ..."
  modify_info="select event_time,user,host,info_statement from general_log_$backupdate where sql_type != '0';"
  [ ! -d $log_dir/modify_history ] && mkdir -p $log_dir/modify_history
  mysql_local_connect "$modify_info"  > $log_dir/modify_history/$modify_log
}

function dump_select_log() {
  memsage "Dumping [ $log_dir/$select_log ] ..."
  select_info="select '',event_time,user,host,info_statement from general_log_$backupdate where sql_type = '0' and user != '$webuser';"
  mysql_local_connect "$select_info"  > $log_dir/$select_log
}

function dump_modify_log() {
  memsage "Dumping [ $log_dir/$modify_log ] ..."
  modify_info="select '',event_time,user,host,info_statement from general_log_$backupdate where sql_type != '0' and user != '$webuser';"
  mysql_local_connect "$modify_info"  > $log_dir/$modify_log
}

function map_select_log () {
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

function map_modify_log () {
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

function reduce_select_log() {
  memsage "Reducing [ $log_dir/$select_log ] file ..."
  awk -F '\t' '$1!=""{print "\t"$0}' $log_dir/$select_log > $log_dir/select_$$.tmp && mv $log_dir/select_$$.tmp $log_dir/$select_log
}

function import_select_log() {
  memsage "Import [ $log_dir/$select_log ] file ..."
  mysql_local_connect "load data infile '$log_dir/$select_log' into table select_log " &
}

function reduce_modify_log() {
  memsage "Reducing [ $log_dir/$modify_log ] file ..."
  awk -F '\t' '$1!=""{print "\t"$0}' $log_dir/$modify_log > $log_dir/modify_$$.tmp && mv $log_dir/modify_$$.tmp $log_dir/$modify_log
}

function import_modify_log() {
  memsage "Import [ $log_dir/$modify_log ] file to modify_log table ..."
  mysql_local_connect "load data infile '$log_dir/$modify_log' into table modify_log " &
}

function clean_tmpfile() {
  memsage "Cleanup tmp file on localhost ... "
  mysql_local_connect "drop table if exists general_log_bak;drop table if exists general_log_$backupdate"
  rm -f /tmp/tmp.*
  rm -f $log_dir/history/$general_log
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
import_history_log_day
# dump_history_select_log
# dump_history_modify_log
dump_select_log
dump_modify_log
map_select_log
reduce_select_log
import_select_log
map_modify_log
reduce_modify_log
import_modify_log
clean_tmpfile
end_time=`date +%s`
total_time=$(expr $end_time - $start_time)
memsage "Total running time : $total_time second"
exit 0

