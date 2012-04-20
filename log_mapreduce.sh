#========================================================================
#     FileName: log_mapreduce.sh
#         Desc: 
#       Author: hongbin
#        Email: hongbin@actionsky.com
#      Version: 1.0
#   CreateTime: 2012-02-27 11:27
#========================================================================
# * 0 * * * bash /path/log_mapreduce.sh 
#!/usr/bin/env bash


source ./log_conf

general_log=`date +%Y-%m-%d`_general.log
select_log=`date +%Y-%m-%d`_select.log
modify_log=`date +%Y-%m-%d`_modify.log
keyword_relate=`date +%Y-%m-%d`_relate.log

backupdate=`date +%Y_%m_%d`
mysql=`which mysql`
mysqladmin=`which mysqladmin`
select_keyword=`mktemp`
modify_keyword=`mktemp`
all_keyword=`mktemp`
logdate=`date "+%Y_%m_%d %H:%M:%S"`
sys_log=$log_dir/shell_log
memsage() {
	printf "$logdate \033[32m[ INFO ]\033[0m $@ \033[32m[OK]\033[0m\n" |tee -a $sys_log
}

die() {
	printf " $logdate \033[31m[ ERROR ]\033[0m $@ \033[31m[FAILD]\033[0m\n" |tee -a $sys_log; exit 2
}

[ ! -d $log_dir ] && mkdir -p $log_dir
[ ! -x $mysql ] && die "File $mysql does not exists."
[ ! -x $mysqladmin ] && die " File $mysqladmin does not exists."
	
memsage "Check [ $desthost ] mysql is alive ?"
$mysqladmin -u$user -p$passwd -h$desthost -P$port ping > /dev/null
[ $? != 0 ] && die "target mysql host not alive"

memsage "Check [ $desthost ] general_log table is myisam engine ?"
engine=`$mysql -ss -u$user -p$passwd -h$desthost -P$port mysql -e "select engine from information_schema.tables where table_schema='mysql' and table_name='general_log'"`
[ $engine != "MyISAM" ] && die "general_log not is myisam engine"

# 备份general_log表为general_log_bak
memsage "Rename general_log to general_log_bak on [ $desthost ] ... "
$mysql -ss -u$user -p$passwd -h$desthost -P$port mysql -e "drop table if exists general_log_bak;create table if not exists tmp_general_log like general_log;set global general_log=off;rename table general_log to general_log_bak, tmp_general_log to general_log;set global general_log=on"
mysqldata=`$mysql -ss -u$user -p$passwd -h$desthost -P$port -e "select VARIABLE_VALUE from information_schema.global_variables where variable_name='datadir';"`
# 打包general_log_bak
memsage "Compress [ ${backupdate}_general_log_bak.tar.gz ] on [ $desthost ] ... "
ssh -q -tt $os_user@$desthost "cd $mysqldata/mysql/ && tar czf /tmp/${backupdate}_general_log_bak.tar.gz ./general_log_bak*"

memsage "Remote copy [ ${backupdate}_general_log_bak.tar.gz ] to local [ $log_dir ] ... "
scp -q $os_user@$desthost:/tmp/${backupdate}_general_log_bak.tar.gz $log_dir

memsage "Cleanup [ /tmp/${backupdate}_general_log_bak.tar.gz ] on [ $desthost ] ... "
ssh -q -tt $os_user@$desthost "rm -f /tmp/${backupdate}_general_log_bak.tar.gz "

memsage "Cleanup general_log_bak table on [ $desthost ] ... "
$mysql -ss -u$user -p$passwd -h$desthost -P$port mysql -e "drop table if exists general_log_bak"

memsage "Extracting [ ${backupdate}_general_log_bak.tar.gz ] to [ $mysql_datadir/$log_database ] ... "
tar xf $log_dir/${backupdate}_general_log_bak.tar.gz -C $mysql_datadir/$log_database

[ ! -e  $mysql_datadir/$log_database/general_log_bak.MYD ] && die "Data file does not exists."
[ ! -e  $mysql_datadir/$log_database/general_log_bak.frm ] && die "Table strucate file does not exists."


gather_sql="select event_time,substring_index(concat_ws('@',substring_index(user_host,'[',1),substring_index(user_host,'[',-1)),']',1),argument from general_log_bak where command_type='query' and argument regexp '^select.*from|^insert|^update|^delete';"
memsage "Dumping [ $log_dir/$general_log ] ..."
$mysql -ss -S $sock -p$local_passwd $log_database -e "$gather_sql" | awk -F '\t' 'BEGIN{IGNORECASE=1;OFS="\t"};{sub(/@/,"\t")};{if ($4 ~ /^select/){print NR,0,$0};if ($4 ~ /^update/){print NR,1,$0};if ($4 ~ /^insert/){print NR,2,$0};if ($4 ~ /^delete/){print NR,3,$0}}' > $log_dir/$general_log 

memsage "Dumping [ $log_dir/$select_log ] ..."
awk -F '\t' 'BEGIN{IGNORECASE=1;OFS="\t"};$2 ~ /0/{print "\t"$3,$4,$5,$6}' $log_dir/$general_log > $log_dir/$select_log 

memsage "Dumping [ $log_dir/$modify_log ] ..."
awk -F '\t' 'BEGIN{IGNORECASE=1;OFS="\t"};$2 ~ /1|2|3/{print "\t"$3,$4,$5,$6}' $log_dir/$general_log > $log_dir/$modify_log 


$mysql -S $sock -p$local_passwd -ss $log_database -e "select DISTINCT(column_name) from info_columns "  > $all_keyword
declare -a keyword_array=`awk  'BEGIN{OFS="";ORS=" "}{print $0}' $all_keyword|awk '{print "("$0")"}'`
awk -v array_key="${keyword_array[*]}" 'BEGIN{nof = split(array_key,a)};{for (i in a){FS="\t";OFS="\t";if ($6 ~ a[i]){print $1,a[i]}}}' $log_dir/$general_log > $log_dir/$keyword_relate
# |awk -F '\t' 'i==1{if($1==x){print  $1"\t",$2,y;i=0;next}else{print $0}}{x=$1;y=$2;i=1}END{if(i==1) print $0}' 

function map_select () {
	
	memsage "Collecting select keyword to [ $select_keyword ] file ..."
	$mysql -S $sock -p$local_passwd -ss $log_database -e "select GROUP_CONCAT(DISTINCT(column_name)),column_id from attention_columns where select_checked=1 group by column_name" |awk '{printf("keyword=%s keyid=%s\n", $1,$2)}' > $select_keyword
	memsage "Analysing select statement ..."
	cat $select_keyword |while read line; do
		eval "$line"
		awk -F '\t' 'BEGIN{IGNORECASE=1};{if ($5 ~ /'$keyword'/){print " '$keyid'",$0} else {print $0}}' $log_dir/$select_log > $log_dir/select_$$.tmp && mv $log_dir/select_$$.tmp $log_dir/$select_log
		keyword=""
		keyid=""
	done
}

function map_modify () {
	memsage "Collecting modify keyword to [ $modify_keyword ] file ..."
	$mysql -S $sock -p$local_passwd -ss $log_database -e "select GROUP_CONCAT(DISTINCT(column_name)),column_id from attention_columns where modify_checked=1 group by column_name" |awk '{printf("keyword=%s keyid=%s\n", $1,$2)}' > $modify_keyword
	memsage "Analysing modify statement ..."
	cat $modify_keyword |while read line; do
		eval "$line"
		awk -F '\t' 'BEGIN{IGNORECASE=1};{if ($5 ~ /'$keyword'/){print " '$keyid'",$0} else {print $0}}' $log_dir/$modify_log > $log_dir/modify_$$.tmp && mv $log_dir/modify_$$.tmp $log_dir/$modify_log
		keyword=""
		keyid=""
	done
}

map_select 

memsage "Reducing [ $log_dir/$select_log ] file ..."
awk -F '\t' '$1!=""{print "\t"$0}' $log_dir/$select_log > $log_dir/select_$$.tmp && mv $log_dir/select_$$.tmp $log_dir/$select_log

memsage "Import [ $log_dir/$select_log ] file ..."
setsid $mysql -S $sock -p$local_passwd $log_database -e "load data infile '$log_dir/$select_log' into table select_log " 

map_modify

memsage "Reducing [ $log_dir/$modify_log ] file ..."
awk -F '\t' '$1!=""{print "\t"$0}' $log_dir/$modify_log > $log_dir/modify_$$.tmp && mv $log_dir/modify_$$.tmp $log_dir/$modify_log

memsage "Import [ $log_dir/$modify_log ] file to modify_log table ..."
setsid $mysql -S $sock -p$local_passwd $log_database -e "load data infile '$log_dir/$modify_log' into table modify_log " 

memsage "Import [ $log_dir/$general_log ] file to [ $log_database.general_log_$backupdate ] ..."
setsid $mysql -S $sock -p$local_passwd $log_database -e "create table if not exists general_log_$backupdate like general_log;load data infile '$log_dir/$general_log' into table general_log_$backupdate" 

memsage "Import [ $log_dir/$keyword_relate ] file to [ $log_database.keyword_relate_$backupdate ] ..."
setsid $mysql -S $sock -p$local_passwd $log_database -e "create table if not exists keyword_relate_$backupdate like keyword_relate;load data infile '$log_dir/$keyword_relate' into table keyword_relate_$backupdate" 

memsage "Cleanup general_log_bak table on localhost ... "
$mysql -ss -S $sock -p$local_passwd $log_database -e "drop table if exists general_log_bak"
memsage "Analyse Completed ..."
exit 0



