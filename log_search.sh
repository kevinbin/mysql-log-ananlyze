#========================================================================
#     FileName: log-search.sh
#         Desc:
#       Author: hongbin
#        Email: hongbin@actionsky.com
#      Version: 1.0
#   CreateTime: 2012-05-09 13:42
#========================================================================

#!/usr/bin/env bash

TEMP=`getopt -oh,u:,n:,d:,k:,t:,m:,c: --long help,user:,host:,date:,keyword:,type:,time:,comment: -n 'log-search.sh' -- "$@"`
eval set -- "$TEMP"

help=""

while true ; do
    case "$1" in
        -h|--help) help="1" ; shift ;;
        -u|--user) dbuser="$2" ; shift 2 ;;
        -n|--host) host="$2" ; shift 2 ;;
        -d|--date) date="$2" ; shift 2 ;;
        -m|--time) mtime="$2"; shift 2 ;;
        -k|--keyword) keyword="$2" ; shift 2 ;;
        -t|--type) optype="$2"; shift 2 ;;
        -c|--comment) comment="$2"; shift 2 ;;
        --) shift ; break ;;
        *) echo "invaild option\n"; usage ;;
   esac
done
function usage() {
    echo "Usage: `basename $0` [-h|--help] [option] [argument]"
    echo "        -u|--dbuser= -- sql statement execute dbuser"
    echo "        -n|--host= -- sql statement from host "
    echo "        -d|--date= -- sql statement execute date"
    echo "        -m|--time= -- sql statement execute time. eg 11:34:.."
    echo "        -k|--keyword= -- filter keyword"
    echo "        -t|--type=<0 or 1> -- sql statement type. select=0, modify=1 "
    echo "        -c|--comment= -- sql statement comment "
    echo "        -h|--help -- show help"
    exit 1
}
if [ -n "$help" ]; then
    usage
fi

source /root/tomcat_log/log_conf
filedate=`echo $date | sed -e 's/-/_/g'`
function memsage() {
  local logdate=`date "+%Y_%m_%d %H:%M:%S"`
  printf "$logdate \033[32m[ INFO ]\033[0m $@ \033[32m[OK]\033[0m\n" |tee -a $sys_log
}

if [[ -z "$optype" ]]; then
  optype="1"
fi
if [[ -z "$dbuser" ]]; then
  dbuser=".*"
fi
if [[ -z "$host" ]]; then
  host=".*"
fi

if [[ $optype == "0" ]]; then
  cd $log_dir/select_history
  if [[ -n "$mtime" ]]; then
    hour=`echo $mtime |awk -F ':' '{print $1}'`
    filelist=`ls $log_dir/select_history | grep $filedate |grep $hour`
  else
    filelist=`ls $log_dir/select_history | grep $filedate`
  fi
else
  cd $log_dir/modify_history
  if [[ -n "$mtime" ]]; then
    hour=`echo $mtime |awk -F ':' '{print $1}'`
    filelist=`ls $log_dir/modify_history | grep $filedate |grep $hour`
  else
    filelist=`ls $log_dir/modify_history | grep $filedate`
  fi
fi

memsage "Reset result file"
cat /dev/null > $log_dir/analysed_file
start_time=`date +%s`

for filename in `echo $filelist`; do
  memsage "Anaylsing $filename ..."
  cat $filename \
  | awk -F '\t' -v cmt=$comment "BEGIN{IGNORECASE=1;OFS=\"\t\"}\$1 ~ /\<$date\>/{print \$0,cmt}" \
  | awk -F '\t' "BEGIN{IGNORECASE=1;OFS=\"\t\"}\$4 ~ /\w $keyword/" \
  | awk -F '\t' "BEGIN{IGNORECASE=1;OFS=\"\t\"}\$2 ~ /$dbuser/" \
  | awk -F '\t' "BEGIN{IGNORECASE=1;OFS=\"\t\"}\$1 ~ /$mtime/" \
  | awk -F '\t' "BEGIN{IGNORECASE=1;OFS=\"\t\"}\$3 ~ /$host/" \
  >> $log_dir/analysed_file
  memsage "Complete $filename anaylse ..."
done
end_time=`date +%s`
total_time=$(expr $end_time - $start_time)
memsage "Total running time : $total_time second"
memsage "Result : $log_dir/analysed_file"

