#!/bin/bash

center_mysql='192.168.8.214'
username='ddt_dba'
passwd='1@3$5qWeRt'
center_dbname='ddt_admin'
ssh_options="-o StrictHostKeyChecking=no"

RED_COL='\033[31m'
GREEN_COL='\033[32m'
YELLOW_COL='\033[33m'
ORANGE_COL='\033[34m'
PURPLE_COL='\033[35m'
END_COL='\033[m'


function Check_Mysql_Connection(){
    mysql -h ${center_mysql} -u ${username} -p"${passwd}" "${center_dbname}" -Ne ""
    result=$?
    [ "${result}" -ne 0 ] && { echo -e "${RED_COL}Cannot Connect To Mysql ${center_mysql}${END_COL}" ; exit ${result}; }
}

function Simple_Query(){
    local query=${1:-"None_"}
    local result=''
    [ "${query}" == 'None_' ] && { result='None_'; return 1; }
    mysql -h ${center_mysql} -u ${username} -p"${passwd}" "${center_dbname}" -Ne "${query}"
    
}

function Get_Server_List(){
    get_sid_query="SELECT id FROM osa_realmlist WHERE status =1 AND id < 10000" 
    sid_arry=$( Simple_Query "${get_sid_query}" )
    echo ${sid_arry[*]}
}

function Get_Slave_Ip(){
    # column "datadb" in table "osa_relamlist" stores the mongo slave ip
    local sid=${1:-"'None_'"}
    [ "${sid}" == "'None_'" ] && { echo -e "${RED_COL}Need An Sid${END_COL}"; exit 99 ;}
    local datadb_query="SELECT datadb FROM osa_realmlist WHERE id = ${sid}"
    local configdb_query="SELECT configdb FROM osa_realmlist WHERE id = ${sid}"
    tmp=$(Simple_Query "${datadb_query}")
    local datadb="$(echo ${tmp} | awk -F '@' '{print $NF}' | awk -F '/' '{print $1}')"
    tmp=$(Simple_Query "${configdb_query}")
    local configdb="$(echo ${tmp} | awk -F '@' '{print $NF}' | awk -F '/' '{print $1}')"
    [ "${datadb}" == "${configdb}" -o "${datadb}" == "" ] \
    && { echo -e "${PURPLE_COL}Can Not Find Slave Ip For GS ${sid}, Pls Find It By Yourself ${END_COL}" >&2 ; return 1; } 
    echo "${datadb}" | awk -F ':' '{print $1}'
}

function Pull_Backup(){
    local sid=${1}
    local backup_time="${2:-$(date -d '1 days ago' '+%Y-%m-%d')}"
    local backup_path="/data/backup/${backup_time}"
    [ ! -e "${backup_path}" ] && { mkdir ${backup_path} ;}
    slave_ip=$(Get_Slave_Ip ${sid})
    [ -z "${slave_ip}" ] && { exit 99; }
    #echo "$sid $slave_ip $backup_time $backup_path"
    backup_file="${backup_path}/ddt_${sid}-$(date -d ${backup_time} '+%Y%m%d').tar.bz2"
    echo "Pull Backup Of ${sid} From ${slave_ip}:${backup_file}"
    nohup rsync -acvze "ssh ${ssh_options}" root@${slave_ip}:${backup_file} "${backup_path}/" > /dev/null 2>&1 
    rsl=$?
    if [ "${rsl}" -ne 0 ]
    then
        echo "Error: Can Not Pull Backup Of ${sid} From ${slave_ip}:${backup_file}"
    fi
}

function help_info(){
     echo -e "Options:
     -h   print this menue
     -s   sid
     -d   datetime
Example: 
    拉取某个服，某一天的备份
    bash $(basename $0) -i 1 -d '2017-03-01' 
    拉取全服昨天的备份
    bash $(basename $0)
"
}

function parse_args(){
    while getopts s:d:h arg; do
        case ${arg} in
            s) arg_sid=$OPTARG ;;
            d) arg_backup_time=$OPTARG ; arg_backup_time="$(date -d "${arg_backup_time}" '+%Y-%m-%d')";;
            h) help_info; exit 9;;
            *) echo "Please specify a argument to run"; help_info ; exit 9  ;;
        esac
    done
    #echo $arg_sid $arg_backup_time
}

function main(){
    parse_args "$@"
    # if can not connect to mysql, then exit
    Check_Mysql_Connection 
    if [ ! -z "${arg_sid}" ]
    then
        Pull_Backup "${arg_sid}" "${arg_backup_time}"
    else
        for sid in $(Get_Server_List)
        do
            Pull_Backup "${sid}" "${arg_backup_time}"
        done
    fi
}

main "$@"
