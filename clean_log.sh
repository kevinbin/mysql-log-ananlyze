#!/bin/bash


find /home/logfiles -maxdepth 2 -type d -mtime +30 -exec rm -rf {} \;

find /var/lib/mysql/aslog/general_log_* -type f -mtime +30 -exec rm -f {} \;

