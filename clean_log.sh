#!/bin/bash

find /home/logfiles -type f -mtime +30 -exec rm -f {} \;

find /home/logindex -type d -mtime +30 -exec rm -f {} \;

find /var/lib/mysql/aslog/general_log_* -type f -mtime +30 -exec rm -f {} \;
