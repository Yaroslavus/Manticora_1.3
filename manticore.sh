#!/bin/bash

######################################################################

bold=$(tput bold)
normal=$(tput sgr0)

if [ -z "$1" ]; then
    echo "EMPTY FLAG!!! Use ${bold}./manticore.sh'${normal} with one of the flags: ${bold}-f${normal} (for fast preprocessing using all kernels) or ${bold}-s${normal} (for slow preprocessing using one kernel)"
elif [ $1 == -f ]; then
    python3 manticore_main_fast.py | tee manticore_stdout_fast.txt
elif [ $1 == -s ]; then
    python3 manticore_main_slow.py | tee manticore_stdout_slow.txt
else
    echo "WRONG FLAG!!! Use ${bold}./manticore.sh'${normal} with one of the flags: ${bold}-f${normal} (for fast preprocessing using all kernels) or ${bold}-s${normal} (for slow preprocessing using one kernel)"
fi

######################################################################