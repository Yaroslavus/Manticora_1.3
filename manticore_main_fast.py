#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 20:11:45 2020

@author: yaroslav
"""
# =============================================================================
#
# =============================================================================
import manticore_parser
import manticore_tools
import manticore_multiprocessing
#==============================================================================
# All user sets saves in main branch (manticore_main_fast, HERE)===============
# and oly main branch has access to operate them ==============================
#==============================================================================

print("{0}manticore 1.3 FAST mode{0}\n\n".format("_"*39))
START_TIME = manticore_tools.what_time_is_now()
manticore_tools.mess_destroyer(START_TIME)

SET_1, SET_2, SET_3 = manticore_tools.read_input_card()
if SET_1 not in ('1', '2', '3'):
    print("ERROR: SET_1 IS WRONG!")
    manticore_tools.system_exit()

manticore_parser.parser(SET_3, START_TIME)
manticore_tools.is_preprocessing_needed(SET_1, START_TIME)
manticore_multiprocessing.to_process_mult(START_TIME)
TOTAL_DICT_OF_DAYS = manticore_multiprocessing.count_tails_range_mult(START_TIME)
for key, value in TOTAL_DICT_OF_DAYS.items():
    print(key)
    for key_1, value_1 in value.items():
        print(key_1, value_1)
manticore_multiprocessing.fill_the_summary_files_mult(TOTAL_DICT_OF_DAYS, START_TIME)

if SET_2 != '1':
    manticore_tools.mess_destroyer(START_TIME)
print("{} {}".format(
    "\nAll time for processing input data:\n",
    manticore_tools.time_check(START_TIME)))
# =============================================================================
#
# =============================================================================
