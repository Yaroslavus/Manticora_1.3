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
import manticore_preprocessing
# import manticore_parser_new
# ==============================================================================
# All user sets saves in main branch (manticore_main_slow, HERE)===============
# and oly main branch has access to operate them ==============================
# ==============================================================================

print("{0}manticore 1.3 SLOW mode{0}\n\n".format("_"*39))
START_TIME = manticore_tools.what_time_is_now()
manticore_tools.mess_destroyer(START_TIME)

SET_1, SET_2, SET_3 = manticore_tools.read_input_card()
if SET_1 not in ('1', '2', '3'):
    print("ERROR: SET_1 IS WRONG!")
    manticore_tools.system_exit()

# manticore_parser_new.parser(SET_3, START_TIME)
manticore_parser.universal_parser(SET_3, START_TIME)
manticore_tools.is_preprocessing_needed(SET_1, START_TIME)
manticore_preprocessing.to_process(START_TIME)
TOTAL_DICT_OF_DAYS = manticore_preprocessing.count_tails_range(START_TIME)
manticore_preprocessing.fill_the_summary_files(TOTAL_DICT_OF_DAYS, START_TIME)

if SET_2 != '1':
    manticore_tools.mess_destroyer(START_TIME)
print("{} {}".format(
    "\nAll time for processing input data:\n",
    manticore_tools.time_check(START_TIME)))
# =============================================================================
#
# =============================================================================
