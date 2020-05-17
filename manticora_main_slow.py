#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 20:11:45 2020

@author: yaroslav
"""
# =============================================================================
#
# =============================================================================
import manticora_parser
import manticora_tools
import manticora_preprocessing
#==============================================================================
# All user sets saves in main branch (manticora_processor, HERE)===============
# and oly main branch has access to operate them ==============================
#==============================================================================

print("{0}Manticora 1.3 SLOW mode{0}\n\n".format("_"*39))
START_TIME = manticora_tools.what_time_is_now()
manticora_tools.mess_destroyer(START_TIME)

SET_1, SET_2, SET_3 = manticora_tools.read_input_card()
if (SET_1 != '1' and SET_1 != '2' and SET_1 != '3'):
    print("ERROR: SET_1 IS WRONG!")
    manticora_tools.system_exit()

manticora_parser.universal_parser(SET_3, START_TIME)
manticora_tools.is_preprocessing_needed(SET_1, START_TIME)
manticora_preprocessing.to_process(START_TIME)
TOTAL_DICT_OF_DAYS = manticora_preprocessing.count_tails_range(START_TIME)
manticora_preprocessing.fill_the_summary_files(TOTAL_DICT_OF_DAYS, START_TIME)

if SET_2 != '1':
    manticora_tools.mess_destroyer(START_TIME)
print("{} {}".format(
    "\nAll time for processing input data:\n",
    manticora_tools.time_check(START_TIME)))
# =============================================================================
#
# =============================================================================
