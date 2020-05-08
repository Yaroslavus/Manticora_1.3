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
import manticora_preprocessing
import manticora_tools
#==============================================================================
# All user sets saves in main branch (manticora_processor, HERE)===============
# and oly main branch has access to operate them ==============================
#==============================================================================

SET_1, SET_2, SET_3, SET_4 = manticora_tools.read_input_card()
manticora_parser.main_parser(SET_2, SET_4)
START_TIME = manticora_tools.what_time_is_now()
manticora_tools.is_preprocessing_needed(SET_1, START_TIME)
manticora_preprocessing.to_process(START_TIME)
manticora_preprocessing.fill_the_summary_files(START_TIME)
if SET_3 == '2':
    manticora_tools.mess_destroyer(START_TIME)
print("{} {}".format(
    "\nAll time for processing input data:\n",
    manticora_tools.time_check(START_TIME)))
# =============================================================================
#
# =============================================================================
