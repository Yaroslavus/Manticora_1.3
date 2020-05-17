#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 15:18:23 2020

@author: yaroslav
"""
import re
import manticore_tools as tools
# =============================================================================
#
# =============================================================================

#def old_parser(set_2, string_with_objects_to_process):
#
## =============================================================================
## From the start we create .files_list.txt and .mess.txt. =====================
##
## .files_list.txt will be consists of the lines. Each line is the absolute ====
## path to the one file. Number of lines  = number of files. ===================
##
## .mess.txt consists all temporary files will be created. Includes the ========
## .files_list. Builded through the same scheme. ===============================
## =============================================================================
#
#    with open(".files_list.txt", "tw", encoding="utf-8") as files_list:
#        with open(".mess.txt", "tw", encoding="utf-8") as mess_file:
#            mess_file.write(
#                "Made temporary file:  {}/.files_list.txt\n".format(
#                    tools.SCRIPT_DIRECTORY))
## =============================================================================
## set_2 - is "what-to-process-variable". BSM? One file? etc. ==================
## In dependence of it we must ask user to input relative path to his object. ==
## =============================================================================
#
#        if set_2 == "1": # all the data in /DATA directory
#            print("\nThe list of files to process are compiling...\n")
#            list_of_days = tools.directory_objects_parser(
#                tools.data_dir() + '/', tools.DAY_REGULAR_PATTERN)
#            for day_name in list_of_days.split():
#                abs_day_path = tools.data_dir() + '/' + day_name + '/'
#                list_of_BSM = tools.directory_objects_parser(
#                    abs_day_path, tools.BSM_REGULAR_PATTERN)
#                for BSM in list_of_BSM.split():
#                    BSM_name = abs_day_path + BSM + '/'
#                    list_of_files = tools.directory_objects_parser(
#                        BSM_name, tools.RAW_FILE_REGULAR_PATTEN)
#                    files_list.write('\n'.join(
#                        [(BSM_name + relative_file_path) for
#                         relative_file_path in list_of_files.split()]))
#                    files_list.write('\n')
#            print("The list of files to process was made. It's in the\
#                  script directory under the name  .files_list.txt\n")
#
##==============================================================================
#
#        elif set_2 == "2": # a couple of days
#
#            list_of_days = list(string_with_objects_to_process.split())
#            print("\nThe list of files to process are compiling...\n")
#            for day_name in list_of_days:
#                abs_day_path = tools.data_dir() + day_name + '/'
#                list_of_BSM = tools.directory_objects_parser(
#                    abs_day_path, tools.BSM_REGULAR_PATTERN)
#                for BSM in list_of_BSM.split():
#                    BSM_name = abs_day_path + BSM + '/'
#                    list_of_files = tools.directory_objects_parser(
#                        BSM_name, tools.RAW_FILE_REGULAR_PATTEN)
#                    files_list.write('\n'.join(
#                        [(BSM_name + relative_file_path) for
#                         relative_file_path in list_of_files.split()]))
#                    files_list.write('\n')
#            print("The list of files to process was made. It's in the\
#                  script directory under the name  .files_list.txt\n")
#
##==============================================================================
#
#        elif set_2 == "3": # a day
#
#            day_name = string_with_objects_to_process
#            print("\nThe list of files to process are compiling...\n")
#            abs_day_path = tools.data_dir() + day_name + '/'
#            list_of_BSM = tools.directory_objects_parser(
#                abs_day_path, tools.BSM_REGULAR_PATTERN)
#            for BSM in list_of_BSM.split():
#                BSM_name = abs_day_path + BSM + '/'
#                list_of_files = tools.directory_objects_parser(
#                    BSM_name, tools.RAW_FILE_REGULAR_PATTEN)
#                files_list.write('\n'.join(
#                    [(BSM_name + relative_file_path) for
#                     relative_file_path in list_of_files.split()]))
#                files_list.write('\n')
#            print("The list of files to process was made. It's in the\
#                  script directory under the name  .files_list.txt\n")
#
##==============================================================================
#
#        elif set_2 == "4": # one BSM from one day
#
#            BSM_name = string_with_objects_to_process
#            print("\nThe list of files to process are compiling...\n")
#            abs_BSM_path = tools.data_dir() + BSM_name + '/'
#            list_of_files = tools.directory_objects_parser(
#                abs_BSM_path, tools.RAW_FILE_REGULAR_PATTEN)
#            files_list.write('\n'.join(
#                [(abs_BSM_path + relative_file_path) for
#                 relative_file_path in list_of_files.split()]))
#            print("The list of files to process was made. It's in the\
#                  script directory under the name  .files_list.txt\n")
#
##==============================================================================
#
#        elif set_2 == "5": # one file
#
#            relative_file_path = string_with_objects_to_process
#            print("\nThe list of files to process are compiling...\n")
#            abs_file_path = tools.data_dir() + relative_file_path
#            files_list.write(abs_file_path)
#            print("The list of files to process was made. It's in the\
#                  script directory under the name  .files_list.txt\n")
#
##==============================================================================
#
#        elif set_2 == "6": # a couple of files
#
#            list_of_files = string_with_objects_to_process
#            print("\nThe list of files to process are compiling...\n")
#            files_list.write('\n'.join(
#                [(tools.data_dir() + relative_file_path) for
#                 relative_file_path in list_of_files.split()]))
#            print("The list of files to process was made. It's in the\
#                  script directory under the name  .files_list.txt\n")

#
# =============================================================================
# =============================================================================
#
# =============================================================================

def universal_parser(string_with_objects_to_process, start_time):

    print("The list of files to process are compiling...")
    list_with_objects_to_process = string_with_objects_to_process.split()

    list_of_bsm = []
    list_of_files = []
    list_of_days = []
    list_of_tails = []

    for item in list_with_objects_to_process:
        bsm = re.findall(tools.BSM_REGULAR_PATTERN_U, item)
        file = re.findall(tools.RAW_FILE_REGULAR_PATTERN_U, item)
        tail = re.findall(tools.TAIL_FILE_REGULAR_PATTERN_U, item)
        day = re.findall(tools.DAY_REGULAR_PATTERN_U, item)
        if bsm:
            list_of_bsm.extend(bsm)
        elif file:
            list_of_files.extend(file)
        elif tail:
            list_of_tails.extend(tail)
        elif day:
            list_of_days.extend(day)
        elif string_with_objects_to_process == 'a':
            pass
        else:
            print("ERROR: SET_3 IS FILLED WRONG!")
            tools.system_exit()

    files_list = open('.files_list.txt', 'w+', encoding="utf-8")
    files_list.close()
    with open(".mess.txt", "w+", encoding="utf-8") as mess_file:
        mess_file.write(
            "Made temporary file:  {}/.files_list.txt\n".format(
                tools.SCRIPT_DIRECTORY))

    if list_of_files:
        for file in list_of_files:
            parse_one_file(file)
    if list_of_tails:
        for tail in list_of_tails:
            parse_one_tail(tail)

    if list_of_days:
        for day in list_of_days:
            parse_one_day(day)

    if list_of_bsm:
        for bsm in list_of_bsm:
            parse_one_BSM(bsm)

    if string_with_objects_to_process == 'a':
        parse_all_data()

    print("{} {}".format(
        "The list of files to process was made.",
        "It's in the script directory under the name  .files_list.txt"))

    print("Parsing finished.")
    print(tools.time_check(start_time))
# =============================================================================
#
# =============================================================================

def parse_one_file(relative_file_path):

    with open(".files_list.txt", "a") as files_list:
        abs_file_path = "{}{}".format(
            tools.data_dir(),
            relative_file_path)
        if tools.is_exist(abs_file_path):
            files_list.write("{}\n".format(abs_file_path))
        else: print("{} DOES NOT EXIST!".format(abs_file_path))
# =============================================================================
#
# =============================================================================

def parse_one_BSM(BSM_relative_path):

    with open(".files_list.txt", "a") as files_list:
        abs_BSM_path = "{}{}/".format(
            tools.data_dir(),
            BSM_relative_path)
        if tools.is_exist(abs_BSM_path):
            list_of_files = tools.directory_objects_parser(
                abs_BSM_path,
                tools.RAW_FILE_REGULAR_PATTERN).split()
            for file in list_of_files:
                files_list.write(
                    "{}{}\n".format(
                        abs_BSM_path,
                        file))
        else: print("{} DOES NOT EXIST!".format(abs_BSM_path))
# =============================================================================
#
# =============================================================================

def parse_one_tail(tail_relative_path):

    with open(".files_list.txt", "a") as files_list:
        day_and_tail_name = tail_relative_path.split('.')
        abs_day_path = "{}{}".format(
            tools.data_dir(),
            day_and_tail_name[0])
        list_of_BSM = tools.directory_objects_parser(
            abs_day_path,
            tools.BSM_REGULAR_PATTERN).split()
        for BSM_relative_path in list_of_BSM:
            abs_BSM_path = "{}{}/".format(
                abs_day_path,
                BSM_relative_path)
            list_of_files = tools.directory_objects_parser(
                abs_BSM_path, "{}{}$".format(
                    tools.TAIL_FILE_REGULAR_PATTERN,
                    day_and_tail_name[1])).split()
            for file in list_of_files:
                files_list.write(
                    "{}{}\n".format(
                        abs_BSM_path,
                        file))
# =============================================================================
#
# =============================================================================

def parse_one_day(day_relative_path):

    with open(".files_list.txt", "a") as files_list:
        abs_day_path = "{}{}/".format(
            tools.data_dir(),
            day_relative_path)
        list_of_BSM = tools.directory_objects_parser(
            abs_day_path,
            tools.BSM_REGULAR_PATTERN).split()
        for BSM in list_of_BSM:
            BSM_name = "{}{}/".format(abs_day_path, BSM)
            list_of_files = tools.directory_objects_parser(
                BSM_name,
                tools.RAW_FILE_REGULAR_PATTERN).split()
            for relative_file_path in list_of_files:
                files_list.write("{}{}\n".format(
                    BSM_name,
                    relative_file_path))
# =============================================================================
#
# =============================================================================

def parse_all_data():

    with open(".files_list.txt", "a") as files_list:

        list_of_days = tools.directory_objects_parser(
            "{}".format(tools.data_dir()),
            tools.DAY_REGULAR_PATTERN).split()
        for day_name in list_of_days:
            abs_day_path = "{}/{}/".format(
                tools.data_dir(),
                day_name)
            list_of_BSM = tools.directory_objects_parser(
                abs_day_path,
                tools.BSM_REGULAR_PATTERN).split()
            for BSM_relative_path in list_of_BSM:
                BSM_name = "{}{}/".format(
                    abs_day_path,
                    BSM_relative_path)
                list_of_files = tools.directory_objects_parser(
                    BSM_name,
                    tools.RAW_FILE_REGULAR_PATTERN).split()
                for file_relative_path in list_of_files:
                    files_list.write("{}{}\n".format(
                        BSM_name,
                        file_relative_path))
# =============================================================================
#
# =============================================================================
