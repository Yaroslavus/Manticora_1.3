#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 15:18:23 2020

@author: yaroslav
"""

import manticora_tools as tools
# =============================================================================
#
# =============================================================================

def main_parser(set_2, string_with_objects_to_process):

# =============================================================================
# From the start we create .files_list.txt and .mess.txt. =====================
#
# .files_list.txt will be consists of the lines. Each line is the absolute ====
# path to the one file. Number of lines  = number of files. ===================
#
# .mess.txt consists all temporary files will be created. Includes the ========
# .files_list. Builded through the same scheme. ===============================
# =============================================================================

    with open(".files_list.txt", "tw", encoding="utf-8") as files_list:
        with open(".mess.txt", "tw", encoding="utf-8") as mess_file:
            mess_file.write(
                "Made temporary file:  {}/.files_list.txt\n".format(
                    tools.SCRIPT_DIRECTORY))
# =============================================================================
# set_2 - is "what-to-process-variable". BSM? One file? etc. ==================
# In dependence of it we must ask user to input relative path to his object. ==
# =============================================================================

        if set_2 == "1": # all the data in /DATA directory
            print("\nThe list of files to process are compiling...\n")
            list_of_days = tools.directory_objects_parser(
                tools.data_dir() + '/', tools.DAY_REGULAR_PATTERN)
            for day_name in list_of_days.split():
                abs_day_path = tools.data_dir() + '/' + day_name + '/'
                list_of_BSM = tools.directory_objects_parser(
                    abs_day_path, tools.BSM_REGULAR_PATTERN)
                for BSM in list_of_BSM.split():
                    BSM_name = abs_day_path + BSM + '/'
                    list_of_files = tools.directory_objects_parser(
                        BSM_name, tools.RAW_FILE_REGULAR_PATTEN)
                    files_list.write('\n'.join(
                        [(BSM_name + relative_file_path) for
                         relative_file_path in list_of_files.split()]))
                    files_list.write('\n')
            print("The list of files to process was made. It's in the\
                  script directory under the name  .files_list.txt\n")

#==============================================================================

        elif set_2 == "2": # a couple of days

            list_of_days = list(string_with_objects_to_process.split())
            print("\nThe list of files to process are compiling...\n")
            for day_name in list_of_days:
                abs_day_path = tools.data_dir() + day_name + '/'
                list_of_BSM = tools.directory_objects_parser(
                    abs_day_path, tools.BSM_REGULAR_PATTERN)
                for BSM in list_of_BSM.split():
                    BSM_name = abs_day_path + BSM + '/'
                    list_of_files = tools.directory_objects_parser(
                        BSM_name, tools.RAW_FILE_REGULAR_PATTEN)
                    files_list.write('\n'.join(
                        [(BSM_name + relative_file_path) for
                         relative_file_path in list_of_files.split()]))
                    files_list.write('\n')
            print("The list of files to process was made. It's in the\
                  script directory under the name  .files_list.txt\n")

#==============================================================================

        elif set_2 == "3": # a day

            day_name = string_with_objects_to_process
            print("\nThe list of files to process are compiling...\n")
            abs_day_path = tools.data_dir() + day_name + '/'
            list_of_BSM = tools.directory_objects_parser(
                abs_day_path, tools.BSM_REGULAR_PATTERN)
            for BSM in list_of_BSM.split():
                BSM_name = abs_day_path + BSM + '/'
                list_of_files = tools.directory_objects_parser(
                    BSM_name, tools.RAW_FILE_REGULAR_PATTEN)
                files_list.write('\n'.join(
                    [(BSM_name + relative_file_path) for
                     relative_file_path in list_of_files.split()]))
                files_list.write('\n')
            print("The list of files to process was made. It's in the\
                  script directory under the name  .files_list.txt\n")

#==============================================================================

        elif set_2 == "4": # one BSM from one day

            BSM_name = string_with_objects_to_process
            print("\nThe list of files to process are compiling...\n")
            abs_BSM_path = tools.data_dir() + BSM_name + '/'
            list_of_files = tools.directory_objects_parser(
                abs_BSM_path, tools.RAW_FILE_REGULAR_PATTEN)
            files_list.write('\n'.join(
                [(abs_BSM_path + relative_file_path) for
                 relative_file_path in list_of_files.split()]))
            print("The list of files to process was made. It's in the\
                  script directory under the name  .files_list.txt\n")

#==============================================================================

        elif set_2 == "5": # one file

            relative_file_path = string_with_objects_to_process
            print("\nThe list of files to process are compiling...\n")
            abs_file_path = tools.data_dir() + relative_file_path
            files_list.write(abs_file_path)
            print("The list of files to process was made. It's in the\
                  script directory under the name  .files_list.txt\n")

#==============================================================================

        elif set_2 == "6": # a couple of files

            list_of_files = string_with_objects_to_process
            print("\nThe list of files to process are compiling...\n")
            files_list.write('\n'.join(
                [(tools.data_dir() + relative_file_path) for
                 relative_file_path in list_of_files.split()]))
            print("The list of files to process was made. It's in the\
                  script directory under the name  .files_list.txt\n")

#
# =============================================================================
