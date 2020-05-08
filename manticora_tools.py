#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sun Apr 19 19:10:55 2020

@author: yaroslav
"""

import sys
import os
import time
import re
import struct
import math

#SCRIPT_DIRECTORY = os.getcwd()
#WFP_FILE_RATTERN = r'^.\d{8}.\d{3}.wfp$'
#TIME_PATTERN_STRING_1 = r'\d{1,3}'
#TIME_PATTERN_STRING = r'\d{1,2}:\d{1,2}:\d{1,3}:\d{1,3}.\d{1,3}.\d{1,3}'
#SUM_FILE_PATTERN = r'sum.\d{3}'
SCRIPT_DIRECTORY = os.getcwd()
RAW_FILE_REGULAR_PATTEN = r"^\d{8}\.\d{3}$"
BSM_REGULAR_PATTERN = r"^BSM\d{2}$"
DAY_REGULAR_PATTERN = r"^\d{6}(|.\d{2,3})$"
# =============================================================================
#
# =============================================================================

def syprogressbar(current_step, all_steps, symbol, operation):
    """Progressbar to show status of any operation.

    Takes current step and number of all steps.
    Also takes symbol for progressbar and name
    of the operation. Work inside the cycles and
    requires the counter of its steps"""
    print("{} {}{} {}{}".format(
        "Progress of",
        operation,
        ":",
        int(current_step/all_steps*100),
        "%"))
    print("{}{}{}{}".format(
        "[",
        int(current_step/all_steps*100)*symbol,
        int((all_steps-current_step)/all_steps*100)*"_",
        "]"))
# =============================================================================
#
# =============================================================================

def is_preprocessing_needed(set_1, start_time):
    """Fills the files_list and the mess_file without preprocessing itself

    Works if user wants only to create list of files to process or/and
    list of the temporary files that might be created through perprocessing.
    ALso if user ants only to destroy temporary files from the previous
    preprocessing"""
    if set_1 == "1":
        pass
    elif set_1 == "2":
        mess_destroyer(start_time)
    elif set_1 == "3":
        print("{}  {}".format(
            "The list of files to process was made. It's in the script directory under the name",
            ".files_list.txt\n"))
        sys.exit()
    if set_1 == "4":
        print("\nThe list of temporary files are compiling...\n")
        with open(os.getcwd() + "/.files_list.txt", "r") as files_list:
            with open(os.getcwd() + "/.mess.txt", "a") as mess_file:
                for line in files_list:
                    line = check_and_cut_the_tail(line)
                    mess_file.write("{}  {}{}".format(
                        "Made temporary file:",
                        make_PED_file_temp(line),
                        ".fpd"))
                    mess_file.write("{}  {}{}".format(
                        "\nMade temporary file:",
                        make_PED_file_temp(line),
                        ".sgm"))
                    mess_file.write("{}  {}{}".format(
                        "\nMade temporary file:",
                        make_PED_file_temp(line),
                        ".ig"))
                    mess_file.write("{}  {}{}".format(
                        "\nMade temporary file:",
                        make_PED_file_temp(line),
                        ".hdr"))
                    mess_file.write("{}  {}{}".format(
                        "\nMade temporary file:",
                        make_PED_file_temp(line),
                        ".wfp\n"))
        print("{} {}".format(
            "The list of temporary files was made. It's in the script directory under the name",
            ".mess.txt\n"))
        sys.exit()
# =============================================================================
#
# =============================================================================
def data_dir():
    """Returns absolute path of the data directory.

    Opens the configure file which contains the absolute path
    to the directory with the IACT data. And returns it."""
    with open("data_directory.config", "r") as dir_config:
        return dir_config.readlines(1)[0]
# =============================================================================
#
# =============================================================================

def mess_destroyer(start_time):
    """Destroys the temporary files.

    Takes the .mess file (from the scropt directory)
    like input information. .mess file is the place,
    whee all temporary files from previous work was wrote.
    Every line - one file. The function tries to remove it
    in cycle. Finally tries to remove .mess file too.
    This function is so tender. It can't remove any temporary
    file which is not present in .mess file. So if in some reason
    you collected the garbage in data directories but this files
    is not in .mess file, you must remove it manually. This function
    will inform you about every of them.
    So earnestly recommended use this function every time after
    preprocessing"""

    print("Mess destroying...")
    try:
        mess_file = open(os.getcwd() + "/.mess.txt", "r")
        mess_file.close()
    except FileNotFoundError:
        print("{} {}".format(
            "There are no temporary files or the list of them is damaged.",
            "Check the file '.mess.txt' in the script directory."))
    with open(".mess.txt", "r") as mess_f:
        for line in mess_f.readlines():
            line = check_and_cut_the_tail(line)
#            if line == '':
#                pass
            try:
                os.remove(line[22:])
            except FileNotFoundError:
                print("{}\t{}".format(
                    line[22:],
                    "can't been deleted by unknown reason. Try manually!"))
    try:
        os.remove(os.getcwd() + "/.mess.txt")
    except FileNotFoundError:
        print(
            "Unknown error while temporary files deleting! Please, delete manually!")

    print(time_check(start_time))
# =============================================================================
#
# =============================================================================

def make_BSM_file_temp(input_string):
    """Inserts the dot into the correct place of filename.

    Takes string with the absolute path of the raw file
    with the data. Returns the same string with the dot
    between the file directory and its name."""

    return input_string[:-12] + "." + input_string[-12:]
# =============================================================================
#
# =============================================================================

def make_PED_file_temp(input_string):
    """Inserts the dot into the correct place of filename.

    Takes string with the absolute path of the raw file
    with the pedestals. Returns the same string with the
    dot between the file directory and its name."""

    return input_string[:-18] + "PED/." + input_string[-12:-4]
# =============================================================================
#
# =============================================================================

def time_check(start_time):
    """Returns the time from the start of some operation.

    Takes start time point in seconds from the epoch
    beginning, calculates the difference between it and
    the current moment and returns it in hh:mm:ss format."""

    current_time = time.time() - start_time
    return "\nTime from the start:\n{} h {:2} m {:2} s\n".format(
        int(current_time//3600),
        int(current_time//60 - current_time//3600),
        int(current_time%60))
# =============================================================================
#
# =============================================================================

def read_input_card():
    """Reads input card and returns the whole pull of the user input sets.

    This sets contain work modes and the string with objects to process
    (files, lone BSMs, days etc.). Returns all of them outside to the
    manticora_main module."""

    with open("input_card.txt", "r") as input_card:
        ans_list = []
        for line in input_card.readlines():
            if line[0] != '#':
                ans_list.append(line[:-1])
    return [ans for ans in ans_list]

# =============================================================================
#
# =============================================================================

def directory_objects_parser(directory, object_pattern):
    """Parses the objects through the given directory.

    Takes regular expression of the object and
    the absolute path of the directory. Finally
    returns the sting with relative names of this
    objects separated by spaces in this string"""

    object_pattern = re.compile(object_pattern)
    list_files_unsorted_in_lists = [re.findall(object_pattern, k) for k in os.listdir(directory)]
    list_files_in_lists = sorted(list_files_unsorted_in_lists)
    list_of_objects_in_string = ''
    for i in list_files_in_lists:
        for j in i:
            list_of_objects_in_string += ' ' + j
    return list_of_objects_in_string[1:]
# =============================================================================
#
# =============================================================================

def what_time_is_now():
    """Returns current time in seconds from the epoch beginning."""

    return time.time()
# =============================================================================
#
# =============================================================================

def check_and_cut_the_tail(line):
    """Cuts the '\n' symbols from the tail of the string.

    If string from the file contains the '\n' in the tail,
    it may be invisible through the debugging some weird
    mistakes. You will think that it is magic. So it is better
    to check and cut it on the very first step."""

    if line[-1] == '\n':
        line = line[:-1]
    return line
# =============================================================================
#
# =============================================================================

def square_root(expression):
    """Returns square root of the expression"""

    return math.sqrt(expression)
# =============================================================================
#
# =============================================================================

def file_is_exist(file):
    """Check is file is present. Returns True or False."""

    return os.path.isfile(file)
# =============================================================================
#
# =============================================================================

def packed_bytes(rule, input_list):
    """Makes the bytes chunk from the input string using the rule.

    Takes the pointer to the list to pack.
    Rule is the strict order of objects - integers, floats etc.
    For example 'fh4B'*10 means that in bytes chunk you have 10
    same chunks and every chunk have one float (4 bytes), then
    one short integer (2 bytes), then two unsigned chars
    (each - 1 byte). So your output bytes chunk WILL BE STRICTLY
    100 bytes length.
    You must check if your input string have exatly right objects.
    For example function can't convert string to float.
    And, the worst, you can lost data through your unattention.
    For example, it will mutely make 5 from your float 5.483 if you
    will try to pack this number to unsigned integer (both 4 bytes)"""

    return struct.pack(rule, *input_list)
# =============================================================================
#
# =============================================================================

def unpacked_from_bytes(rule, bytes_chunk):
    """Makes the list of the objects from the bytes chunk using the rule

    Rule is the strict order of objects - integers, floats etc.
    For example 'fh4B'*10 means that in bytes chunk you have 10
    same chunks and every chunk have one float (4 bytes), then
    one short integer (2 bytes), then two unsigned chars
    (each - 1 byte). So your input bytes chunk HAVE TO BE 100 bytes length"""

    return struct.unpack(rule, bytes_chunk)
# =============================================================================
#
# =============================================================================
