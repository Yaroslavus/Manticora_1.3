#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri May 15 17:46:18 2020

@author: yaroslav
"""
from multiprocessing import Process, Manager
import manticore_preprocessing
import manticora_preprocessing_1
import manticore_tools as tools
# =============================================================================
#
# =============================================================================

def to_process_single_file_mult(file_to_process, strings_to_write_to_mess_file):
    """Conducts one input file through all sequence of needed operations.

    Firstly checks is needed pedestal files exist. If no:
    - calculates pedestals,
    - calculates pedestals sigmas,
    - make ignore file, which contains information is channel make noise
    (concluded from the sigmas).
    If needed pedestal files exists, skips it and calculates amplitudes
    for all events in this files and lightweight header file. By the way,
    header file is not further used anywhere. It is rather a tribute to
    tradition. What the data processing without header files?!

    The details of functions see in their own docstrings."""

    if (tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".fpd") or
            tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".sgm") or
            tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".ig")) is False:
        manticora_preprocessing_1.make_pedestals(file_to_process)
#       .fpd - fine pedestals
        print("Cleaning for pedestals")
        strings_to_write_to_mess_file.append(
            "Made temporary file:  {}.fpd\n".format(
                tools.make_PED_file_temp(file_to_process)))
        print("Creating of the report about temporary file:  {}.fpd".format(
            tools.make_PED_file_temp(file_to_process)))
#           .sgm - sigmas of pedestals
        print("Calculating pedestals sigmas...")
        strings_to_write_to_mess_file.append(
            "Made temporary file:  {}.sgm\n".format(
                tools.make_PED_file_temp(file_to_process)))
        print("Creating of the report about temporary file:  {}.sgm".format(
            tools.make_PED_file_temp(file_to_process)))
#           .ig ignore status from sigma
#           (PED_sigma < PED_average + 3*sigma_sigma)
        print("Compiling file with channels to be ignored...")
        strings_to_write_to_mess_file.append(
            "Made temporary file:  {}.ig\n".format(
                tools.make_PED_file_temp(file_to_process)))
        print("Creating of the report about temporary file:  {}.ig".format(
            tools.make_PED_file_temp(file_to_process)))
    else: print("Pedestal file for\t{}\texists.".format(file_to_process))
    if (tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".wfp") or
            tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".hdr")) is False:
        manticora_preprocessing_1.make_clean_amplitudes_and_headers(file_to_process)
#           .hdr - file with only events number #1, event number #2,
#           time of event and maroc number
        print("Creating of the header...")
        strings_to_write_to_mess_file.append(
            "Made temporary file:  {}.hdr\n".format(
                tools.make_BSM_file_temp(file_to_process)))
        print("Creating of the report about temporary file:  {}.hdr".format(
            tools.make_BSM_file_temp(file_to_process)))
#           .wfp - amplitudes minus fine pedestals
        print("Cleaning for pedestals...")
        strings_to_write_to_mess_file.append(
            "Made temporary file:  {}.wfp\n".format(
                tools.make_BSM_file_temp(file_to_process)))
        print("Creating of the report about temporary file:  {}.wfp".format(
            tools.make_BSM_file_temp(file_to_process)))
    else: print("Cleaned files for\t{}\texists.".format(file_to_process))
# =============================================================================
#
# =============================================================================

def to_process_mult(start_time):
    """Manages the conveyor of processing. Put all the files on it in order.

    Takes .files_list.txt and file by file (line by line) put them to the
    process_one_file function like children in "Another Brick In The Wall".
    In addition provides all needed interface. Exactly from here
    comes the BASH outstream through all binary files cleaning."""

    strings_to_write_to_mess_file = Manager().list()
    with open('.files_list.txt', 'r') as file_of_files:
        files_list = file_of_files.readlines()
        number_of_files_to_process = len(files_list)
        print("\nStart to process...\n")
        files_counter = 0
        chunk_size = tools.MAX_NUMBER_OF_SIMULTANEOUSLY_OPENED_FILES
        chunk_counter = 0
        chunk_to_process =\
        files_list[chunk_size*chunk_counter:chunk_size*(chunk_counter + 1)]
        while chunk_to_process:
            processes = []
            for file_to_process in chunk_to_process:
                file_to_process = tools.check_and_cut_the_tail(
                    file_to_process)
                process = Process(
                    target=to_process_single_file_mult,
                    args=(file_to_process, strings_to_write_to_mess_file))
                processes.append(process)
                process.start()
                print("\nPreparing binary files:\n")
                files_counter += 1
                tools.syprogressbar(
                    files_counter,
                    number_of_files_to_process,
                    u'\u24B7',
                    "preparing binary files",
                    start_time)
                print("\n{} is processing now.".format(file_to_process))
            for process in processes:
                process.join()
            chunk_counter += 1
            chunk_to_process =\
            files_list[chunk_size*chunk_counter:chunk_size*(chunk_counter + 1)]
    with open(".mess.txt", "a") as mess_file:
        print("Filling the mess file by reports...")
        for string in strings_to_write_to_mess_file:
            mess_file.write(string)
# =============================================================================
#
# =============================================================================

def fill_the_summary_files_mult(dict_of_days, start_time):
    """Fill the final summary files with events named (tail).sum.

    Here each existed tail.sum file is being filled by cleaned amplitudes.
    For each file function runs through all (22) repacking and cleaned data
    files with this tail in this day directory. For each data file function
    one by one reads numbers of events. Each block of data function puts to
    the correspondent place in (tail).sum file. This place is the N_1-th string
    in N_2-th blank, where N_1 - BSM number, N_2 - event number. So, this
    string in the tail.sum file will contain exatly amplitudes of THIS BSM in
    THIS event.
    Finally each tail.sum file contains full information about every event
    from two minutes that corresponds to this tail: number, and amplitudes
    of every BSM, also the time of event in every BSM and trigger-status
    and ignore-status of every channel in every BSM."""

    print("The summary files of events are fillng by data...")
    list_of_tails = []
    for day_directory, tail_dict in dict_of_days.items():
        print("The day  {}  is analizyng...".format(day_directory))
        tail_max_min_list = []
        list_of_BSM = tools.directory_objects_parser(
            day_directory, tools.BSM_REGULAR_PATTERN).split()
        for tail, max_min_list in tail_dict.items():
            list_of_tails.append(tail)
            tail_max_min_list = max_min_list
        tails_counter = 0
        chunk_size = tools.MAX_NUMBER_OF_SIMULTANEOUSLY_OPENED_FILES
        chunk_counter = 0
        chunk_to_process =\
        list_of_tails[chunk_size*chunk_counter:chunk_size*(chunk_counter + 1)]
        mm_chunk_to_process =\
        tail_max_min_list[chunk_size*chunk_counter:chunk_size*(chunk_counter + 1)]
        while chunk_to_process:
            processes = []
            for i in range(len(list_of_tails)):
                print("The total information about tail  {}  is compiling...".format(tail))
                tails_counter += 1
                process = Process(
                    target=manticore_preprocessing.create_summary_file_for_tail,
                    args=(chunk_to_process[i], mm_chunk_to_process[i], start_time,
                          list_of_BSM, day_directory, tails_counter,
                          list_of_tails))
                processes.append(process)
                process.start()
            print(tools.time_check(start_time))
            for process in processes:
                process.join()
            chunk_counter += 1
            chunk_to_process =\
            list_of_tails[chunk_size*chunk_counter:chunk_size*(chunk_counter + 1)]
            mm_chunk_to_process =\
            tail_max_min_list[chunk_size*chunk_counter:chunk_size*(chunk_counter + 1)]
        print("The summary files for  {}  have been created".format(day_directory))
    print(tools.time_check(start_time))
# =============================================================================
#
# =============================================================================

def count_tails_range_mult(start_time):
    """For all tails in .files_list.txt find maximum and minimum event number.

    For each day it finds all the files with the same tail (all the
    files ".001", then all the files ".002" etc.). There must be 22 files
    for every tail in every day in general case. And it searches the
    minimal and maximal event number for this (22) files.

    Finally it returnes the dictionary of dictionaries with next construction:
    dict_of_days = {day: dict_of_max_min},
    dict_of_max_min = {tail: [min_number, max_number]}"""

    dict_of_max_min = Manager().dict()
    dict_of_days = {}
    tails_counter = 0
    print("Evevt numbers range in parallel bsms are finding out...")
    with open('.files_list.txt', 'r') as files:
        files_list = files.readlines()
    days_set = manticore_preprocessing.set_of_days(files_list)

    for day in sorted(list(days_set)):
        tails_list = sorted(list(manticore_preprocessing.set_of_tails(files_list, day)))
        chunk_size = tools.MAX_NUMBER_OF_SIMULTANEOUSLY_OPENED_FILES
        chunk_counter = 0
        chunk_to_process =\
        tails_list[chunk_size*chunk_counter:chunk_size*(chunk_counter + 1)]
        while chunk_to_process:
            processes = []
            for tail in chunk_to_process:
                process = Process(
                    target=dict_of_num_min_max_in_tail_mult,
                    args=(tail, files_list, day, dict_of_max_min))
                processes.append(process)
                process.start()
                tails_counter += 1
                tools.syprogressbar(tails_counter,
                                len(tails_list),
                                u'\u24C2',
                                "finding out of evevt numbers range in {} tail finished".format(
                                    tail),
                                start_time)
                for process in processes:
                    process.join()
                chunk_counter += 1
                chunk_to_process =\
                tails_list[chunk_size*chunk_counter:chunk_size*(chunk_counter + 1)]
        dict_of_days[day] = dict_of_max_min
        print(tools.time_check(start_time))
    print("Finding out of evevt numbers range in parallel bsms was finished.")
    print(tools.time_check(start_time))
    return dict_of_days
# =============================================================================
#
# =============================================================================
def dict_of_num_min_max_in_tail_mult(tail, files_list, day, dict_of_max_min):
    """Find minimum and maximum event number from one tail from one day.

    Takes one tail from one day in format '001', '002' etc. Then searches
    for all the corresponding tail files (22 in general case). Runs through
    each of them, reads their event numbers.
    Finally returns list with two values: minimum event number for all this
    files and the maximum one."""

    print("Evevt numbers range in tail {} are finding out...".format(tail))
    chunk_size = 282
    num_max, num_min = 0, 0
    for file in files_list:
        file = tools.check_and_cut_the_tail(file)
        day_of_this_file = file[:-18]
        if day_of_this_file == day:
            tail_of_this_file = file[-3:]
            if tail_of_this_file == tail:
                file = tools.make_BSM_file_temp(file) + '.wfp'
                with open(file, 'rb') as wfp_file:
                    try:
                        chunk = wfp_file.read(chunk_size)
                        num_ev_bytes = chunk[4:8]
                        num_ev = tools.unpacked_from_bytes(
                            'I', num_ev_bytes)[0]
                        if num_ev < num_min:
                            num_min = num_ev
                    except Exception:
                        print("{} Chunk number {} in file {} is seems to be corrupted!\n".format(
                            "WFP FILE CORRUPTION ERROR!",
                            'ZERO',
                            file))
                    while chunk:
                        next_chunk = wfp_file.read(chunk_size)
                        if next_chunk: chunk = next_chunk
                        else: break
                    try:
                        num_ev_bytes = chunk[4:8]
                        num_ev = tools.unpacked_from_bytes(
                            'I', num_ev_bytes)[0]
                        if num_ev > num_max:
                            num_max = num_ev
                    except Exception:
                        print("{} Chunk number {} in file {} is seems to be corrupted!\n".format(
                            "WFP FILE CORRUPTION ERROR!",
                            'LAST',
                            file))
    dict_of_max_min[tail] = [num_min, num_max]
# =============================================================================
#
# =============================================================================
