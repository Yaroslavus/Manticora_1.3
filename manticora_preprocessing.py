#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 23:45:35 2020

@author: yaroslav
"""
import re
import manticora_tools as tools
# =============================================================================
#
# =============================================================================

def to_process(start_time):
    """Manages the conveyor of processing. Put all the files on it in order.

    Takes .files_list.txt and file by file (line by line) put them to the
    process_one_file function like children in "Another Brick In The Wall".
    In addition provides all needed interface. Exactly from here
    comes the BASH outstream through all binary files cleaning."""

    with open('.files_list.txt', 'r') as file_of_files:
        number_of_files_to_process = len(file_of_files.readlines())
    with open(".files_list.txt", "r") as files_list:
        print("\nStart to process...\n")
        counter = 0
        for file_to_process in files_list:
            file_to_process = tools.check_and_cut_the_tail(file_to_process)
            to_process_single_file(file_to_process)
            counter += 1
            print("\nPreparing binary files:\n")
            tools.syprogressbar(
                counter,
                number_of_files_to_process,
                u'\u2589',
                " preparing binary files")
            print("\n{} is processing now.".format(file_to_process))
            print(tools.time_check(start_time))
# =============================================================================
#
# =============================================================================

def to_process_single_file(file_to_process):
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

    with open(".mess.txt", "a") as mess_file:
        if (tools.file_is_exist(tools.make_PED_file_temp(file_to_process) + ".fpd") or
                tools.file_is_exist(tools.make_PED_file_temp(file_to_process) + ".sgm") or
                tools.file_is_exist(tools.make_PED_file_temp(file_to_process) + ".ig")) is False:
            make_pedestals(file_to_process)
#           .fpd - fine pedestals
            print("Cleaning for pedestals")
            mess_file.write("Made temporary file:  {}.fpd\n".format(
                tools.make_PED_file_temp(file_to_process)))
            print("Made temporary file:  {}.fpd".format(
                tools.make_PED_file_temp(file_to_process)))
#           .sgm - sigmas of pedestals
            print("Calculating pedestals sigmas...")
            mess_file.write("Made temporary file:  {}.sgm\n".format(
                tools.make_PED_file_temp(file_to_process)))
            print("Made temporary file:  {}.sgm".format(
                tools.make_PED_file_temp(file_to_process)))
#           .ig ignore status from sigma
#           (PED_sigma < PED_average + 3*sigma_sigma)
            print("Compiling file with channels to be ignored...")
            mess_file.write("Made temporary file:  {}.ig\n".format(
                tools.make_PED_file_temp(file_to_process)))
            print("Made temporary file:  {}.ig".format(
                tools.make_PED_file_temp(file_to_process)))
        else: print("Pedestal file exists.")
    if (tools.file_is_exist(tools.make_BSM_file_temp(file_to_process) + ".wfp") or
            tools.file_is_exist(tools.make_BSM_file_temp(file_to_process) + ".hdr")) is False:
        make_clean_amplitudes_and_headers(file_to_process)
#       .hdr - file with only events number #1, event number #2,
#       time of event and maroc number
        print("Creating of the header...")
        mess_file.write("Made temporary file:  {}.hdr\n".format(
            tools.make_BSM_file_temp(file_to_process)))
        print("Made temporary file:  {}.hdr".format(
            tools.make_BSM_file_temp(file_to_process)))
#       .wfp - amplitudes minus fine pedestals
        print("Cleaning for pedestals...")
        mess_file.write("Made temporary file:  {}.wfp\n".format(
            tools.make_BSM_file_temp(file_to_process)))
        print("Made temporary file:  {}.wfp".format(
            tools.make_BSM_file_temp(file_to_process)))
    else: print("Cleaned files exists.")
#==============================================================================
#
# =============================================================================

def make_pedestals(file_to_process):

    with open(file_to_process[:-18] + "PED/" + file_to_process[-12:-4] + ".ped", "rb") as ped_fin:
        counter = [0]*64
        PED_av = [0]*64
        PED_square = [0]*64
        PED_sigma = [0]*64
        PED = [0]*64
        ignore_status = [0]*64
        sigma_sigma = 0
        chunk_size = 156
        codes_beginning_byte = 24
        codes_ending_byte = 152
        number_of_codes = 64
        chunk = ped_fin.read(chunk_size)
        while chunk:
            chunk_bytes = bytearray(chunk)
            codes_array = tools.unpacked_from_bytes(
                '<64h',
                chunk_bytes[codes_beginning_byte:codes_ending_byte])
            cycle_ampl_matrix = [0]*64
            for i in range(number_of_codes):
                cycle_ampl_matrix[i] = codes_array[i]/4
            for i in range(len(cycle_ampl_matrix)):
                PED[i] += cycle_ampl_matrix[i]
                PED_square[i] += cycle_ampl_matrix[i]*cycle_ampl_matrix[i]
                counter[i] += 1
            chunk = ped_fin.read(chunk_size)
    for i in range(len(PED)):
        PED_av[i] = PED[i]/counter[i]
    for i in range(len(PED)):
        PED_square[i] = PED_square[i] / (counter[i] - 1)
#   Deviation from real SIGMA is less then 0.5%
    for i in range(len(PED)):
        PED_sigma[i] = tools.square_root(
            abs(PED_square[i] - (PED_av[i]*PED_av[i]*counter[i])/(counter[i] - 1)))
    with open(tools.make_PED_file_temp(file_to_process) +".fpd", "wb") as ped_fout:
        peds_average = tools.packed_bytes('<64f', PED_av)
        ped_fout.write(peds_average)
    with open(tools.make_PED_file_temp(file_to_process) +".sgm", "wb") as sigma_fout:
        peds_sigma = tools.packed_bytes('<64f', PED_sigma)
        sigma_fout.write(peds_sigma)

    sigma_av = sum(PED_sigma)/len(PED_sigma)
    for item in PED_sigma:
        sigma_sigma += (sigma_av - item)**2
    sigma_sigma = tools.square_root(sigma_sigma/len(PED_sigma))
    for i in range(0, len(PED), 2):
        if ((PED_sigma[i] > PED_av[i] + 3*sigma_sigma) or
                (PED_sigma[i] < -1*PED_av[i] - 3*sigma_sigma)):
            ignore_status[i] = 0
        else: ignore_status[i] = 2
    for i in range(1, len(PED), 2):
        if ((PED_sigma[i] > PED_av[i] + 3*sigma_sigma) or
                (PED_sigma[i] < -1*PED_av[i] - 3*sigma_sigma)):
            ignore_status[i] = 0
        else: ignore_status[i] = 1

    with open(tools.make_PED_file_temp(file_to_process) +".ig", "wb") as ignore_status_fout:

        ignore_pack = tools.packed_bytes('<64B', ignore_status)
        ignore_status_fout.write(ignore_pack)
# =============================================================================
#
# =============================================================================

def make_clean_amplitudes_and_headers(file_to_process):

    chunk_size = 156
    codes_beginning_byte = 24
    codes_ending_byte = 152
    number_1_beginning_byte = 4
#   number_2_beginning_byte = 8
#   event_time_beginning_byte = 12
    maroc_number_byte = 20
#   number_of_codes = 64

    with open(tools.make_PED_file_temp(file_to_process) + ".fpd", "rb") as ped_fin:
        peds = ped_fin.read()
        peds_array = tools.unpacked_from_bytes('<64f', peds)
    with open(tools.make_PED_file_temp(file_to_process) + ".ig", "rb") as ig_fin:
        ig_bytes = ig_fin.read()
        ig_array = tools.unpacked_from_bytes('<64B', ig_bytes)

    with open(file_to_process, "rb") as codes_fin:
        with open(tools.make_BSM_file_temp(file_to_process) + ".wfp", "wb") as cleaned_file:
            with open(tools.make_BSM_file_temp(file_to_process) + ".hdr", "wb") as header_file:

                chunk = codes_fin.read(chunk_size)
#               counter = [0]*64
#               ampl_av = [0]*64
#               ampl_square = [0]*64
#               ampl_sigma = [0]*64
#               ampl = [0]*64
                while chunk:
                    cleaned_amplitudes = [0]*96
                    chunk_bytes = bytearray(chunk)
                    codes_array = tools.unpacked_from_bytes(
                        '<64h', chunk_bytes[codes_beginning_byte:codes_ending_byte])
#                   a = [0]*64
#                   for i in range (len (a)):
#                       ampl [i] += codes_array [i]
#                       ampl_square [i] += codes_array [i]*codes_array [i]
#                       counter [i] += 1
                    for i in range(0, len(cleaned_amplitudes), 3):
                        if codes_array[2*i//3] <= 1800:
                            cleaned_amplitudes[i] =\
                            codes_array[2*i//3]/4 - peds_array[2*i//3]
                        else:
                            cleaned_amplitudes[i] =\
                            codes_array[2*i//3 + 1]/4 - peds_array[2*i//3 + 1]
                        cleaned_amplitudes[i+1] =\
                        int(bin(codes_array[2*i//3 + 1])[-1])
#                       ig_status = 0 if BOTH channels IS ignored
#                       1 if LOW IS NOT ignored, HIGH IS ignored
#                       2 if HIGH IS NOT ignored, LOW IS ignored
#                       3 if BOTH IS NOT ignored
                        cleaned_amplitudes[i+2] =\
                        ig_array[2*i//3] + ig_array[2*i//3 + 1]
                    cleaned_amplitudes_pack = tools.packed_bytes(
                        'fBB'*32,
                        cleaned_amplitudes)
                    # chunk_size in wfp_file will be 282 bytes
                    cleaned_file.write(chunk[:codes_beginning_byte])
                    cleaned_file.write(cleaned_amplitudes_pack)
                    cleaned_file.write(chunk[codes_ending_byte:])
                    # chunk_size in header will be 17 bytes
                    header_file.write(
                        chunk[number_1_beginning_byte:maroc_number_byte +1])
                    chunk = codes_fin.read(chunk_size)

#    for i in range (len (ampl)):
#        ampl_av [i] = ampl [i]/counter [i]
#    for i in range (len (ampl)):
#        ampl_square [i] = ampl_square [i] / (counter [i] - 1)
#    for i in range (len (ampl)):
#        ampl_sigma [i] = mt.square_root (
#            abs (ampl_square [i] - (ampl_av [i]*ampl_av [i]*counter [i])/(counter [i] - 1)))
# =============================================================================
#
# =============================================================================

def fill_the_summary_files(start_time):
    """I feel sorry for man who will refactor this monster. Really

    This function consists from three parts.

    Part one:

    Firstly it creates days_set which contain all the days present
    in .files_list. If you preprocessed one day, there will be only it.

    Then for every day it creates tails_set. Tails_set contains the tails
    of all files preprocessed in the directory of this day. For example,
    if you preprocessed only the files xxx.001, set will contain only one
    item - "001". If you preprocessed all the day, the set will contain all
    the tails from "001" (001, 002, 003, ..., the last one).

    Then for each day it run all the files with the same tail (all the
    files ".001", then all the files ".002" etc.). There must be 22 files
    in general case. And it searches the minimal and maximal event number
    for this (22) files.

    Finally it creates the dictionary of dictionaries with next construction:
    dict_of_days = {day: dict_of_max_min},
    dict_of_max_min = {tail: [min_number, max_number]}

    The end of part one."""

    days_set = set()
    chunk_size = 282
    dict_of_max_min = {}
    dict_of_days = {}
    print("Evevt numbers range in parallel bsms are finding out...")
    files = open('.files_list.txt', 'r')
    files_list = files.readlines()
    files.close()
    for file_1 in files_list:
        file_1 = tools.check_and_cut_the_tail(file_1)
        file_directory = file_1[:-18]
        days_set.add(file_directory)
    for day in days_set:
        tails_set = set()
        for file_2 in files_list:
            if file_2[:-18] == day:
                file_2 = tools.check_and_cut_the_tail(file_2)
                tails_set.add(file_2[60:])
                with open('.files_list.txt', 'r') as files:
                    files_list = files.readlines()
                    for tail in tails_set:
                        num_max, num_min = 0, 0
                        for file_3 in files_list:
                            file_3 = tools.check_and_cut_the_tail(file_3)
                            file_directory_1 = file_3[:-18]
                            if file_directory == file_directory_1:
                                file_3 =\
                                file_3[:-12] + "." + file_3[-12:-3] + tail + '.wfp'
                                with open(file_3, 'rb') as wfp_file:
                                    chunk = wfp_file.read(chunk_size)
                                    while chunk:
                                        num_ev_bytes = chunk[4:8]
                                        num_ev = tools.unpacked_from_bytes(
                                            'I', num_ev_bytes)[0]
                                        if num_ev > num_max:
                                            num_max = num_ev
                                        elif num_ev < num_min:
                                            num_min = num_ev
                                        chunk = wfp_file.read(chunk_size)
                        dict_of_max_min[tail] = [num_min, num_max]
        dict_of_days[day] = dict_of_max_min
    print(tools.time_check(start_time))
#    print(dict_of_days)
#    print(days_set)

    """Part two:

    For each day (or only for one day if one day was preprocessed)
    creates N summary files, where N - number of tails. Each
    summary file is being created in the day directory and have name
    tail.sum (001.sum, 002.sum etc.). Each tail.sum file is being
    filled by M empty blanks, where M = max_number - num_number.
    Blanks numerated from 1 to M. Every blank contain 24 strings:
    1 string with BSM number and event number (from 1 to M).
    Next 22 strings are empty. They will be filled later, in part three.
    And the last one is empty and won't be filled. It's for separation.

    The end of part two."""

    print("The empty summary files are creating...")
    for key_1, val_1 in dict_of_days.items():
        for key_2, val_2 in val_1.items():
            with open(key_1 + key_2 + '.sum', 'w+') as sum_file:
                min_num, max_num = val_2[0], val_2[1]
                for i in range(min_num, max_num):
                    sum_file.write(
                        "BSM{}\tnumber_of_event:\t{}\n".format(
                            key_2[1:],
                            i))
                    for _ in range(22):
                        sum_file.write('\n')
                    sum_file.write('\n')
    print(tools.time_check(start_time))

    """Part three.

    Here each existed tail.sum file is being filled by cleaned amplitudes.
    For each file function runs through all (22) repacking and cleaned data
    files with this tail in this day directory. For each data file function
    one by one reads numbers of events. Each block of data function puts to
    the correspondent place in tail.sum file. This place is the N_1-th string
    in N_2-th blank, where N_1 - BSM number, N_2 - event number. So, this
    string in the tail.sum file will contain exatly amplitudes of THIS BSM in
    THIS event.
    Finally each tail.sum file contains full information about every event:
    number, and amplitudes of every BSM, also the time of event in every BSM
    and trigger-status and ignore-status of every channel in every BSM.

    The end of part three."""
# dict_of_days = {'/home/yaroslav/Yaroslavus_GitHub/DATA/281017/': {'001': [0, 258162]}}
# days_set = {'/home/yaroslav/Yaroslavus_GitHub/DATA/281017/'}

    print("The empty summary files are fillng by data...")
    for key, val in dict_of_days.items():
        list_of_sum_files = tools.directory_objects_parser(
            key, tools.SUM_FILE_PATTERN).split()
        for sum_file in list_of_sum_files:
            list_of_BSM = tools.directory_objects_parser(
                key, tools.BSM_REGULAR_PATTERN).split()
            tail_id = sum_file[:3]
            sum_file = key + sum_file
            tail_id_pattern = re.compile(
                tools.TAIL_FILE_REGULAR_PATTERN + tail_id)
            tail_files = []
            for BSM in list_of_BSM:
                BSM_name = day + BSM + '/'
                new_tail_file = BSM_name + tools.directory_objects_parser(
                    BSM_name, tail_id_pattern).split()[0]
                tail_files.append(new_tail_file)
            print(tail_files)
            for tail_file in tail_files:
                with open(tail_file, 'rb') as tail_file:
"""Read chunks and write them to the right places of the sum_file through bash sed using"""





























#    print(tools.time_check(start_time))
