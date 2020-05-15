#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 23:45:35 2020

@author: yaroslav
"""
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
                u'\u24B7',
                " preparing binary files")
            print("\n{} is processing now.".format(file_to_process))
            print(tools.time_check(start_time))

# u'\u2589'
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
        if (tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".fpd") or
                tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".sgm") or
                tools.is_exist(tools.make_PED_file_temp(file_to_process) + ".ig")) is False:
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
        if (tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".wfp") or
                tools.is_exist(tools.make_BSM_file_temp(file_to_process) + ".hdr")) is False:
            make_clean_amplitudes_and_headers(file_to_process)
#           .hdr - file with only events number #1, event number #2,
#           time of event and maroc number
            print("Creating of the header...")
            mess_file.write("Made temporary file:  {}.hdr\n".format(
                tools.make_BSM_file_temp(file_to_process)))
            print("Made temporary file:  {}.hdr".format(
                tools.make_BSM_file_temp(file_to_process)))
#           .wfp - amplitudes minus fine pedestals
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
        PED_square = [0]*64
        PED = [0]*64
        PED_av = [0]*64
        PED_sigma = [0]*64
        ignore_status = [0]*64
        sigma_sigma = 0
        chunk_size = 156
        codes_beginning_byte = 24
        codes_ending_byte = 152
        number_of_codes = 64
        chunk = ped_fin.read(chunk_size)
        while chunk:
            codes_array = tools.unpacked_from_bytes(
                '<64h',
                chunk[codes_beginning_byte:codes_ending_byte])
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
#                    chunk_bytes = bytearray(chunk)
                    codes_array = tools.unpacked_from_bytes(
                        '<64h', chunk[codes_beginning_byte:codes_ending_byte])
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
#
#def find_num_min_in_tail(dict_of_tails, tail):

# =============================================================================
#
# =============================================================================
                    

def count_tails_range(start_time):
    """I feel sorry for man who will refactor this monster. Really.

    Firstly it creates days_set which contain all the days present
    in .files_list. If you preprocessed one day, there will be only it.

    Then for every day it creates tails_set. Tails_set contains the tails
    of all files preprocessed in the directory of this day. For example,
    if you preprocessed only the files xxx.001, set will contain only one
    item - "001". If you preprocessed all the day, the set will contain all
    the tails from "001" (001, 002, 003, ..., the last one).

    Then for each day it finds all the files with the same tail (all the
    files ".001", then all the files ".002" etc.). There must be 22 files
    in general case. And it searches the minimal and maximal event number
    for this (22) files.

    Finally it returnes the dictionary of dictionaries with next construction:
    dict_of_days = {day: dict_of_max_min},
    dict_of_max_min = {tail: [min_number, max_number]}"""

    days_set = set()
    chunk_size = 282
    dict_of_max_min = {}
    dict_of_days = {}
    tails_counter = 0
    print("Evevt numbers range in parallel bsms are finding out...")
    files = open('.files_list.txt', 'r')
    files_list = files.readlines()

    files.close()
    for file in files_list:
        file = tools.check_and_cut_the_tail(file)
        file_directory = file[:-18]
        days_set.add(file_directory)
    print("Set of days have been created.")

    for day in sorted(days_set):
        tails_set = set()
        for file in files_list:
            if file[:-19] == day:
                file = tools.check_and_cut_the_tail(file)
                tails_set.add(file[60:])
        print("Set of tails have been created.")

        for tail in sorted(tails_set):
            print("Evevt numbers range in tail {} are finding out...".format(tail))
            num_max, num_min = 0, 0
            for file in files_list:
                file = tools.check_and_cut_the_tail(file)
                day_of_this_file = file[:-18]
                if day_of_this_file == day:
                    file = file[:-12] + "." + file[-12:-3] + tail + '.wfp'
                    with open(file, 'rb') as wfp_file:
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
            tails_counter += 1
            tools.syprogressbar(tails_counter,
                                len(tails_set),
                                u'\u24C2',
                                " finding out of evevt numbers range in {} tail finished".format(
                                    tail))
            print(tools.time_check(start_time))
        print(tools.time_check(start_time))
        dict_of_days[day] = dict_of_max_min
    print("Finding out of evevt numbers range in parallel bsms was finished.")
    print(tools.time_check(start_time))
    return dict_of_days

#    print(dict_of_days)
#    print(days_set)

##    dict_of_days = {'/home/yaroslav/Yaroslavus_GitHub/DATA/281017/': {'001': [0, 258162]}}
##    days_set = {'/home/yaroslav/Yaroslavus_GitHub/DATA/281017/'}

def fill_the_summary_files(dict_of_days, start_time):
    """Fill the final summary files xith events named (tail).sum.

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
    and trigger-status and ignore-status of every channel in every BSM."""

    chunk_size = 282
    print("The summary files of events are fillng by data...")
    list_of_tails = []
    for day_directory, tail_dict in dict_of_days.items():
        print("The {} is analizyng...".format(day_directory))
        tail_max_min_list = []
        for tail, max_min_list in tail_dict.items():
            list_of_tails.append(tail)
            tail_max_min_list = max_min_list

        list_of_BSM = tools.directory_objects_parser(
            day_directory, tools.BSM_REGULAR_PATTERN).split()

        tails_counter = 0
        for tail in list_of_tails:
            print("The {} is analizyng...".format(tail))
            matrix_of_events = [['']*22 for
                                i in range(
                                    tail_max_min_list[1] + 1 - tail_max_min_list[0])]

            tail_files = []
            print("Tail files list are creating...")
            for BSM in list_of_BSM:
                BSM_name = day_directory + BSM + '/'
                new_tail_file = BSM_name + tools.directory_objects_parser(
                    BSM_name, tools.TAIL_FILE_REGULAR_PATTERN + tail).split()[0]
                tail_files.append(new_tail_file)

            tail_files_counter = 0
            for tail_file in tail_files:
                print("Tail file  {}  is analizyng...".format(tail_file))
                tail_file = tools.make_BSM_file_temp(tail_file) + '.wfp'
                with open(tail_file, 'rb') as tail_file:
                    chunk = tail_file.read(chunk_size)
                    while chunk:
                        head_array = tools.unpacked_from_bytes('hhii', chunk[:12])
#                        data_type_id = head_array[0]
#                        data_chunk_size = head_array[1]
                        num_event_1 = head_array[2]
#                        num_event_2 = head_array[3]
                        maroc_number = tools.unpacked_from_bytes('h', chunk[20:22])[0]
                        time_array = tools.unpacked_from_bytes('hhhh', chunk[12:20])
                        ns = (time_array[0] & 0x7f)*10
                        mks = (time_array[0] & 0xff80) >> 7
                        mks |= (time_array[1] & 1) << 9
                        mls = (time_array[1] & 0x7fe) >> 11
                        s = (time_array[1] & 0xf800) >> 11
                        s |= (time_array[2] & 1) << 5
                        m = (time_array[2] & 0x7e) >> 1
                        h = (time_array[2] & 0xf80) >> 7
                        time_string = "{}:{}:{}.{}.{}.{}".format(h, m, s, mls, mks, ns)
                        result_array = tools.unpacked_from_bytes('fBB'*32, chunk[24:-4])
                        result_string_ampls = '\t'.join([str(x) for x in result_array])
                        matrix_of_events[num_event_1][maroc_number] =\
                        "{}\t{}\t{}".format(
                            maroc_number,
                            time_string,
                            result_string_ampls)
                        chunk = tail_file.read(chunk_size)
            print("Out file for {}-tails are filling for the {}...".format(
                tail,
                day_directory))
            with open(day_directory + tail + '.out', 'w+') as out_tail_file:
                for i in range(len(matrix_of_events)):
                    out_tail_file.write(
                        "Event_number\t{}in tail_files\t{}for the\t{}\n".format(
                            i,
                            tail,
                            day_directory))
                    for j in range(len(matrix_of_events[i])):
                        out_tail_file.write("{}\n".format(matrix_of_events[i][j]))
                    out_tail_file.write('\n')
                tail_files_counter += 1
                tools.syprogressbar(
                    tail_files_counter,
                    len(tail_files),
                    u'\u24BB',
                    "tail files {} amplitudes collecting".format(tail))
            tails_counter += 1
            tools.syprogressbar(
                tails_counter,
                len(list_of_tails),
                u'\u24C9',
                "creating summary files for tails")

            print("Statistics for tail {} are calculating...".format(tail))
            bsm = [0]*22
            for i in range(len(matrix_of_events)):
                current_event_ampls = []
                for j in range(len(matrix_of_events[i])):
                    if matrix_of_events[i][j] != '':
                        current_event_ampls.append(1)
                        bsm[len(current_event_ampls)] += 1
            for i in range(len(bsm)):
                print("coins: {}\tevents: {}\n".format(i, bsm[i]))

        print("The summary files for  {}  have been created".format(day_directory))
        print(tools.time_check(start_time))
    print(tools.time_check(start_time))
