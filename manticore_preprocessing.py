#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Mon Apr 20 23:45:35 2020

@author: yaroslav
"""
import manticora_preprocessing_1 as mp1
import manticore_tools as tools
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
            print("\nPreparing binary files:\n")
            counter += 1
            tools.syprogressbar(
                counter,
                number_of_files_to_process,
                u'\u24B7',
                "preparing binary files",
                start_time)
            print("\n{} is processing now.".format(file_to_process))
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
            mp1.make_pedestals(file_to_process)
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
            mp1.make_clean_amplitudes_and_headers(file_to_process)
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
        chunk_counter = 0
        while chunk:
            try:
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
            except:
                print("{} Chunk number {} in file {} is seems to be corrupted!\n".format(
                    "RAW CHUNK CORRUPTION ERROR!",
                    chunk_counter,
                    file_to_process[:-18] + "PED/" + file_to_process[-12:-4] + ".ped"))
            chunk_counter += 1
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
    maroc_number_byte = 20

    with open(tools.make_PED_file_temp(file_to_process) + ".fpd", "rb") as ped_fin:
        peds = ped_fin.read()
        try:
            peds_array = tools.unpacked_from_bytes('<64f', peds)
        except:
            print("{} File {} is seems to be corrupted!\n".format(
                "FPD-file CORRUPTION ERROR!",
                tools.make_PED_file_temp(file_to_process) + ".fpd"))

    with open(tools.make_PED_file_temp(file_to_process) + ".ig", "rb") as ig_fin:
        ig_bytes = ig_fin.read()
        try:
            ig_array = tools.unpacked_from_bytes('<64B', ig_bytes)
        except:
            print("{} File {} is seems to be corrupted!\n".format(
                "IG-file CORRUPTION ERROR!",
                tools.make_PED_file_temp(file_to_process) + ".ig"))

    with open(file_to_process, "rb") as codes_fin:
        with open(tools.make_BSM_file_temp(file_to_process) + ".wfp", "wb") as cleaned_file:
            with open(tools.make_BSM_file_temp(file_to_process) + ".hdr", "wb") as header_file:

                chunk_counter = 0
                chunk = codes_fin.read(chunk_size)
                while chunk:
                    try:
                        cleaned_amplitudes = [0]*96
                        codes_array = tools.unpacked_from_bytes(
                            '<64h', chunk[codes_beginning_byte:codes_ending_byte])
                        for i in range(0, len(cleaned_amplitudes), 3):
                            if codes_array[2*i//3] <= 1800:
                                cleaned_amplitudes[i] =\
                                codes_array[2*i//3]/4 - peds_array[2*i//3]
                            else:
                                cleaned_amplitudes[i] =\
                                codes_array[2*i//3 + 1]/4 - peds_array[2*i//3 + 1]
                            cleaned_amplitudes[i+1] =\
                            int(bin(codes_array[2*i//3 + 1])[-1])
#                           ig_status = 0 if BOTH channels IS ignored
#                           1 if LOW IS NOT ignored, HIGH IS ignored
#                           2 if HIGH IS NOT ignored, LOW IS ignored
#                           3 if BOTH IS NOT ignored
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
                    except:
                        print("{} Chunk number {} in file {} is seems to be corrupted!\n".format(
                            "RAW CHUNK CORRUPTION ERROR!",
                            chunk_counter,
                            file_to_process))
                    chunk_counter += 1
                    chunk = codes_fin.read(chunk_size)
# =============================================================================
#
# =============================================================================

def set_of_days(files_list):
    """Creates days_set which contain full pathes of all the days present
    in .files_list. If you preprocessed one day, there will be only it."""

    days_set = set()
    for file in files_list:
        file = tools.check_and_cut_the_tail(file)
        file_directory = file[:-18]
        days_set.add(file_directory)
    print("Set of days have been created.")
    return days_set
# =============================================================================
#
# =============================================================================

def set_of_tails(files_list, day):
    """Creates tails_set for every day. Works for every day from days_set.

    Tails_set contains the tails of all files preprocessed in the
    directory of this day. For example, if you preprocessed only the
    files xxx.001, set will contain only one item - "001". If you
    preprocessed all the day, the set will contain all the tails from
    ('001', '002', '003', ..., the last one) of this day."""

    tails_set = set()
    for file in files_list:
        if file[:-19] == day:
            file = tools.check_and_cut_the_tail(file)
            tails_set.add(file[60:])
    print("Set of tails have been created.")
    return tails_set
# =============================================================================
#
# =============================================================================

def count_tails_range(start_time):
    """For all tails in .files_list.txt find maximum and minimum event number.

    For each day it finds all the files with the same tail (all the
    files ".001", then all the files ".002" etc.). There must be 22 files
    for every tail in every day in general case. And it searches the
    minimal and maximal event number for this (22) files.

    Finally it returnes the dictionary of dictionaries with next construction:
    dict_of_days = {day: dict_of_max_min},
    dict_of_max_min = {tail: [min_number, max_number]}"""

    dict_of_max_min = {}
    dict_of_days = {}
    tails_counter = 0
    print("Evevt numbers range in parallel bsms are finding out...")
    with open('.files_list.txt', 'r') as files:
        files_list = files.readlines()
    days_set = set_of_days(files_list)

    for day in sorted(days_set):
        tails_set = set_of_tails(files_list, day)
        for tail in sorted(tails_set):
            dict_of_max_min[tail] = dict_of_num_min_max_in_tail(
                tail,
                files_list,
                day)
            dict_of_days[day] = dict_of_max_min
            tails_counter += 1
            tools.syprogressbar(tails_counter,
                                len(tails_set),
                                u'\u24C2',
                                "finding out of evevt numbers range in {} tail finished".format(
                                    tail),
                                start_time)
        print(tools.time_check(start_time))
    print("Finding out of evevt numbers range in parallel bsms was finished.")
    print(tools.time_check(start_time))
    return dict_of_days
# =============================================================================
#
# =============================================================================
def print_statistics_for_matrix_of_events(matrix_of_events, stat_file):
    """Print the coincidences statistics for every 2 minutes of data
    (for one tail). From 0_BSM events to 22_BSM events.

    Takes the matrix of events in format M[event number][BSM], where
    each item -
    string = maroc number + event time + 64*'amplitude + trigger status + ignore status'"""

    coin = [0]*23
    for string in matrix_of_events:
        string_counter = 0
        for item in string:
            if item != '':
                string_counter += 1
        coin[string_counter] += 1
    with open(stat_file, 'w+') as stat_file:
        for i in range(len(coin)):
            print("coins: {}\tevents: {}\n".format(i, coin[i]))
            stat_file.write("coins: {}\tevents: {}\n".format(i, coin[i]))
# =============================================================================
#
# =============================================================================

def list_of_tail_files(day_directory, list_of_BSM, tail):
    tail_files = []
    for BSM in list_of_BSM:
        BSM_name = "{}{}/".format(
            day_directory,
            BSM)
        new_tail_file = BSM_name + tools.directory_objects_parser(
            BSM_name,
            tools.TAIL_FILE_REGULAR_PATTERN + tail).split()[0]
        tail_files.append(new_tail_file)
    return tail_files
# =============================================================================
#
# =============================================================================

def fill_the_matrix_of_events(matrix_of_events, tail_files, tail, start_time):

    chunk_size = 282
    tail_files_counter = 0
    for tail_file in tail_files:
        print("Tail file  {}  amplitudes collecting...".format(tail_file))
        tail_file = tools.make_BSM_file_temp(tail_file) + '.wfp'
        try:
            with open(tail_file, 'rb') as tail_file:
                chunk = tail_file.read(chunk_size)
                chunk_counter = 0
                while chunk:
                    try:
                        head_array = tools.unpacked_from_bytes('hhii', chunk[:12])
                        num_event_1 = head_array[2]
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
                    except:
                        print("{} Chunk number {} in file {} is seems to be corrupted!".format(
                            "WFP-file CHUNK CORRUPTION ERROR!",
                            chunk_counter,
                            tail_file))
                    chunk_counter += 1
                    chunk = tail_file.read(chunk_size)

            tail_files_counter += 1
            tools.syprogressbar(
                tail_files_counter,
                len(tail_files),
                u'\u24BB',
                "tail files {} amplitudes collecting".format(tail),
                start_time)
        except:
            print("{} File {} is seems to be not existed!".format(
                    "WFP-file EXISTING ERROR!",
                    tail_file))
    return matrix_of_events
# =============================================================================
#
# =============================================================================

def create_summary_file_for_tail(tail, tail_max_min_list, start_time,
                                 list_of_BSM, day_directory,
                                 tails_counter, list_of_tails):

    matrix_of_events = [['']*22 for
                        i in range(tail_max_min_list[1] + 1 - tail_max_min_list[0])]

    print("\nFiles list for tail  {}  from  {}  are creating...".format(tail, day_directory))
    tail_files = list_of_tail_files(day_directory, list_of_BSM, tail)
    print("Event matrix for tail  {}  from  {}  are creating...".format(tail, day_directory))
    matrix_of_events = fill_the_matrix_of_events(matrix_of_events, tail_files, tail, start_time)
    print("Cleaning the event matrix for tail  {}  from  {}  for 0-coincidences events...".format(tail, day_directory))
    before = len(matrix_of_events)
    matrix_of_events = clean_the_matrix_of_events(matrix_of_events)
    after = len(matrix_of_events)
    print("DELETED  {:.3f}% events".format((before - after)/before*100))
    print("Out file for  {}  tail from  {}  are filling...".format(tail, day_directory))
    with open(day_directory + tail + '.out', 'w+') as out_tail_file:
        for i in range(len(matrix_of_events)):
            out_tail_file.write(
                "Event_number\t{}\tin_tail_files\t{}\tfor_the\t{}\n".format(
                    i, tail, day_directory))
            for j in range(len(matrix_of_events[i])):
                out_tail_file.write("{}\n".format(matrix_of_events[i][j]))
            out_tail_file.write('\n')

    tools.syprogressbar(
        tails_counter,
        len(list_of_tails),
        u'\u24C9',
        "creating summary files for tails",
        start_time)
    stat_file = day_directory + tail + '.stat'
    print("Statistics for tail {} from {} are calculating...".format(tail, day_directory))
    print_statistics_for_matrix_of_events(matrix_of_events, stat_file)
# =============================================================================
#
# =============================================================================

def fill_the_summary_files(dict_of_days, start_time):
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
            tails_counter += 1
            create_summary_file_for_tail(tail, tail_max_min_list, start_time,
                                         list_of_BSM, day_directory,
                                         tails_counter, list_of_tails)
        print("The summary files for  {}  have been created".format(day_directory))
        print(tools.time_check(start_time))
    print(tools.time_check(start_time))
# =============================================================================
#
# =============================================================================

def dict_of_num_min_max_in_tail(tail, files_list, day):
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
            file = file[:-12] + "." + file[-12:-3] + tail + '.wfp'
            with open(file, 'rb') as wfp_file:
                chunk = wfp_file.read(chunk_size)
                num_ev_bytes = chunk[4:8]
                num_ev = tools.unpacked_from_bytes(
                    'I', num_ev_bytes)[0]
                if num_ev < num_min:
                    num_min = num_ev
                while chunk:
                    next_chunk = wfp_file.read(chunk_size)
                    if next_chunk: chunk = next_chunk
                    else: break
                num_ev_bytes = chunk[4:8]
                num_ev = tools.unpacked_from_bytes(
                    'I', num_ev_bytes)[0]
                if num_ev > num_max:
                    num_max = num_ev

    return [num_min, num_max]
# =============================================================================
#
# =============================================================================

def clean_the_matrix_of_events(matrix_of_events):
    empty_event = ['']*22
    return [value for value in matrix_of_events if value != empty_event]
# =============================================================================
#
# =============================================================================
