#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed Apr 29 18:17:03 2020

@author: yaroslav
"""

#import re
#import os

import manticora_parser as mpar
import time
import manticora_tools as mt
import os

cur_dir = mpar.data_dir() + "/281017/"


#  NOT CHECKED #
def fill_sum_by_all_bsms_001 ():

    f = mpar.directory_objects_parser (cur_dir, mpar.bsm_regular_pattern)
    start_time = time.time()
    n = '001'
    for bsm_dir in f.split():
        bsm_id = bsm_dir [3:]
        with open (cur_dir + bsm_dir + "/.281070" + bsm_id + "." + n + ".wfpl", "r") as cur_wfpl:
            print (cur_dir + bsm_dir + "/.281070" + bsm_id + "." + n + ".wfpl" + "   is processed")
            print (mt.time_check(start_time))
            for line in cur_wfpl.readlines():
                d = line.split()
                num = d[0]
                time = d[3]
                string_0 = '0' + '\t0'*63
                ampls = ' '.join([str(x) for x in d[4:]])
                os.system("sed -i '" + str(24*int(num)+1+1)  + "s/" + string_0 + "/" + time + '\t' + ampls + "'/ sum_001.txt")

#class Chunk:
#
#    def __init__(self,
#                 data_type_id,
#                 chunk_size,
#                 num_event_1,
#                 num_event_2,
#                 maroc_number,
#                 end_of_pack,
#                 codes_array,
#                 h,
#                 m,
#                 s,
#                 mls,
#                 mks,
#                 ns):
#
#        self.data_type_id = data_type_id
#        self.chunk_size = chunk_size
#        self.num_event_1 = num_event_1
#        self.num_event_2 = num_event_2
#        self.maroc_number = maroc_number
#        self.end_of_pack = end_of_pack
#        self.codes_array = codes_array
#        self.h = h
#        self.s = s
#        self.mls = mls
#        self.mks = mks
#        self.ns = ns
##        self.list_of_channels.append (self)
#
#    def show_chunk (item):
#        print("data_type_id:\t{}\tchunk_size:\t{}\tnum_event_1:\t{}\tnum_event_2:\t{}\tmaroc_number:\t{}\tend_of_pack:\t{}\tcodes_array:\t{}\ttime:\t{}:{}:{}.{}.{}.{}".format(
#                item.data_type_id, item.chunk_size, item.num_event_1, item.num_event_2,
#                item.maroc_number, item.end_of_pack, item.codes_array,
#                item.h, item.m, item.s, item.mls, item.nks, item.ns))
# =============================================================================
#
# =============================================================================

#def process_one_codes_array_in_chunk (chunk, some_manipulation_with_chunk, out_file):
#
#    chunk_bytes = bytearray (chunk)
#    codes_array = struct.unpack ('64h', chunk_bytes [24:152])
#    some_manipulation_with_chunk (chunk, out_file)
#    return codes_array
## =============================================================================
##
## =============================================================================
#
#def process_one_full_chunk (chunk, some_manipulation_with_chunk, out_file):
#
#    chunk_bytes = bytearray (chunk)
##    data_type_id = chunk_bytes [1]*256 + chunk_bytes [0]
##    chunk_size = ((chunk_bytes [3] & 0xff)*256) + (chunk_bytes [2] & 0xff)
##    num_event_1 = chunk_bytes [7]*(256)**3 + chunk_bytes [6]*256**2 + chunk_bytes [5]*256 + chunk_bytes [4]
##    num_event_2 = chunk_bytes [11]*(256)**3 + chunk_bytes [10]*256**2 + chunk_bytes [9]*256 + chunk_bytes [8]
#    head_array = struct.unpack ('hhii', chunk_bytes [:12])
#    data_type_id = head_array [0]
#    chunk_size = head_array [1]
#    num_event_1 = head_array [2]
#    num_event_2 = head_array [3]
##    BSM counted from 1 to 22
##    maroc_number = (chunk_bytes [20] & 0x1f) + 1
#    maroc_number = struct.unpack ('h', chunk_bytes [20:22]) [0] + 1
##    codes_array = chunk_bytes [24:152]
#    codes_array = struct.unpack ('64h', chunk_bytes [24:152])
##    end_of_pack = chunk_bytes [152:]
#    end_of_pack = struct.unpack ('i', chunk_bytes [152:]) [0]
##    event_time = [0]*4
##    for i in range (0, 8, 2):
##        event_time [i//2] = chunk_bytes [i + 13]*256 + chunk_bytes [i + 12]
#    time_array = struct.unpack ('hhhh', chunk_bytes [12:20])
#    ns = (time_array [0] & 0x7f)*10
#    mks = (time_array [0] & 0xff80) >> 7
#    mks |= (time_array [1] & 1) << 9
#    mls = (time_array [1] & 0x7fe) >> 1
#    s = (time_array [1] & 0xf800) >> 11
#    s |= (time_array [2] & 1) << 5
#    m = (time_array [2] & 0x7e) >> 1
#    h = (time_array [2] & 0xf80) >> 7
#    chunk = Chunk (data_type_id,
#                   chunk_size,
#                   num_event_1,
#                   num_event_2,
#                   maroc_number,
#                   end_of_pack,
#                   h,
#                   m,
#                   s,
#                   mls,
#                   mks,
#                   ns,
#                   codes_array)
#    some_manipulation_with_chunk (chunk, out_file)
#    return data_type_id, chunk_size, num_event_1, num_event_2, maroc_number, end_of_pack, codes_array, h, m, s, mls, mks, ns
# =============================================================================
#
# =============================================================================
