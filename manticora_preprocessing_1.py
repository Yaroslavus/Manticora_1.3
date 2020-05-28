#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Wed May 27 12:57:52 2020

@author: yaroslav
"""
import manticore_tools as tools
# =============================================================================
#
# =============================================================================

def make_pedestals(file_to_process):

    codes_beginning_byte = 24
    codes_ending_byte = 152
    number_of_codes = 64
    counter = [0]*64
    PED_square = [0]*64
    PED = [0]*64
    PED_av = [0]*64
    PED_sigma = [0]*64
    ignore_status = [0]*64
    sigma_sigma = 0
    chunk_size = 156
    chunk_counter = 0
        
    with open(file_to_process[:-18] + "PED/" + file_to_process[-12:-4] + ".ped", "rb") as ped_fin:

        byte = ped_fin.read(1)
        while byte:
            if byte == '\xd8':
                next_byte = ped_fin.read(1)
                if next_byte == '\x0b':
                    chunk = byte + next_byte
                    next_byte = ped_fin.read(1)
                    while next_byte != '\xff':
                        chunk += next_byte
                        next_byte = ped_fin.read(1)
                    flag = ped_fin.read(3)
                    if flag == '\xff\xff\xff':
                        flag += next_byte
                        chunk += flag
                        if len(chunk) == chunk_size:
                            
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
                                chunk_counter += 1
                            except Exception:
                                print("{} Chunk number {} in file {} is seems to be corrupted!\n".format(
                                    "RAW CHUNK CORRUPTION ERROR!",
                                    chunk_counter,
                                    file_to_process[:-18] + "PED/" + file_to_process[-12:-4] + ".ped"))  
                                
            byte = ped_fin.read(1)
                
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
    chunk_counter = 0

    with open(tools.make_PED_file_temp(file_to_process) + ".fpd", "rb") as ped_fin:
        peds = ped_fin.read()
        try:
            peds_array = tools.unpacked_from_bytes('<64f', peds)
        except Exception:
            print("{} File {} is seems to be corrupted!\n".format(
                "FPD-file CORRUPTION ERROR!",
                tools.make_PED_file_temp(file_to_process) + ".fpd"))

    with open(tools.make_PED_file_temp(file_to_process) + ".ig", "rb") as ig_fin:
        ig_bytes = ig_fin.read()
        try:
            ig_array = tools.unpacked_from_bytes('<64B', ig_bytes)
        except Exception:
            print("{} File {} is seems to be corrupted!\n".format(
                "IG-file CORRUPTION ERROR!",
                tools.make_PED_file_temp(file_to_process) + ".ig"))

    with open(file_to_process, "rb") as codes_fin:
        with open(tools.make_BSM_file_temp(file_to_process) + ".wfp", "wb") as cleaned_file:
            with open(tools.make_BSM_file_temp(file_to_process) + ".hdr", "wb") as header_file:

                byte = codes_fin.read(1)
                while byte:
                    if byte == '\xd8':
                        next_byte = codes_fin.read(1)
                        if next_byte == '\x0b':
                            chunk = byte + next_byte
                            next_byte = codes_fin.read(1)
                            while next_byte != '\xff':
                                chunk += next_byte
                                next_byte = codes_fin.read(1)
                            flag = codes_fin.read(3)
                            if flag == '\xff\xff\xff':
                                flag += next_byte
                                chunk += flag
                                if len(chunk) == chunk_size:
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
                                #           ig_status = 0 if BOTH channels IS ignored
                                #           1 if LOW IS NOT ignored, HIGH IS ignored
                                #           2 if HIGH IS NOT ignored, LOW IS ignored
                                #           3 if BOTH IS NOT ignored
                                            cleaned_amplitudes[i+2] =\
                                            ig_array[2*i//3] + ig_array[2*i//3 + 1]
                                        cleaned_amplitudes_pack = tools.packed_bytes(
                                            'fBB'*32,
                                            cleaned_amplitudes)
                                #       chunk_size in wfp_file will be 282 bytes
                                        cleaned_file.write(chunk[:codes_beginning_byte])
                                        cleaned_file.write(cleaned_amplitudes_pack)
                                        cleaned_file.write(chunk[codes_ending_byte:])
                                #       chunk_size in header will be 17 bytes
                                        header_file.write(
                                            chunk[number_1_beginning_byte:maroc_number_byte +1])
                                        chunk_counter += 1
                                    except Exception:
                                        print("{} Chunk number {} in file {} is seems to be corrupted!\n".format(
                                            "RAW CHUNK CORRUPTION ERROR!",
                                            chunk_counter,
                                            file_to_process))
                    byte = codes_fin.read(1)
# =============================================================================
#
# =============================================================================
