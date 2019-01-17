import sys
import os

option = sys.argv[1]
freq = sys.argv[2]
print('# Initiating Process Preparation - {} ({})'.format(option, freq))
from batch_utils.utils_dateSeq import batch_sequence
from batch_utils.utils_mapping_orig import get_Mapping_orig

bkfil, _, _ = batch_sequence(option, freq, rtvDays=60)

for mapping in ['IBES', 'worldscope']:
    codeMap = get_Mapping_orig(mapping)

if bkfil:
    folder = 'save_total'
else:
    folder = 'save_batch'

if os.path.exists(folder) & os.path.isdir(folder):
    file_lst = os.listdir(folder)
    if len(file_lst) > 0:
        for file in file_lst:
            file_path = os.path.join(folder, file)
            try:
                if os.path.isfile(file_path):
                    os.unlink(file_path)
            except Exception as e:
                print(e)

print('# Process Prep is Ready!')
