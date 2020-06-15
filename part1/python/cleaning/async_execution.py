import multiprocessing as mp
import logging
import os
from hemu243.cs598.part1.python.cleaning.clean_data import processed_zip_files
from hemu243.cs598.part1.python.cleaning.get_zipfiles import ListZipFiles

logger = logging.getLogger()
if 'DEBUG' in os.environ and os.environ['DEBUG'] == 'true':
    logger.setLevel(logging.DEBUG)
    logger.debug('debug mode enabled.')
else:
    logger.setLevel(logging.INFO)

pool = mp.Pool(mp.cpu_count())

results = []


# Step 1: Redefine, to accept `i`, the iteration number



# Step 2: Define callback function to collect the output in `results`
def collect_result(result):
    global results
    results.append(result)


list_files = ListZipFiles('src-zips', '').execute()

# Step 3: Use loop to parallelize
for key in list_files[0:1]:
    pool.apply_async(processed_zip_files, args=(key, "src-zips", "", "hsc4-clean-data", "ptest"), callback=collect_result)

# Step 4: Close Pool and let all the processes complete
pool.close()
pool.join()  # postpones the execution of next line of code until all processes in the queue are done.

# Step 5: Check results
print("********************************* Final results ***********************")
validation_ok = 1
for result in results:
    if result.get('message') != 'success':
        logger.error("Failed to process zip, file=%s, failed_csv=%s, msg=%s", result['key'], result['failed_csv'],
                     result["message"])
        validation_ok = 0

print(results)
