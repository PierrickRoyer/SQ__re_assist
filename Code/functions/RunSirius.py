import os
import subprocess
from concurrent.futures import ThreadPoolExecutor
from IPython.display import display, Markdown
from multiprocessing import Process
import logging
import zmq
import time

from multiprocessing import Semaphore


def run_sirius_quality(SQpath, sqproPath, run_argument, semaphore):
    semaphore.acquire()
    command = f"{SQpath} -sim true true {sqproPath} --Run {run_argument}"
    logging.info(f"Executing: {command}")

    try:
        # Run the command using subprocess.Popen to handle it asynchronously
        process = subprocess.Popen(command, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, text=True)
        stdout, stderr = process.communicate()  # Wait for the process to complete

        # Check for errors in stderr
        if process.returncode != 0:
            logging.error(f"Error occurred in run {run_argument}: {stderr}")
        else:
            logging.info(f"Run {run_argument} completed successfully.\n{stdout}")
            # Display output for Jupyter notebook
            display(Markdown(f"**Run {run_argument} completed successfully.**\n\n{stdout}"))
    except Exception as e:
        logging.error(f"An error occurred while running {run_argument}: {e}")
        display(Markdown(f"**Error in run {run_argument}:** {str(e)}"))
    finally:
        semaphore.release()

# Parallelize using ThreadPoolExecutor
# def execute_parallel_runs(SQpath, sqproPath, runList, max_workers=None):
#     with ThreadPoolExecutor(max_workers=max_workers) as executor:
#         futures = [executor.submit(run_sirius_quality, SQpath, sqproPath, run_arg) for run_arg in runList]
#         for future in futures:
#             try:
#                 future.result()  # This will block until the future is completed
#             except Exception as e:
#                 logging.error(f"Error in executing a task: {e}")
#                 display(Markdown(f"**Error:** {str(e)}"))


def start_worker(worker_list, SQpath, sqproPath, runList,num_workers):
    """Start a worker process."""
    sema = Semaphore(num_workers)
    print(sema)
    for run in runList:
        worker_process = Process(target=run_sirius_quality, args=(SQpath, sqproPath,run, sema))
        worker_process.start()
        worker_list.append(worker_process)
    return worker_list

# Example: Create multiple worker processes
