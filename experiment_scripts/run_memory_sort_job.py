"""
This script runs jobs that process the same amount of data, but use different
numbers of tasks to do so.

Each job sorts data read from HDFS, and saves the output in HDFS. To vary the
number of tasks, we re-write the input data for each experiment, using a
different number of tasks to write the data (each task writes at least one
partition, so using more tasks forces HDFS to use more partitions).

Before running this experiment, be sure to change the block size in
ephemeral-hdfs/conf/hdfs-site.xml to be 2GB (which is the maximum allowed block size,
and larger than the largest block size used in this experiment).
"""

import os
import subprocess
import time

import utils

MEGABYTES_PER_GIGABYTE = 1024

slaves = [slave_line.strip("\n") for slave_line in open("/root/spark/conf/slaves").readlines()]
print "Running experiment assuming slaves %s" % slaves

num_machines = len(slaves)
values_per_key = 8
num_shuffles = 5

base_num_tasks = num_machines * 8
num_tasks_multipliers = [8, 4]
target_total_data_gb = num_machines * 0.5

for num_tasks_multiplier in num_tasks_multipliers:
  num_tasks = base_num_tasks * num_tasks_multiplier

  total_num_items = target_total_data_gb / (4.9 + values_per_key * 1.92) * (64 * 4000000)
  items_per_task =  int(total_num_items / num_tasks)
  parameters = [num_tasks, num_tasks, items_per_task, values_per_key, num_shuffles]
  stringified_parameters = ["%s" % p for p in parameters]

  # Clear the buffer cache, to sidestep issue with machines dying because they've run out of memory.
  subprocess.check_call("/root/ephemeral-hdfs/sbin/slaves.sh /root/spark-ec2/clear-cache.sh",
    shell=True)

  # Delete any existing sorted data, to avoid naming conflicts.
  if num_shuffles > 0:
    try:
      subprocess.check_call("/root/ephemeral-hdfs/bin/hadoop dfs -rm -r ./*sorted*", shell=True) 
    except:
      print "No sorted data deleted, likely because no sorted data existed"

  # Run the job.
  command = "/root/spark/bin/run-example monotasks.MemorySortJob %s" % " ".join(stringified_parameters)
  print "Running sort job with command: ", command
  subprocess.check_call(command, shell=True)

  utils.copy_and_zip_all_logs(stringified_parameters, slaves)

