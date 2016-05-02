"""
Utilities to help with running experiments.

The environment variable MONOTASKS_HOME should be set to the directory
that contains the "spark" directory (with all of the Spark source code),
the "ephemeral-hdfs" directory, the "spark-ec2" directory, and so on. If this
variable isn't set, it defaults to "root" (which is correct when running
using the default ec2 setup).
"""

import os
import subprocess
import time

# Returns the given path with the correct base directory at the
# beginning of it (the MONOTASKS_HOME environment variable, or
# /root by default, which is correct for EC2).
def get_full_path(relative_path):
  if "MONOTASKS_HOME" in os.environ:
    monotasks_home = os.environ["MONOTASKS_HOME"]
  else:
    monotasks_home = "/root"
  return os.path.join(monotasks_home, relative_path)

# Returns a list of the workers in the cluster.
def get_workers():
  workers_filename = get_full_path("spark/conf/slaves")
  return [slave_line.strip("\n") for slave_line in open(workers_filename).readlines()]

# Copy a file from a given host through scp, throwing an exception if scp fails.
def scp_from(host, remote_file, local_file, identity_file=None):
  subprocess.check_call(
    "scp %s -q -o StrictHostKeyChecking=no '%s:%s' '%s'" %
    (get_identity_file_argument(identity_file), host, remote_file, local_file), shell=True)

# Run a command on the given host and return the standard output.
def ssh_get_stdout(host, command, identity_file=None):
  ssh_command = build_ssh_command(host, command, identity_file)
  return subprocess.Popen(ssh_command, stdout=subprocess.PIPE, shell=True).communicate()[0]

# Run a command on the given host and print the standard output.
def ssh_call(host, command, identity_file=None):
  subprocess.check_call(build_ssh_command(host, command, identity_file), shell=True)

def build_ssh_command(host, command, identity_file=None):
  if "ec2" in host:
    command = "source /root/.bash_profile; %s" % command
  return "ssh %s -t -o StrictHostKeyChecking=no %s '%s'" % \
    (get_identity_file_argument(identity_file), host, command)

def get_identity_file_argument(identity_file):
  return "" if (identity_file is None) else "-i %s" % identity_file

def copy_and_zip_all_logs(stringified_parameters, slaves):
  """ Packages up all of the logs from running an experiment.

  Args:
    stringified_parameters: A list of strings that were parameters to the experiment. Used
      in naming the resulting directory.
    slaves: A list of workers used to run the experiment (the continuous monitor logs will be
      copied back from all of these machines).
  """
  # Name the directory with the logs based on the parameters, along with a timestamp.
  log_subdirectory_name = "experiment_log_%s_%s" % ("_".join(stringified_parameters), time.time())
  log_directory_name = "/mnt/%s" % log_subdirectory_name
  os.makedirs(log_directory_name)

  for slave_hostname in slaves:
    continuous_monitor_relative_filename = ssh_get_stdout(
      slave_hostname,
      "ls -t /tmp/ | grep continuous_monitor | head -n 1").strip("\n").strip("\r")
    continuous_monitor_filename = "/tmp/%s" % continuous_monitor_relative_filename
    local_continuous_monitor_file = "%s/%s_executor_monitor" % (log_directory_name, slave_hostname)
    print ("Copying continuous monitor from file %s on host %s back to %s" %
      (continuous_monitor_filename, slave_hostname, local_continuous_monitor_file))
    scp_from(slave_hostname, continuous_monitor_filename, local_continuous_monitor_file)

  event_log_relative_filename = subprocess.Popen(
    "ls -t /tmp/spark-events | head -n 1", stdout=subprocess.PIPE, shell=True).communicate()[0]
  event_log_filename = "/tmp/spark-events/%s" % event_log_relative_filename.strip("\n").strip("\r")
  new_event_log_filename = "%s/event_log" % log_directory_name
  print "Moving event log from %s to %s" % (event_log_filename, new_event_log_filename)
  command = "mv %s %s" % (event_log_filename, new_event_log_filename)
  subprocess.check_call(command, shell=True)

  # Copy the configuration into the directory to make it easy to see config later.
  configuration_filename = get_full_path("spark/conf/spark-defaults.conf")
  subprocess.check_call("cp %s %s/" % (configuration_filename, log_directory_name),
    shell=True)
  print "Finished copying results to %s" % log_directory_name

  # Tar and zip the file so that it can easily be copied out of the cluster.
  tar_filename = log_directory_name + ".tar.gz"

  # For some reason, the tar command fails without this.
  subprocess.check_call("touch %s" % tar_filename, shell=True)

  subprocess.check_call("tar czfv %s --directory=/mnt %s" % (tar_filename, log_subdirectory_name),
    shell=True)

def check_if_hdfs_file_exists(hdfs_path):
  """ Returns true if the given HDFS path exists, and false otherwise. """
  command = "%s dfs -ls %s" % (get_full_path("ephemeral-hdfs/bin/hdfs"), hdfs_path)
  output = subprocess.Popen(command, stderr=subprocess.PIPE, shell=True).communicate()
  index = (output[1].find("No such file"))
  return (index == -1)

def cleanup_sort_job():
  """ Cleans up after a sort experiment by clearing the buffer cache and deleting sorted data. """
  # Clear the buffer cache, to sidestep issue with machines dying.
  slaves_filename = get_full_path("ephemeral-hdfs/sbin/slaves.sh")
  clear_cache_script = get_full_path("spark-ec2/clear-cache.sh")
  subprocess.check_call("%s %s" % (slaves_filename, clear_cache_script), shell=True)

  try:
    # Delete any existing sorted data.
    hadoop_filename = get_full_path("ephemeral-hdfs/bin/hadoop")
    subprocess.check_call("%s dfs -rm -r ./*sorted*" % hadoop_filename, shell=True)
  except:
    print "No sorted data found, so didn't delete anything"

