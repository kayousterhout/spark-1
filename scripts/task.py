import logging

class Task:
  def __init__(self, data):
    self.initialize_from_json(data)

    self.scheduler_delay = (self.finish_time - self.executor_run_time -
      self.executor_deserialize_time - self.result_serialization_time - self.start_time)
    # Should be set to true if this task is a straggler and we know the cause of the
    # straggler behavior.
    self.straggler_behavior_explained = False

  def initialize_from_json(self, json_data):
    self.logger = logging.getLogger("Task")

    task_info = json_data["Task Info"]
    task_metrics = json_data["Task Metrics"]

    self.start_time = task_info["Launch Time"]
    self.finish_time = task_info["Finish Time"]
    self.executor = task_info["Host"]
    self.executor_run_time = task_metrics["Executor Run Time"]
    self.executor_deserialize_time = task_metrics["Executor Deserialize Time"]
    self.result_serialization_time = task_metrics["Result Serialization Time"]
    self.broadcast_block_time = task_metrics["Broadcast Blocked Nanos"] / 1.0e6
    self.gc_time = task_metrics["JVM GC Time"]

    self.shuffle_write_time = 0
    self.shuffle_mb_written = 0
    SHUFFLE_WRITE_METRICS_KEY = "Shuffle Write Metrics"
    if SHUFFLE_WRITE_METRICS_KEY in task_metrics:
      shuffle_write_metrics = task_metrics[SHUFFLE_WRITE_METRICS_KEY] 
      # Convert to milliseconds (from nanoseconds).
      self.shuffle_write_time = shuffle_write_metrics["Shuffle Write Time"] / 1.0e6
      OPEN_TIME_KEY = "Shuffle Open Time"
      if OPEN_TIME_KEY in shuffle_write_metrics:
        shuffle_open_time = shuffle_write_metrics[OPEN_TIME_KEY] / 1.0e6
        print "Shuffle open time: ", shuffle_open_time
        self.shuffle_write_time += shuffle_open_time
      CLOSE_TIME_KEY = "Shuffle Close Time"
      if CLOSE_TIME_KEY in shuffle_write_metrics:
        shuffle_close_time = shuffle_write_metrics[CLOSE_TIME_KEY] / 1.0e6
        print "Shuffle close time: ", shuffle_close_time
        self.shuffle_write_time += shuffle_close_time
      self.shuffle_mb_written = shuffle_write_metrics["Shuffle Bytes Written"] / 1048576.

    # TODO: print warning when non-zero disk bytes spilled??
    # TODO: are these accounted for in shuffle metrics?

    INPUT_METRICS_KEY = "Input Metrics"
    self.input_read_time = 0
    self.input_read_method = "unknown"
    self.input_mb = 0
    if INPUT_METRICS_KEY in task_metrics:
      input_metrics = task_metrics[INPUT_METRICS_KEY]
      self.input_read_time = 0 # TODO: fill in once input time has been added.
      self.input_read_method = input_metrics["Data Read Method"]
      self.input_mb = input_metrics["Bytes Read"] / 1048576.

    # TODO: add write time and MB.
    self.output_write_time = 0 #int(items_dict["OUTPUT_WRITE_BLOCKED_NANOS"]) / 1.0e6
    self.output_mb = 0
    #if "OUTPUT_BYTES" in items_dict:
    #  self.output_mb = int(items_dict["OUTPUT_BYTES"]) / 1048576.

    self.has_fetch = True
    # False if the task was a map task that did not run locally with its input data.
    self.data_local = True
    SHUFFLE_READ_METRICS_KEY = "Shuffle Read Metrics"
    if SHUFFLE_READ_METRICS_KEY not in task_metrics:
      if (task_info["Locality"] != "NODE_LOCAL") and (task_info["Locality"] != "PROCESS_LOCAL"):
        self.data_local = False
      self.has_fetch = False
      return

    shuffle_read_metrics = task_metrics[SHUFFLE_READ_METRICS_KEY]
      
    self.fetch_wait = shuffle_read_metrics["Fetch Wait Time"]
    self.map_output_fetch_wait = shuffle_read_metrics["Map Output Locations Fetch Time Millis"]
    self.local_blocks_read = shuffle_read_metrics["Local Blocks Fetched"]
    self.remote_blocks_read = shuffle_read_metrics["Remote Blocks Fetched"]
    self.remote_mb_read = shuffle_read_metrics["Remote Bytes Read"] / 1048576.
    self.local_mb_read = 0
    LOCAL_BYTES_READ_KEY = "Local Bytes Read"
    if LOCAL_BYTES_READ_KEY in shuffle_read_metrics:
      self.local_mb_read = shuffle_read_metrics[LOCAL_BYTES_READ_KEY] / 1048576.
    # The local read time is not included in the fetch wait time: the task blocks
    # on reading data locally in the BlockFetcherIterator.initialize() method.
    self.local_read_time = 0
    LOCAL_READ_TIME_KEY = "Local Read Time"
    if LOCAL_READ_TIME_KEY in shuffle_read_metrics:
      self.local_read_time = shuffle_read_metrics[LOCAL_READ_TIME_KEY]
    self.total_time_fetching = shuffle_read_metrics["Fetch Wait Time"]

  def input_size_mb(self):
    if self.has_fetch:
      return self.remote_mb_read + self.local_mb_read
    else:
      return self.input_mb

  def runtime(self):
    return self.finish_time - self.start_time

  def compute_time_without_gc(self):
     """ Returns the time this task spent computing.
     
     Assumes shuffle writes don't get pipelined with task execution (TODO: verify this).
     Does not include GC time.
     """
     compute_time = (self.runtime() - self.scheduler_delay - self.gc_time -
       self.shuffle_write_time - self.input_read_time - self.output_write_time)
     if self.has_fetch:
       # Subtract off of the time to read local data (which typically comes from disk) because
       # this read happens before any of the computation starts.
       compute_time = compute_time - self.fetch_wait - self.local_read_time - self.map_output_fetch_wait
     return compute_time

  def compute_time(self):
    """ Returns the time this task spent computing (potentially including GC time).

    The reason we don't subtract out GC time here is that garbage collection may happen
    during fetch wait.
    """
    return self.compute_time_without_gc() + self.gc_time

  def runtime_no_disk(self):
    """ Returns a lower bound on what the runtime would have been without disk IO.

    Includes shuffle read time, which is partially spent using the network and partially spent
    using disk.
    """
    disk_time = self.output_write_time + self.shuffle_write_time + self.input_read_time
    if self.has_fetch:
      disk_time += self.local_read_time + self.fetch_wait
    return self.runtime() - disk_time

  def __str__(self):
    if self.has_fetch:
      base = self.start_time
      # Print times relative to the start time so that they're easier to read.
      desc = (("Start time: %s, local read time: %s, " +
            "fetch wait: %s, compute time: %s, gc time: %s, shuffle write time: %s, " +
            "result ser: %s, finish: %s, shuffle bytes: %s, input bytes: %s") %
             (self.start_time, self.local_read_time,
              self.fetch_wait, self.compute_time(), self.gc_time,
              self.shuffle_write_time, self.result_serialization_time, self.finish_time - base,
              self.local_mb_read + self.remote_mb_read, self.input_mb)) 
    else:
      desc = ("Start time: %s, finish: %s, scheduler delay: %s, input read time: %s, gc time: %s, shuffle write time: %s" %
        (self.start_time, self.finish_time, self.scheduler_delay, self.input_read_time, self.gc_time, self.shuffle_write_time))
    return desc

  def runtime_no_network(self):
    runtime_no_in_or_out = self.runtime_no_output()
    if not self.data_local:
      runtime_no_in_or_out -= self.input_read_time
    if self.has_fetch:
      return runtime_no_in_or_out - self.fetch_wait
    else:
      return runtime_no_in_or_out

