import collections
import json
import logging
import math
import numpy
import os
import sys

import concurrency
import simulate
import stage
import task

class Job:
  def __init__(self):
    self.logger = logging.getLogger("Job")
    # Map of stage IDs to Stages.
    self.stages = collections.defaultdict(stage.Stage)
  
  def add_event(self, data):
    event_type = data["Event"]
    if event_type == "SparkListenerTaskEnd":
      stage_id = data["Stage ID"]
      self.stages[stage_id].add_event(data)

  def remove_empty_stages(self):
    """ Should be called after adding all events to the job. """
    # Drop empty stages.
    stages_to_drop = []
    for id, s in self.stages.iteritems():
      if len(s.tasks) == 0:
        stages_to_drop.append(id)
    for id in stages_to_drop:
      print "Dropping stage %s" % id
      del self.stages[id]

  def all_tasks(self):
    """ Returns a list of all tasks. """
    return [task for stage in self.stages.values() for task in stage.tasks]

  def print_stage_info(self):
    for id, stage in self.stages.iteritems():
      print "STAGE %s: %s" % (id, stage.verbose_str())

  def print_heading(self, text):
    print "\n******** %s ********" % text

  def get_simulated_runtime(self, waterfall_prefix=""):
    """ Returns the simulated runtime for the job.

    This should be approximately the same as the original runtime of the job, except
    that it doesn't include scheduler delay.

    If a non-empty waterfall_prefix is passed in, makes a waterfall plot based on the simulated
    runtimes.
    """
    total_runtime = 0
    all_start_finish_times = []
    for id, stage in self.stages.iteritems():
      tasks = sorted(stage.tasks, key = lambda task: task.start_time)
      simulated_runtime, start_finish_times = simulate.simulate(
        [t.runtime() for t in tasks], concurrency.get_max_concurrency(tasks))
      start_finish_times_adjusted = [
        (start + total_runtime, finish + total_runtime) for start, finish in start_finish_times]
      all_start_finish_times.append(start_finish_times_adjusted)
      total_runtime += simulated_runtime

    if waterfall_prefix:
      self.write_simulated_waterfall(all_start_finish_times, "%s_simulated" % waterfall_prefix)
    return total_runtime 

  def simulated_runtime_over_actual(self, prefix):
    simulated_runtime = self.get_simulated_runtime(waterfall_prefix=prefix)
    # TODO: Incorporate Shark setup time here!
    print "Simulated runtime: ", simulated_runtime, "actual time: ", self.original_runtime()
    return simulated_runtime * 1.0 / self.original_runtime()

  def original_runtime(self):
    actual_start_time = min([s.start_time for s in self.stages.values()])
    actual_finish_time = max([s.finish_time() for s in self.stages.values()])
    return actual_finish_time - actual_start_time

  def calculate_speedup(self, description, compute_base_runtime, compute_faster_runtime):
    """ Returns how much faster the job would have run if each task had a faster runtime.

    Parameters:
      description: A description for the speedup, which will be printed to the command line.
      compute_base_runtime: Function that accepts a task and computes the runtime for that task.
        The resulting runtime will be used as the "base" time for the job, which the faster time
        will be compared to.
      compute_faster_runtime: Function that accepts a task and computes the new runtime for that
        task. The resulting job runtime will be compared to the job runtime using
        compute_base_runtime.

    Returns:
      A 3-item tuple: [relative speedup, original runtime, estimated new runtime]
        TODO: Right now this doesn't return the actual original runtime; just the estimated one (so
        that it is comparable to the estimated new one).
    """
    self.print_heading(description)
    # Making these single-element lists is a hack to ensure that they can be accessed from
    # inside the nested add_tasks_to_totals() function.
    total_time = [0]
    total_faster_time = [0]

    def add_tasks_to_totals(unsorted_tasks):
      # Sort the tasks by the start time, not the finish time -- otherwise the longest tasks
      # end up getting run last, which can artificially inflate job completion time.
      tasks = sorted(unsorted_tasks, key = lambda task: task.start_time)
      max_concurrency = concurrency.get_max_concurrency(tasks)

      # Get the runtime for the stage
      task_runtimes = [compute_base_runtime(task) for task in tasks]
      base_runtime = simulate.simulate(task_runtimes, max_concurrency)[0]
      total_time[0] += base_runtime

      faster_runtimes = [compute_faster_runtime(task) for task in tasks]
      faster_runtime = simulate.simulate(faster_runtimes, max_concurrency)[0]
      total_faster_time[0] += faster_runtime
      print "Base: %s, faster: %s" % (base_runtime, faster_runtime)

    for id, stage in self.stages.iteritems():
      print "STAGE", id, stage
      add_tasks_to_totals(stage.tasks)

    print "Faster time: %s, base time: %s" % (total_faster_time[0], total_time[0])
    return total_faster_time[0] * 1.0 / total_time[0], total_time[0], total_faster_time[0]

  def no_scheduler_delay_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without scheduler delay",
      lambda t: t.runtime(),
      lambda t: (t.runtime() - t.scheduler_delay))

  def no_map_output_fetch_speedup(self):
    def get_time_with_no_output_fetch(t):
      if t.has_fetch:
        return t.runtime() - t.map_output_fetch_wait
      else:
        return t.runtime()

    return self.calculate_speedup(
      "Computing speedup without time to fetch map output locations",
      lambda t: t.runtime(),
      lambda t: get_time_with_no_output_fetch(t))

  def no_broadcast_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without time to retrieve broadcast variables",
      lambda t: t.runtime(),
      lambda t: (t.runtime() - t.broadcast_block_time))

  def no_overhead_speedup(self):
    """
    Returns the speedup if the task hadn't had any of the overheads that drizzle attempts
    to eliminate: no time to retrieve broadcast variables, no time to fetch map outputs,
    and no scheduler delay.
    """
    def get_time_with_no_overhead(t):
      map_output_fetch_time = 0
      if t.has_fetch:
        map_output_fetch_time = t.map_output_fetch_wait
      return t.runtime() - t.scheduler_delay - map_output_fetch_time - t.broadcast_block_time

    return self.calculate_speedup(
      "Computing speedup without any scheduling overheads",
      lambda t: t.runtime(),
      lambda t: get_time_with_no_overhead(t))

  def no_network_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without network",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_network())

  def no_disk_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without disk",
      lambda t: t.runtime(),
      lambda t: t.runtime_no_disk())

  def no_gc_speedup(self):
    return self.calculate_speedup(
      "Computing speedup without gc",
      lambda t: t.runtime(),
      lambda t: t.runtime() - t.gc_time)

  def write_simulated_waterfall(self, start_finish_times, prefix):
    """ Outputs a gnuplot file that visually shows all task runtimes.
    
    start_finish_times is expected to be a list of lists, one for each stage,
    where the list for a particular stage contains the start and finish times
    for each task.
    """
    cumulative_tasks = 0
    stage_cumulative_tasks = []
    all_times = []
    # Sort stages by the start time of the first task.
    for stage_times in sorted(start_finish_times):
      all_times.extend(stage_times)
      cumulative_tasks = cumulative_tasks + len(stage_times)
      stage_cumulative_tasks.append(str(cumulative_tasks))

    base_file = open("waterfall_base.gp", "r")
    plot_file = open("%s_waterfall.gp" % prefix, "w")
    for line in base_file:
      plot_file.write(line)
    base_file.close()

    LINE_TEMPLATE = "set arrow from %s,%s to %s,%s ls %s nohead\n"

    # Write all time relative to the first start time so the graph is easier to read.
    first_start = all_times[0][0]
    for i, start_finish in enumerate(all_times):
      start = start_finish[0] - first_start
      finish = start_finish[1] - first_start
      # Write data to plot file.
      plot_file.write(LINE_TEMPLATE % (start, i, finish, i, 3))

    last_end = all_times[-1][1]
    ytics_str = ",".join(stage_cumulative_tasks)
    plot_file.write("set ytics (%s)\n" % ytics_str)
    plot_file.write("set xrange [0:%s]\n" % (last_end - first_start))
    plot_file.write("set yrange [0:%s]\n" % len(all_times))
    plot_file.write("set output \"%s_waterfall.pdf\"\n" % prefix)
    plot_file.write("plot -1\n")

    plot_file.close()

  def write_waterfall(self, prefix, pdf_relative_path=False, title=""):
    """ Outputs a gnuplot file that visually shows all task runtimes. """
    all_tasks = []
    cumulative_tasks = 0
    stage_cumulative_tasks = []
    for stage in sorted(self.stages.values(), key = lambda x: x.start_time):
      all_tasks.extend(sorted(stage.tasks, key = lambda x: x.start_time))
      cumulative_tasks = cumulative_tasks + len(stage.tasks)
      stage_cumulative_tasks.append(str(cumulative_tasks))

    base_file = open("waterfall_base.gp", "r")
    plot_file = open("%s_waterfall.gp" % prefix, "w")
    for line in base_file:
      plot_file.write(line)
    base_file.close()

    if title != "":
      plot_file.write("set title \"%s\"\n" % title)
    

    LINE_TEMPLATE = "set arrow from %s,%s to %s,%s ls %s nohead\n"

    # Write all time relative to the first start time so the graph is easier to read.
    first_start = all_tasks[0].start_time
    for i, task in enumerate(all_tasks):
      start = task.start_time - first_start
      # Show the scheduler delay at the beginning -- but it could be at the beginning or end or
      # split.
      scheduler_delay_end = start + task.scheduler_delay
      deserialize_end = scheduler_delay_end + task.executor_deserialize_time
      # Show future task queue time next
      future_task_queue_end = deserialize_end + task.future_task_queue_time
      # Show time spent in the executor threadpool queue next
      executor_queue_delay_end = future_task_queue_end + task.executor_queue_delay
      # TODO: input_read_time should only be included when the task reads input data from
      # HDFS, but with the current logging it's also recorded when data is read from memory,
      # so should be included here to make the task end time line up properly.
      hdfs_read_end = executor_queue_delay_end + task.input_read_time
      local_read_end = hdfs_read_end
      fetch_wait_end = hdfs_read_end
      if task.has_fetch:
        local_read_end = executor_queue_delay_end + task.local_read_time
        fetch_wait_end = local_read_end + task.fetch_wait + task.map_output_fetch_wait 
      # Here, assume GC happens as part of compute (although we know that sometimes
      # GC happens during fetch wait.
      serialize_millis = task.result_serialization_time
      serialize_end = fetch_wait_end + serialize_millis
      compute_end = (fetch_wait_end + task.compute_time_without_gc() - 
        task.executor_deserialize_time)
      gc_end = compute_end + task.gc_time
      task_end = gc_end + task.shuffle_write_time + task.output_write_time
      if math.fabs((first_start + task_end) - task.finish_time) >= 0.1:
        print "Mismatch at index %s" % i
        print "%.1f" % (first_start + task_end)
        print task.finish_time
        print task
        assert False

      # Write data to plot file.
      plot_file.write(LINE_TEMPLATE % (start, i, scheduler_delay_end, i, 6))
      plot_file.write(LINE_TEMPLATE % (scheduler_delay_end, i, deserialize_end, i, 8))
      plot_file.write(LINE_TEMPLATE % (deserialize_end, i, future_task_queue_end, i, 10))
      plot_file.write(LINE_TEMPLATE % (future_task_queue_end, i, executor_queue_delay_end, i, -1))
      if task.has_fetch:
        plot_file.write(LINE_TEMPLATE % (executor_queue_delay_end, i, local_read_end, i, 1))
        plot_file.write(LINE_TEMPLATE % (local_read_end, i, fetch_wait_end, i, 2))
        plot_file.write(LINE_TEMPLATE % (fetch_wait_end, i, serialize_end, i, 9))
      else:
        plot_file.write(LINE_TEMPLATE % (executor_queue_delay_end, i, hdfs_read_end, i, 7))
        plot_file.write(LINE_TEMPLATE % (hdfs_read_end, i, serialize_end, i, 9))
      plot_file.write(LINE_TEMPLATE % (serialize_end, i, compute_end, i, 3))
      plot_file.write(LINE_TEMPLATE % (compute_end, i, gc_end, i, 4))
      plot_file.write(LINE_TEMPLATE % (gc_end, i, task_end, i, 5))

    last_end = max([t.finish_time for t in all_tasks])
    ytics_str = ",".join(stage_cumulative_tasks)
    plot_file.write("set ytics (%s)\n" % ytics_str)
    plot_file.write("set xrange [0:%s]\n" % (last_end - first_start))
    plot_file.write("set yrange [0:%s]\n" % len(all_tasks))
    if pdf_relative_path:
      filename_only = os.path.basename(prefix)
      plot_file.write("set output \"%s_waterfall.pdf\"\n" % filename_only)
    else:
      plot_file.write("set output \"%s_waterfall.pdf\"\n" % prefix)

    # Hacky way to force a key to be printed.
    plot_file.write("plot -1 ls 6 title 'Scheduler delay',\\\n")
    plot_file.write(" -1 ls 8 title 'Task deserialization', \\\n")
    plot_file.write(" -1 ls 10 title 'Future Task Queue', \\\n")
    plot_file.write(" -1 ls -1 title 'Executor Queue Delay', \\\n")
    plot_file.write("-1 ls 2 title 'Network wait', -1 ls 3 title 'Compute', \\\n")
    plot_file.write("-1 ls 4 title 'GC', \\\n")
    plot_file.write("-1 ls 5 title 'Output write wait'\\\n")
    plot_file.close()

