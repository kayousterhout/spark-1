import collections
import json
import logging
import numpy
from optparse import OptionParser
import sys

from job import Job

def get_json(line): 
  # Need to first strip the trailing newline, and then escape newlines (which can appear
  # in the middle of some of the JSON) so that JSON library doesn't barf.
  return json.loads(line.strip("\n").replace("\n", "\\n"))

class Analyzer:
  def __init__(self, filename, parse_as_single_job=False, pdf_relative_path=False):
    self.filename = filename
    self.pdf_relative_path = pdf_relative_path
    self.jobs = collections.defaultdict(Job)
    # For each stage, jobs that rely on the stage.
    self.jobs_for_stage = {}
    self.app_name = ""

    f = open(filename, "r")
    for line in f:
      json_data = get_json(line)
      event_type = json_data["Event"]
      if event_type == "SparkListenerJobStart":
        if parse_as_single_job:
          job_id = 0
        else:
          job_id = json_data["Job ID"]
        # Avoid using "Stage Infos" here, which was added in 1.2.0.
        stage_ids = json_data["Stage IDs"]
        print "Stage ids: %s" % stage_ids
        for stage_id in stage_ids:
          if stage_id not in self.jobs_for_stage:
            self.jobs_for_stage[stage_id] = [job_id]
          else:
            self.jobs_for_stage[stage_id].append(job_id)
      elif event_type == "SparkListenerTaskEnd":
        stage_id = json_data["Stage ID"]
        # Add the event to all of the jobs that depend on the stage.
        for job_id in self.jobs_for_stage[stage_id]:
          self.jobs[job_id].add_event(json_data)
      elif event_type == "SparkListenerEnvironmentUpdate":
        self.app_name = json_data["Spark Properties"]["spark.app.name"]

    print "Finished reading input data:"
    for job_id, job in self.jobs.iteritems():
      job.remove_empty_stages()
      print "Job", job_id, " has stages: ", job.stages.keys()

  def output_all_waterfalls(self):
    for job_id, job in self.jobs.iteritems():
      filename = "%s_%s" % (self.filename, job_id)
      job.write_waterfall(filename, self.pdf_relative_path, self.app_name)

  def output_all_job_info(self):
    for job_id, job in self.jobs.iteritems():
      filename = "%s_%s" % (self.filename, job_id)
      self.__output_job_info(job, filename)

  def __output_job_info(self, job, filename):
    #job.print_stage_info()
    job.print_heading("Job")

    job.write_waterfall(filename, self.pdf_relative_path, self.app_name)

    no_scheduler_delay_speedup = job.no_scheduler_delay_speedup()[0]
    no_map_output_fetch_speedup = job.no_map_output_fetch_speedup()[0]
    no_broadcast_speedup = job.no_broadcast_speedup()[0]
    no_overhead_speedup = job.no_overhead_speedup()[0]
    print "\n  Drizzle potential speedups:"
    print "     Eliminate scheduler delay:", no_scheduler_delay_speedup
    print "     Eliminate broadcast time:", no_broadcast_speedup
    print "     Eliminate time to fetch map output locations:", no_map_output_fetch_speedup
    print "     Eliminate all 3 overheads:", no_overhead_speedup

    print "\nSpeedup from eliminating all GC:", job.no_gc_speedup()[0]
    
def main(argv):
  parser = OptionParser(usage="parse_logs.py [options] <log filename>")
  parser.add_option(
      "-d", "--debug", action="store_true", default=False,
      help="Enable additional debug logging")
  parser.add_option(
      "-w", "--waterfall-only", action="store_true", default=False,
      help="Output only the visualization for each job (not other stats)")
  parser.add_option(
      "--pdf-relative-path", action="store_true", default=False,
      help="Set the PDF path for waterfall files to be relative")
  parser.add_option(
      "-s", "--parse-as-single-job", action="store_true", default=False,
      help="Parse the log as a single job, resulting in a single waterfall plot that " +
      "includes all tasks across all jobs")
  (opts, args) = parser.parse_args()
  if len(args) != 1:
    parser.print_help()
    sys.exit(1)
 
  if opts.debug:
    logging.basicConfig(level=logging.DEBUG)
  else:
    logging.basicConfig(level=logging.INFO)
  filename = args[0]
  if filename is None:
    parser.print_help()
    sys.exit(1)

  analyzer = Analyzer(filename, opts.parse_as_single_job, opts.pdf_relative_path)

  if opts.waterfall_only:
    analyzer.output_all_waterfalls()
  else:
    analyzer.output_all_job_info()

if __name__ == "__main__":
  main(sys.argv[1:])
