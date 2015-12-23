package edu.jhu.thrax.hadoop.jobs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.jhu.thrax.hadoop.datatypes.AlignedRuleWritable;
import edu.jhu.thrax.hadoop.datatypes.Annotation;
import edu.jhu.thrax.hadoop.datatypes.RuleWritable;
import edu.jhu.thrax.hadoop.extraction.ExtractionCombiner;
import edu.jhu.thrax.hadoop.extraction.ExtractionReducer;
import edu.jhu.thrax.util.Vocabulary;

public class ParaphraseCountAggregationJob implements ThraxJob {

  private static HashSet<Class<? extends ThraxJob>> prereqs =
      new HashSet<Class<? extends ThraxJob>>();

  public Job getJob(Configuration conf) throws IOException {
    Job job = Job.getInstance(conf, "count-aggregate");

    job.setJarByClass(ExtractionReducer.class);

    job.setMapperClass(CountAggregationMapper.class);
    job.setCombinerClass(ExtractionCombiner.class);
    job.setReducerClass(ExtractionReducer.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(AlignedRuleWritable.class);
    job.setMapOutputValueClass(Annotation.class);
    job.setOutputKeyClass(RuleWritable.class);
    job.setOutputValueClass(Annotation.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setSortComparatorClass(AlignedRuleWritable.RuleYieldComparator.class);
    job.setPartitionerClass(AlignedRuleWritable.RuleYieldPartitioner.class);

    FileInputFormat.setInputPaths(job, new Path(conf.get("thrax.work-dir") + "count-pivoted"));
    int maxSplitSize = conf.getInt("thrax.max-split-size", 0);
    if (maxSplitSize != 0) FileInputFormat.setMaxInputSplitSize(job, maxSplitSize * 20);

    int numReducers = conf.getInt("thrax.reducers", 4);
    job.setNumReduceTasks(numReducers);

    FileOutputFormat.setOutputPath(job, new Path(conf.get("thrax.work-dir") + "rules"));

    return job;
  }

  public String getName() {
    return "count-aggregate";
  }

  public static void addPrerequisite(Class<? extends ThraxJob> c) {
    prereqs.add(c);
  }

  public Set<Class<? extends ThraxJob>> getPrerequisites() {
    prereqs.add(ParaphraseCountPivotingJob.class);
    return prereqs;
  }

  public String getOutputSuffix() {
    return null;
  }
}


class CountAggregationMapper
    extends Mapper<AlignedRuleWritable, Annotation, AlignedRuleWritable, Annotation> {

  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Vocabulary.initialize(conf);
  }

  protected void map(AlignedRuleWritable key, Annotation value, Context context)
      throws IOException, InterruptedException {
    context.write(key, value);
    context.progress();
  }
}
