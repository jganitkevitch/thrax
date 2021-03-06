package edu.jhu.thrax.hadoop.features.mapreduce;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.jhu.thrax.hadoop.datatypes.FeaturePair;
import edu.jhu.thrax.hadoop.datatypes.RuleWritable;
import edu.jhu.thrax.hadoop.features.Feature;
import edu.jhu.thrax.hadoop.jobs.DefaultValues;
import edu.jhu.thrax.hadoop.jobs.ExtractionJob;
import edu.jhu.thrax.hadoop.jobs.ThraxJob;

@SuppressWarnings("rawtypes")
public abstract class MapReduceFeature implements Feature, ThraxJob {
  
  protected static HashSet<Class<? extends ThraxJob>> prereqs =
      new HashSet<Class<? extends ThraxJob>>();

  public Set<Class<? extends ThraxJob>> getPrerequisites() {
    prereqs.add(ExtractionJob.class);
    return prereqs;
  }

  public static void addPrerequisite(Class<? extends ThraxJob> c) {
    prereqs.add(c);
  }
  
  public String getOutputSuffix() {
    return getName();
  }

  public Class<? extends Reducer> combinerClass() {
    return FloatSumReducer.class;
  }

  public abstract Class<? extends Mapper> mapperClass();

  public abstract Class<? extends WritableComparator> sortComparatorClass();

  public abstract Class<? extends Partitioner> partitionerClass();

  public abstract Class<? extends Reducer> reducerClass();

  public Job getJob(Configuration conf) throws IOException {
    String name = getName();
    Job job = Job.getInstance(conf, name);
    job.setJarByClass(this.getClass());

    job.setMapperClass(this.mapperClass());
    job.setCombinerClass(this.combinerClass());
    job.setSortComparatorClass(this.sortComparatorClass());
    job.setPartitionerClass(this.partitionerClass());
    job.setReducerClass(this.reducerClass());

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setOutputKeyClass(RuleWritable.class);
    job.setOutputValueClass(FeaturePair.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    setMapOutputFormat(job);
    
    int num_reducers = conf.getInt("thrax.reducers", conf.getInt("mapreduce.job.reduces", DefaultValues.DEFAULT_NUM_REDUCERS));
    job.setNumReduceTasks(num_reducers);

    FileInputFormat.setInputPaths(job, new Path(conf.get("thrax.work-dir") + "rules"));
    FileOutputFormat.setOutputPath(job, new Path(conf.get("thrax.work-dir") + name));
    return job;
  }

  public abstract void unaryGlueRuleScore(int nt, Map<Integer, Writable> map);

  public abstract void binaryGlueRuleScore(int nt, Map<Integer, Writable> map);
  
  protected void setMapOutputFormat(Job job) {
    job.setMapOutputKeyClass(RuleWritable.class);
    job.setMapOutputValueClass(FloatWritable.class);
  }
}
