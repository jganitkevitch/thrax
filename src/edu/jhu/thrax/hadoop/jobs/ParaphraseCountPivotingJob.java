package edu.jhu.thrax.hadoop.jobs;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.jhu.thrax.hadoop.datatypes.AlignedRuleWritable;
import edu.jhu.thrax.hadoop.datatypes.Annotation;
import edu.jhu.thrax.hadoop.datatypes.RuleWritable;
import edu.jhu.thrax.util.Vocabulary;

public class ParaphraseCountPivotingJob implements ThraxJob {

  private static HashSet<Class<? extends ThraxJob>> prereqs =
      new HashSet<Class<? extends ThraxJob>>();

  public static void addPrerequisite(Class<? extends ThraxJob> c) {
    prereqs.add(c);
  }

  public Set<Class<? extends ThraxJob>> getPrerequisites() {
    prereqs.add(ExtractionJob.class);
    return prereqs;
  }

  public Job getJob(Configuration conf) throws IOException {
    Job job = new Job(conf, "count-pivoting");

    job.setJarByClass(ParaphraseCountPivotingJob.class);

    job.setMapperClass(CountPivotingMapper.class);
    job.setReducerClass(CountPivotingReducer.class);

    job.setInputFormatClass(SequenceFileInputFormat.class);
    job.setMapOutputKeyClass(RuleWritable.class);
    job.setMapOutputValueClass(Annotation.class);
    job.setOutputKeyClass(AlignedRuleWritable.class);
    job.setOutputValueClass(Annotation.class);
    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    job.setSortComparatorClass(RuleWritable.YieldComparator.class);
    job.setPartitionerClass(RuleWritable.SourcePartitioner.class);

    FileInputFormat.setInputPaths(job, new Path(conf.get("thrax.work-dir") + "translation-rules"));
    int maxSplitSize = conf.getInt("thrax.max-split-size", 0);
    if (maxSplitSize != 0) FileInputFormat.setMaxInputSplitSize(job, maxSplitSize * 20);

    int numReducers = conf.getInt("thrax.reducers", 4);
    job.setNumReduceTasks(numReducers);

    FileOutputFormat.setOutputPath(job, new Path(conf.get("thrax.work-dir") + "count-pivoted"));

    return job;
  }

  public String getName() {
    return "count-pivoting";
  }

  public String getOutputSuffix() {
    return "count-pivoted";
  }
}


class CountPivotingMapper extends Mapper<RuleWritable, Annotation, RuleWritable, Annotation> {

  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    Vocabulary.initialize(conf);
  }

  protected void map(RuleWritable key, Annotation value, Context context) throws IOException,
      InterruptedException {
    context.write(key, value);
    context.progress();
  }
}


class CountPivotingReducer
    extends Reducer<RuleWritable, Annotation, AlignedRuleWritable, Annotation> {

  private static enum PivotingCounters {
    F_READ, EF_READ, EF_PRUNED, EE_PRUNED, EE_WRITTEN
  }

  private int[] currentSrc;
  private int currentLhs;

  private int[] nts;
  private int lhs;

  private List<ParaphrasePattern> targets;

  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    String vocabulary_path = conf.getRaw("thrax.work-dir") + "vocabulary/part-*";
    Vocabulary.initialize(conf, vocabulary_path);

    currentLhs = 0;
    currentSrc = null;

    lhs = 0;
    nts = null;
    targets = new ArrayList<ParaphrasePattern>();
  }

  protected void reduce(RuleWritable key, Iterable<Annotation> values, Context context)
      throws IOException, InterruptedException {
    if (currentLhs == 0 || !(key.lhs == currentLhs && Arrays.equals(key.source, currentSrc))) {
      if (currentLhs != 0) pivotAll(context);
      currentLhs = key.lhs;
      currentSrc = key.source;
      lhs = currentLhs;
      nts = extractNonterminals(currentSrc);
      targets.clear();
    }
    boolean seen_first = false;
    for (Annotation annotation : values) {
      if (seen_first)
        throw new RuntimeException("Multiple annotations for one rule:" + key.toString());
      seen_first = true;
      targets.add(new ParaphrasePattern(key.target, nts, lhs, key.monotone, annotation));
    }
  }

  protected void cleanup(Context context) throws IOException, InterruptedException {
    if (currentLhs != 0) pivotAll(context);
  }

  protected void pivotAll(Context context) throws IOException, InterruptedException {
    context.getCounter(PivotingCounters.F_READ).increment(1);
    context.getCounter(PivotingCounters.EF_READ).increment(targets.size());

    for (int i = 0; i < targets.size(); i++) {
      for (int j = i; j < targets.size(); j++) {
        pivotOne(targets.get(i), targets.get(j), context);
        if (i != j) pivotOne(targets.get(j), targets.get(i), context);
      }
    }
  }

  protected void pivotOne(ParaphrasePattern src, ParaphrasePattern tgt, Context context)
      throws IOException, InterruptedException {
    RuleWritable pivoted_rule = new RuleWritable();
    Annotation pivoted_annotation =
        new Annotation(src.annotation.count() * tgt.annotation.count(), Math.min(
            src.annotation.support(), tgt.annotation.support()));
    pivoted_rule.lhs = src.lhs;
    pivoted_rule.source = src.rhs;
    pivoted_rule.target = tgt.rhs;
    pivoted_rule.monotone = (src.monotone == tgt.monotone);

    AlignedRuleWritable arw =
        new AlignedRuleWritable(pivoted_rule, src.annotation.f2e().join(tgt.annotation.f2e()));

    context.write(arw, pivoted_annotation);
    context.getCounter(PivotingCounters.EE_WRITTEN).increment(1);
  }

  protected static int[] extractNonterminals(int[] source) {
    int first_nt = 0;
    for (int token : source)
      if (Vocabulary.nt(token)) {
        if (first_nt == 0)
          first_nt = token;
        else
          return new int[] {first_nt, token};
      }
    return (first_nt == 0 ? new int[0] : new int[] {first_nt});
  }

  class ParaphrasePattern {
    int arity;
    int lhs;
    int[] rhs;
    boolean monotone;
    Annotation annotation;

    public ParaphrasePattern(int[] target, int[] nts, int lhs, boolean mono, Annotation a) {
      this.arity = nts.length;
      this.lhs = lhs;
      this.rhs = target;
      this.monotone = mono;
      this.annotation = new Annotation(a);
    }
  }
}
