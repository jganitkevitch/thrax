package edu.jhu.thrax.hadoop.jobs;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Partitioner;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import edu.jhu.thrax.input.ThraxInputParser;
import edu.jhu.thrax.util.Vocabulary;
import edu.jhu.thrax.util.exceptions.MalformedInputException;

public class VocabularyJob implements ThraxJob {

  public VocabularyJob() {}

  public Job getJob(Configuration conf) throws IOException {
    Job job = new Job(conf, "vocabulary");
    job.setJarByClass(VocabularyJob.class);

    job.setMapperClass(VocabularyJob.Map.class);
    job.setCombinerClass(VocabularyJob.Combine.class);
    job.setReducerClass(VocabularyJob.Reduce.class);

    job.setMapOutputKeyClass(Text.class);
    job.setMapOutputValueClass(NullWritable.class);

    job.setOutputKeyClass(IntWritable.class);
    job.setOutputValueClass(Text.class);

    job.setSortComparatorClass(Text.Comparator.class);

    job.setOutputFormatClass(SequenceFileOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(conf.get("thrax.input-file")));
    int maxSplitSize = conf.getInt("thrax.max-split-size", 0);
    if (maxSplitSize != 0) FileInputFormat.setMaxInputSplitSize(job, maxSplitSize);

    FileOutputFormat.setOutputPath(job, new Path(conf.get("thrax.work-dir") + "vocabulary"));

    int num_reducers = conf.getInt("thrax.reducers", conf.getInt("mapreduce.job.reduces", DefaultValues.DEFAULT_NUM_REDUCERS));
    job.setNumReduceTasks(num_reducers);

    return job;
  }

  public String getOutputSuffix() {
    return "vocabulary";
  }

  @Override
  public String getName() {
    return "vocabulary";
  }

  private static class Map extends Mapper<LongWritable, Text, Text, NullWritable> {

    private boolean allowConstituent = true;
    private boolean allowCCG = true;
    private boolean allowConcat = true;
    private boolean allowDoubleConcat = true;

    private ThraxInputParser parser;

    protected void setup(Context context) {
      Configuration conf = context.getConfiguration();

      allowConstituent = conf.getBoolean("thrax.allow-constituent-label", true);
      allowCCG = conf.getBoolean("thrax.allow-ccg-label", true);
      allowConcat = conf.getBoolean("thrax.allow-concat-label", true);
      allowDoubleConcat = conf.getBoolean("thrax.allow-double-plus", true);

      parser = new ThraxInputParser(conf);
    }

    protected void map(LongWritable key, Text line, Context context) throws IOException,
        InterruptedException {
      try {
        Vocabulary.clear();

        parser.parse(line.toString());

        Set<String> nonterminals = new HashSet<String>();
        for (int i = 1; i < Vocabulary.size(); ++i) {
          String word = Vocabulary.word(i);
          if (!Vocabulary.nt(word))
            context.write(new Text(word), NullWritable.get());
          else
            nonterminals.add(word.substring(0, word.length() - 1));
        }
        combineNonterminals(context, nonterminals);
      } catch (MalformedInputException e) {}
    }

    private void combineNonterminals(Context context, Set<String> nonterminals) throws IOException,
        InterruptedException {
      if (allowConstituent) writeNonterminals(nonterminals, context);
      if (allowConcat) {
        Set<String> concatenated = joinNonterminals("+", nonterminals, nonterminals);
        writeNonterminals(concatenated, context);
      }
      if (allowCCG) {
        Set<String> forward = joinNonterminals("/", nonterminals, nonterminals);
        writeNonterminals(forward, context);
        Set<String> backward = joinNonterminals("\\", nonterminals, nonterminals);
        writeNonterminals(backward, context);
      }
      if (allowDoubleConcat) {
        Set<String> concat = joinNonterminals("+", nonterminals, nonterminals);
        Set<String> double_concat = joinNonterminals("+", concat, nonterminals);
        writeNonterminals(double_concat, context);
      }
    }

    private Set<String> joinNonterminals(String glue, Set<String> prefixes, Set<String> nonterminals) {
      Set<String> joined = new HashSet<String>();
      for (String prefix : prefixes)
        for (String nt : nonterminals)
          joined.add(prefix + glue + nt.substring(1));
      return joined;
    }

    private static void writeNonterminals(Set<String> nts, Context context) throws IOException,
        InterruptedException {
      for (String nt : nts)
        context.write(new Text(nt + "]"), NullWritable.get());
    }
  }

  public static class VocabularyPartitioner extends Partitioner<Text, Writable> {
    public int getPartition(Text key, Writable value, int numPartitions) {
      return (key.hashCode() & Integer.MAX_VALUE) % numPartitions;
    }
  }

  private static class Combine extends Reducer<Text, NullWritable, Text, NullWritable> {

    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
        throws IOException, InterruptedException {
      context.write(key, NullWritable.get());
    }
  }

  private static class Reduce extends Reducer<Text, NullWritable, IntWritable, Text> {

    private int reducerNumber;
    private int numReducers;

    protected void setup(Context context) throws IOException, InterruptedException {
      numReducers = context.getNumReduceTasks();
      reducerNumber = context.getTaskAttemptID().getTaskID().getId();

      Vocabulary.initialize(context.getConfiguration());
    }

    protected void reduce(Text key, Iterable<NullWritable> values, Context context)
        throws IOException, InterruptedException {
      String token = key.toString();
      if (token == null || token.isEmpty()) throw new RuntimeException("Unexpected empty token.");
      Vocabulary.id(token);
      context.progress();
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      for (int i = Vocabulary.head(); i < Vocabulary.size(); ++i)
        context.write(new IntWritable((i - 1) * numReducers + reducerNumber + 1), new Text(
            Vocabulary.word(i)));
    }
  }

  @Override
  public Set<Class<? extends ThraxJob>> getPrerequisites() {
    return new HashSet<Class<? extends ThraxJob>>();
  }
}
