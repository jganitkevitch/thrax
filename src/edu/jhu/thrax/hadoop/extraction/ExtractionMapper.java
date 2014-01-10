package edu.jhu.thrax.hadoop.extraction;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

import edu.jhu.thrax.hadoop.datatypes.AlignedRuleWritable;
import edu.jhu.thrax.hadoop.datatypes.Annotation;
import edu.jhu.thrax.input.ThraxInput;
import edu.jhu.thrax.input.ThraxInputParser;
import edu.jhu.thrax.util.Vocabulary;
import edu.jhu.thrax.util.exceptions.MalformedInputException;

public class ExtractionMapper extends Mapper<LongWritable, Text, AlignedRuleWritable, Annotation> {
  
  private ThraxInputParser parser;
  private RuleWritableExtractor extractor;

  protected void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    String vocabulary_path = conf.getRaw("thrax.work-dir") + "vocabulary/part-*";
    Vocabulary.initialize(conf, vocabulary_path);

    parser = new ThraxInputParser(conf);
    
    // TODO: static initializer call for what Annotation actually carries would go here.
    extractor = RuleWritableExtractorFactory.create(context);
    if (extractor == null)
      System.err.println("WARNING: could not create rule extractor as configured!");
  }

  protected void map(LongWritable key, Text value, Context context) throws IOException,
      InterruptedException {
    if (extractor == null) return;
    try {
      ThraxInput input = parser.parse(value.toString());
      for (AnnotatedRule ar : extractor.extract(input))
        context.write(new AlignedRuleWritable(ar.rule, ar.f2e), ar.annotation);
      context.progress();
    } catch (MalformedInputException e) {
      context.getCounter("input errors", e.getMessage()).increment(1);
    }
  }
}
