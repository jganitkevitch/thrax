package edu.jhu.thrax.hadoop.features.context;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapreduce.Reducer.Context;

import edu.jhu.thrax.datatypes.HierarchicalRule;
import edu.jhu.thrax.extraction.SpanLabeler;
import edu.jhu.thrax.hadoop.datatypes.Annotation;
import edu.jhu.thrax.input.ThraxInput;
import edu.jhu.thrax.util.Vocabulary;

public class FractionalCountFeature implements ContextFeature {

  public static final String NAME = "fraccount";
  public static final String LABEL = "FracCount";

  private static final FloatWritable ZERO = new FloatWritable(0);
  
  @Override
  public String getName() {
    return NAME;
  }

  @Override
  public String getLabel() {
    return LABEL;
  }

  @Override
  public void unaryGlueRuleScore(int nt, Map<Integer, Writable> map) {
    map.put(Vocabulary.id(LABEL), ZERO);
  }

  @Override
  public void binaryGlueRuleScore(int nt, Map<Integer, Writable> map) {
    map.put(Vocabulary.id(LABEL), ZERO);
  }

  @Override
  @SuppressWarnings("rawtypes") 
  public void init(Context context) throws IOException, InterruptedException {}

  @Override
  public void addScore(Annotation annotation, HierarchicalRule r, SpanLabeler spanLabeler,
      ThraxInput input) {
    if (input.weights != null && input.weights.length > 0)
      annotation.setFractional(input.weights[0]);
  }

}
