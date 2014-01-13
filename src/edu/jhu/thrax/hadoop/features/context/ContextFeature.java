package edu.jhu.thrax.hadoop.features.context;

import java.io.IOException;

import org.apache.hadoop.mapreduce.Reducer.Context;

import edu.jhu.thrax.datatypes.HierarchicalRule;
import edu.jhu.thrax.extraction.SpanLabeler;
import edu.jhu.thrax.hadoop.datatypes.Annotation;
import edu.jhu.thrax.hadoop.features.Feature;
import edu.jhu.thrax.input.ThraxInput;

public interface ContextFeature extends Feature {

  @SuppressWarnings("rawtypes")
  public void init(Context context) throws IOException, InterruptedException;

  public void addScore(Annotation annotation, HierarchicalRule r, SpanLabeler spanLabeler,
      ThraxInput input);
}
