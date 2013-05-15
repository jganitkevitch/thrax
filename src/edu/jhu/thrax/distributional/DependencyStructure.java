package edu.jhu.thrax.distributional;

import java.util.ArrayList;
import java.util.Map;

import edu.jhu.thrax.distributional.FeatureTypes.Directionality;
import edu.jhu.thrax.distributional.FeatureTypes.Factor;
import edu.jhu.thrax.distributional.FeatureTypes.Flavor;
import edu.jhu.thrax.util.FormatUtils;
import edu.jhu.thrax.util.Utils;

public class DependencyStructure {

  private ArrayList<Dependency>[] govern;
  private Dependency[] depend;

  @SuppressWarnings("unchecked")
  public DependencyStructure(String input, AnnotatedSentence sentence, boolean[] factors) {
    govern = new ArrayList[sentence.length];
    depend = new Dependency[sentence.length];
    
    for (int i = 0; i < sentence.length; i++)
      govern[i] = new ArrayList<Dependency>();

    String[] entries = FormatUtils.P_SPACE.split(input.trim());
    for (String entry : entries) {
      Dependency d = new Dependency(entry, factors, sentence);
      if (d.gov >= 0) govern[d.gov].add(d);
      depend[d.dep] = d;
    }
  }

  public void addFeatures(Map<String, Integer> features, AnnotatedSentence sentence, int from,
      int to, boolean[] factors) {
    int head = from;
    boolean seen_outlink = false;
    boolean valid = true;
    for (int p = from; p < to; p++) {
      if (depend[p] != null) {
        if (depend[p].gov < from || depend[p].gov >= to) {
          depend[p].addDependingFeatures(features, factors);
          valid = valid && !seen_outlink;
          if (valid) head = p;
          seen_outlink = true;
        } else if (valid && p == head) {
          head = depend[p].gov;
        }
      } else if (govern[p].isEmpty()) {
        valid = false;
      }
      for (Dependency d : govern[p]) {
        if (d.dep < from || d.dep >= to) {
          d.addGoverningFeatures(features, factors);
          valid = false;
        }
      }
    }
    if (valid) {
      for (Factor f : Factor.values())
        if (factors[f.code])
          Utils.increment(features,
              Directionality.CENTER.name + Flavor.HEAD.name + f.name + sentence.get(f, head));
    }
  }
}
