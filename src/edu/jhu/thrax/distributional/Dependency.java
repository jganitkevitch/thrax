package edu.jhu.thrax.distributional;

import java.util.Map;

import edu.jhu.thrax.distributional.FeatureTypes.Directionality;
import edu.jhu.thrax.distributional.FeatureTypes.Factor;
import edu.jhu.thrax.distributional.FeatureTypes.Flavor;
import edu.jhu.thrax.distributional.FeatureTypes.Type;
import edu.jhu.thrax.util.FormatUtils;
import edu.jhu.thrax.util.Utils;

public class Dependency {

  public static final String ROOT = "root";

  final String type;
  final int gov;
  final int dep;

  final String[] dep_features;
  final String[] gov_features;

  public Dependency(String entry, boolean[] labeling, AnnotatedSentence sentence, Type t) {
    String[] fields = FormatUtils.P_DASH.split(entry);

    type = fields[0];

    gov = Integer.parseInt(fields[1]) - 1;
    dep = Integer.parseInt(fields[2]) - 1;

    dep_features = new String[Factor.values().length];
    gov_features = new String[Factor.values().length];

    String dep_side = t.name + (gov > dep ? Directionality.RIGHT.name : Directionality.LEFT.name);
    String gov_side = t.name + (gov > dep ? Directionality.LEFT.name : Directionality.RIGHT.name);

    for (Factor f : Factor.values()) {
      if (labeling[f.code]) {
        dep_features[f.code] =
            dep_side + Flavor.DEP.name + type + f.name + (gov == -1 ? ROOT : sentence.get(f, gov));
        gov_features[f.code] = gov_side + Flavor.GOV.name + type + f.name + sentence.get(f, dep);
      } else {
        dep_features[f.code] = null;
        gov_features[f.code] = null;
      }
    }
  }

  final void addGoverningFeatures(Map<String, Integer> features, boolean[] labeling) {
    for (Factor f : Factor.values())
      if (labeling[f.code]) Utils.increment(features, gov_features[f.code]);
  }

  final void addDependingFeatures(Map<String, Integer> features, boolean[] labeling) {
    for (Factor f : Factor.values())
      if (labeling[f.code]) Utils.increment(features, dep_features[f.code]);
  }
}
