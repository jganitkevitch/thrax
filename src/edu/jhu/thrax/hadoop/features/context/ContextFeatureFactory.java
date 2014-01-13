package edu.jhu.thrax.hadoop.features.context;

import java.util.ArrayList;
import java.util.List;

import edu.jhu.thrax.util.FormatUtils;

public class ContextFeatureFactory {

  public static ContextFeature get(String name) {
    if (name.equals(FractionalCountFeature.NAME)) return new FractionalCountFeature();

    return null;
  }

  public static List<ContextFeature> getAll(String names) {
    String[] feature_names = FormatUtils.P_COMMA_OR_SPACE.split(names);
    List<ContextFeature> features = new ArrayList<ContextFeature>();

    for (String feature_name : feature_names) {
      ContextFeature feature = get(feature_name);
      if (feature != null) features.add(feature);
    }
    return features;
  }
}
