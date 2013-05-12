package edu.jhu.thrax.distributional;

import org.apache.hadoop.conf.Configuration;

import edu.jhu.thrax.distributional.FeatureTypes.Factor;
import edu.jhu.thrax.distributional.FeatureTypes.Type;


public class FeatureSet {

  // Maps factor-type (lex, ner, lem) to context window size.
  private int[] context;

  // Maps factor-type (lex, ner, lem) to max n-gram length.
  private int[] gram;

  // Is feature-type (gram, dep, cdep) and factor-type (lex, ner, lem) combination active?
  private boolean[][] active;

  // TODO: should this be input type?
  // Should feature-type (gram, dep, cdep) be processed?
  private boolean[] use;

  public FeatureSet() {
    context = new int[Factor.values().length];
    gram = new int[Factor.values().length];
    active = new boolean[Type.values().length][Factor.values().length];
    use = new boolean[Type.values().length];
  }

  public FeatureSet(Configuration conf, String group) {
    this();

    for (Factor f : Factor.values())
      context[f.code] = conf.getInt("thrax." + group + ".max-" + f.name + "-context", 0);

    for (Factor f : Factor.values())
      gram[f.code] = conf.getInt("thrax." + group + ".max-" + f.name + "-gram", 0);

    for (Type t : Type.values()) {
      for (Factor f : Factor.values()) {
        if (f == Factor.NONE)
          active[t.code][f.code] = conf.getBoolean("thrax." + group + ".use-" + t.name, false);
        else
          active[t.code][f.code] =
              conf.getBoolean("thrax." + group + ".use-" + f.name + "-" + t.name, false);
        use[t.code] = use[t.code] || active[t.code][f.code];
      }
    }
  }

  public void includeFeatureSet(FeatureSet set) {
    for (int i = 0; i < context.length; ++i)
      context[i] = Math.max(context[i], set.context[i]);

    for (int i = 0; i < gram.length; ++i)
      gram[i] = Math.max(gram[i], set.gram[i]);

    for (int i = 0; i < active.length; ++i)
      for (int j = 0; j < active[i].length; ++j)
        active[i][j] = active[i][j] || set.active[i][j];

    for (int i = 0; i < use.length; ++i)
      use[i] = use[i] || set.use[i];
  }

  public boolean active(Type type, Factor factor) {
    return active[type.code][factor.code];
  }

  public boolean[] factors(Type type) {
    return active[type.code];
  }
  
  public int context(Factor factor) {
    return context[factor.code];
  }

  public int gram(Factor factor) {
    return gram[factor.code];
  }
  
  public boolean use(Type type) {
    return use[type.code];
  }
}
