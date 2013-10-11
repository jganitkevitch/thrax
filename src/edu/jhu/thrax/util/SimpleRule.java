package edu.jhu.thrax.util;

import java.util.HashMap;
import java.util.Map;

public class SimpleRule {
  
  private String line;

  private String source;
  private String target;
  private String lhs;
  private HashMap<String, Float> features;

  private String rule = null;
  private String head = null;
  private double cost = Float.NEGATIVE_INFINITY;

  public SimpleRule(String l) {
    line = l.trim();
    
    String[] fields = FormatUtils.P_DELIM.split(line);
    lhs = fields[0];
    source = fields[1];
    target = fields[2];
    
    
    String[] feature_entries = FormatUtils.P_SPACE.split(fields[3]);
    features = new HashMap<String, Float>();
    for (String f : feature_entries) {
      String[] parts = FormatUtils.P_EQUAL.split(f);
      features.put(parts[0], Float.parseFloat(parts[1]));
    }
  }

  public SimpleRule(String l, Map<String, Double> weights) {
    this(l);
    score(weights);
  }

  public double score(Map<String, Double> weights) {
    if (cost == Double.NEGATIVE_INFINITY) {
      double score = 0;
      for (String name : features.keySet())
        if (weights.containsKey(name)) {
          // TODO: verify non-negativity.
          score += weights.get(name) * features.get(name);
        }
      cost = score;
    }
    return cost;
  }

  public String head() {
    if (head == null) head = lhs + FormatUtils.DELIM + source;
    return head;
  }
  
  public String source() {
    return source;
  }

  public String target() {
    return target;
  }

  public String lhs() {
    return lhs;
  }

  public HashMap<String, Float> features() {
    return features;
  }

  public String getRule() {
    if (rule == null) rule = lhs + FormatUtils.DELIM + source + FormatUtils.DELIM + target;
    return rule;
  }
  
  public String toString() {
    return line;
  }
}
