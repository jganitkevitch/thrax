package edu.jhu.thrax.input;

import java.util.Map;

import edu.jhu.thrax.syntax.DependencyTree;
import edu.jhu.thrax.syntax.ParseTree;
import edu.jhu.thrax.util.exceptions.MalformedInputException;

public class ThraxInput {
  
  public int[] source = null;
  public int[] target = null;
  
  public ParseTree src_parse = null;
  public ParseTree tgt_parse = null;
  
  public Map<String, DependencyTree> src_deps = null;
  public Map<String, DependencyTree> tgt_deps = null;
  
  public Map<String, int[]> src_tags = null;
  public Map<String, int[]> tgt_tags = null;
  
  public Alignment alignment = null;
  public float[] weights = null;
  
  public int[] labels = null;
  
  public boolean sanityCheck() throws MalformedInputException {
    if (source.length == 0 || target.length == 0)
      throw new MalformedInputException("empty sentence");
    if (!alignment.consistentWith(source.length, target.length))
      throw new MalformedInputException("inconsistent alignment");
    
    return true;
  }

}
