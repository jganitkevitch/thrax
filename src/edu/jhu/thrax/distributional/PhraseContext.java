package edu.jhu.thrax.distributional;

import org.apache.hadoop.io.Text;

import edu.jhu.thrax.hadoop.distributional.ContextGroups;

public class PhraseContext {

  private final String phrase;

  private ContextGroups contexts; 

  public PhraseContext(String p, ContextGroups c) {
    phrase = p;
    contexts = c;
  }
  
  public Text getPhrase() {
    return new Text(phrase);
  }
  
  public ContextGroups getContexts() {
    return contexts;
  }
}
