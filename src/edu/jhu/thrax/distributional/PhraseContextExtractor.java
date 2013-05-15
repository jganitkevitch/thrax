package edu.jhu.thrax.distributional;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;

import edu.jhu.jerboa.sim.SLSH;
import edu.jhu.thrax.hadoop.distributional.CommonLSH;
import edu.jhu.thrax.hadoop.distributional.ContextGroups;
import edu.jhu.thrax.hadoop.distributional.ContextWritable;
import edu.jhu.thrax.util.exceptions.MalformedInputException;

public class PhraseContextExtractor {

  private final int MAX_PHRASE_LENGTH;
  private SLSH slsh;
  
  private FeatureSet union;
  private List<FeatureSet> groups;
  
  public PhraseContextExtractor(Configuration conf) {
    MAX_PHRASE_LENGTH = conf.getInt("thrax.max-phrase-length", 4);
    
    slsh = CommonLSH.getSLSH(conf);
    
    String[] group_names = conf.getStrings("thrax.contexts", "context");
    union = new FeatureSet();
    groups = new ArrayList<FeatureSet>();
    for (String g : group_names) {
      FeatureSet group = new FeatureSet(conf, g);
      groups.add(group);
      union.includeFeatureSet(group);
    }
  }

  public List<PhraseContext> extract(String input) throws MalformedInputException {
    AnnotatedSentence sentence = new AnnotatedSentence(input, union);
    List<PhraseContext> output = new ArrayList<PhraseContext>();
    for (int i = 0; i < sentence.length; i++) {
      for (int j = i + 1; j <= Math.min(i + MAX_PHRASE_LENGTH, sentence.length); j++) {
        ContextWritable[] contexts = new ContextWritable[groups.size()];
        for (int k=0; k<groups.size(); ++k)
          contexts[k] = new ContextWritable(sentence.getFeatures(groups.get(k), i, j), slsh);
        PhraseContext pc = new PhraseContext(sentence.getPhrase(i, j), new ContextGroups(contexts));
        output.add(pc);
      }
    }
    return output;
  }
}
