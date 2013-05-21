package edu.jhu.thrax.distributional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

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

  public void extract(String input, Mapper<LongWritable, Text, Text, ContextGroups>.Context context)
      throws MalformedInputException, IOException, InterruptedException {
    AnnotatedSentence sentence = new AnnotatedSentence(input, union);
    for (int i = 0; i < sentence.length; i++) {
      for (int j = i + 1; j <= Math.min(i + MAX_PHRASE_LENGTH, sentence.length); j++) {
        ContextWritable[] contexts = new ContextWritable[groups.size()];
        for (int k = 0; k < groups.size(); ++k)
          contexts[k] = new ContextWritable(sentence.getFeatures(groups.get(k), i, j), slsh);
        context.write(new Text(sentence.getPhrase(i, j)), new ContextGroups(contexts));
      }
    }
  }

  /*
   * public static void main(String[] argv) { String input =
   * "(ROOT (S (NP (PRP We)) (VP (AUXP 're) (VP (IN about) (S (VP (TO to) (VP " +
   * "(VB enter) (NP (NP (PRP$ our) (JJ 19th) (JJ consecutive) (NN year)) (PP (IN of) " +
   * "(NP (NNP Truman-envy))))))))) (. .))) ||| we 're about to enter we 19th " +
   * "consecutive year of Truman-envy . ||| We/O 're/O about/O to/O enter/O our/O " +
   * "19th/DATE consecutive/O year/O of/O Truman-envy/O ./O ||| nsubj-3-1 dep-3-2 " +
   * "root-0-3 aux-5-4 xcomp-3-5 poss-9-6 amod-9-7 amod-9-8 dobj-5-9 prep-9-10 " +
   * "pobj-10-11 ||| xsubj-5-1 dep-3-2 root-0-3 aux-5-4 xcomp-3-5 poss-9-6 amod-9-7 " +
   * "amod-9-8 dobj-5-9 prep_of-9-11 ||| xsubj-5-1 dep-3-2 root-0-3 aux-5-4 " +
   * "xcomp-3-5 poss-9-6 amod-9-7 amod-9-8 dobj-5-9 prep_of-9-11";
   * 
   * Configuration conf = new Configuration(); Map<String, String> options =
   * ConfFileParser.parse(argv[0]); for (String opt : options.keySet()) conf.set("thrax." + opt,
   * options.get(opt));
   * 
   * try { long start = System.currentTimeMillis(); for (int l = 0; l < 100; ++l) {
   * List<PhraseContext> phrases = new ArrayList<PhraseContext>(); PhraseContextExtractor pce = new
   * PhraseContextExtractor(conf); for (PhraseContext pc : pce.extract(input)) { phrases.add(pc); }
   * } long end = System.currentTimeMillis(); System.err.println("Time: " + (end - start));
   * 
   * } catch (Exception e) { e.printStackTrace(); } }
   */
}
