package edu.jhu.thrax.distributional;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

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

  private Map<String, List<ContextCount>> features;

  private ContextWritable[] contexts;

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

    int L = sentence.length;
    int N = MAX_PHRASE_LENGTH;
    int K = groups.size();

    features = new HashMap<String, List<ContextCount>>();
    contexts = new ContextWritable[L * N * K];
    for (int k = 0; k < contexts.length; ++k)
      contexts[k] = new ContextWritable(1, new float[slsh.numBits]);

    for (int i = 0; i < sentence.length; ++i) {
      for (int j = i + 1; j <= Math.min(i + MAX_PHRASE_LENGTH, sentence.length); ++j) {
        for (int k = 0; k < groups.size(); ++k) {
          Map<String, Integer> map = sentence.getFeatures(groups.get(k), i, j);
          for (Map.Entry<String, Integer> e : map.entrySet()) {
            int c = (i * N + j - i - 1) * K + k;
            if (!features.containsKey(e.getKey()))
              features.put(e.getKey(), new ArrayList<ContextCount>());
            features.get(e.getKey()).add(new ContextCount(c, e.getValue()));
          }
        }
      }
    }
    for (Map.Entry<String, List<ContextCount>> e : features.entrySet()) {
      float[] feature_sums = new float[slsh.numBits];
      slsh.hashToSums(feature_sums, e.getKey());
      for (ContextCount cc : e.getValue()) {
        for (int i = 0; i < feature_sums.length; ++i)
          contexts[cc.context].sums[i] += cc.count * feature_sums[i];
      }
    }

    for (int i = 0; i < sentence.length; ++i) {
      for (int j = i + 1; j <= Math.min(i + MAX_PHRASE_LENGTH, sentence.length); ++j) {
        int c = (i * N + j - i - 1) * K;
        context.write(new Text(sentence.getPhrase(i, j)), new ContextGroups(contexts, K, c));
      }
    }
  }

  class ContextCount {
    final int context;
    final int count;

    ContextCount(int context, int count) {
      this.context = context;
      this.count = count;
    }
  }


  // public static void main(String[] argv) {
  // String input =
  // "(ROOT (S (ADVP (RB Surely)) (NP (PRP it)) (VP (VBZ is) (ADJP (JJ possible)) (S (VP (TO to) (VP (VB condemn) (NP (NP (DT the) (JJ grim) (NN state)) (PP (IN of) (NP (DT the) (NN occupation)))) (PP (IN without) (S (VP (VBG denying) (NP (NP (NP (DT the) (NN extirpation)) (PP (IN of) (NP (NP (DT the) (JJ ancient) (JJ Jewish) (NN community)) (PP (IN of) (NP (NNP Hebron))) (PP (IN in) (NP (CD 1929)))))) (, ,) (NP (NP (DT the) (JJ Palestinian) (NNS irregulars)) (SBAR (WHNP (WP who)) (S (VP (VBN murdered) (NP (NP (NNS hundreds)) (PP (IN of) (NP (NNPS Jews)))) (PP (IN during) (NP (NP (DT the) (JJ Arab) (NN revolt)) (PP (IN of) (NP (CD 1936-39))))))))) (, ,) (NP (NP (DT the) (JJ Palestinian) (NNS fighters)) (SBAR (WHNP (WP who)) (S (VP (VBD took) (NP (NN part)) (PP (IN in) (NP (NP (DT the) (CD 1948) (NN war)) (SBAR (WHNP (WDT that)) (S (VP (VBD was) (NP (NP (DT the) (JJ Arab) (NN response)) (PP (TO to) (NP (NN partition))))))))))))) (CC and) (RB then) (NP (NP (JJ Israeli) (NN statehood)) (, ,) (CC and) (NP (NP (DT the) (NN terror) (NNS attacks)) (PP (IN of) (NP (DT the) (NNS 1970s))) (VP (VBN perpetrated) (PP (IN by) (NP (NP (DT the) (NNP Palestine) (NNP Liberation) (NNP Organization)) (, ,) (SBAR (WHNP (WDT which)) (S (VP (AUXD was) (VP (VBN created) (PP (IN in) (NP (CD 1964))))))))))))))))))) (, ,) (SBAR (NP (CD three) (NNS years)) (IN before) (S (NP (EX there)) (VP (VBD was) (NP (NP (NN anything)) (SBAR (S (VP (TO to) (VP (`` ``) (VB liberate) ('' '') (PP (IN besides) (NP (NP (NNP Israel)) (NP (PRP itself))))))))))))) (. .))) ||| surely it be possible to condemn the grim state of the occupation without deny the extirpation of the ancient jewish community of Hebron in 1929 , the palestinian irregular who murder hundred of Jews during the arab revolt of 1936-39 , the palestinian fighter who take part in the 1948 war that be the arab response to partition and then israeli statehood , and the terror attack of the 1970 perpetrate by the Palestine Liberation Organization , which was create in 1964 , three year before there be anything to `` liberate '' besides Israel itself . ||| Surely/O it/O is/O possible/O to/O condemn/O the/O grim/O state/O of/O the/O occupation/O without/O denying/O the/O extirpation/O of/O the/O ancient/O Jewish/MISC community/O of/O Hebron/LOCATION in/O 1929/DATE ,/O the/O Palestinian/MISC irregulars/O who/O murdered/O hundreds/O of/O Jews/MISC during/O the/O Arab/MISC revolt/O of/O 1936-39/NUMBER ,/O the/O Palestinian/MISC fighters/O who/O took/O part/O in/O the/O 1948/DATE war/O that/O was/O the/O Arab/PERSON response/O to/O partition/O and/O then/O Israeli/LOCATION statehood/O ,/O and/O the/O terror/O attacks/O of/O the/O 1970s/DATE perpetrated/O by/O the/O Palestine/ORGANIZATION Liberation/ORGANIZATION Organization/ORGANIZATION ,/O which/O was/O created/O in/O 1964/DATE ,/O three/NUMBER years/NUMBER before/O there/O was/O anything/O to/O ``/O liberate/O ''/O besides/O Israel/LOCATION itself/O ./O ||| advmod-4-1 nsubj-4-2 cop-4-3 root-0-4 aux-6-5 xcomp-4-6 det-9-7 amod-9-8 dobj-6-9 prep-9-10 det-12-11 pobj-10-12 prep-6-13 pcomp-13-14 det-16-15 dobj-14-16 prep-16-17 det-21-18 amod-21-19 amod-21-20 pobj-17-21 prep-21-22 pobj-22-23 prep-21-24 pobj-24-25 det-29-27 amod-29-28 conj-16-29 nsubj-31-30 rcmod-29-31 dobj-31-32 prep-32-33 pobj-33-34 prep-31-35 det-38-36 amod-38-37 pobj-35-38 prep-38-39 pobj-39-40 det-44-42 amod-44-43 conj-16-44 nsubj-46-45 rcmod-44-46 dobj-46-47 prep-46-48 det-51-49 num-51-50 pobj-48-51 nsubj-56-52 cop-56-53 det-56-54 amod-56-55 rcmod-51-56 prep-56-57 pobj-57-58 cc-16-59 advmod-16-60 amod-62-61 dep-16-62 cc-62-64 det-67-65 nn-67-66 conj-62-67 prep-67-68 det-70-69 pobj-68-70 partmod-67-71 prep-71-72 det-76-73 nn-76-74 nn-76-75 pobj-72-76 nsubjpass-80-78 auxpass-80-79 rcmod-76-80 prep-80-81 pobj-81-82 num-85-84 dep-88-85 dep-88-86 expl-88-87 ccomp-4-88 nsubj-88-89 aux-92-90 infmod-89-92 prep-92-94 pobj-94-95 dep-95-96 ||| advmod-4-1 xsubj-6-2 cop-4-3 root-0-4 aux-6-5 xcomp-4-6 det-9-7 amod-9-8 dobj-6-9 det-12-11 prep_of-9-12 prepc_without-6-14 det-16-15 dobj-14-16 det-21-18 amod-21-19 amod-21-20 prep_of-16-21 prep_of-21-23 prep_in-21-25 det-29-27 amod-29-28 nsubj-31-29 rcmod-29-31 dobj-31-32 prep_of-32-34 det-38-36 amod-38-37 prep_during-31-38 prep_of-38-40 det-44-42 amod-44-43 nsubj-46-44 rcmod-44-46 dobj-46-47 det-51-49 num-51-50 nsubj-56-51 cop-56-53 det-56-54 amod-56-55 rcmod-51-56 prep_to-56-58 conj_and-16-60 amod-62-61 dep-16-62 det-67-65 nn-67-66 conj_and-62-67 det-70-69 prep_of-67-70 partmod-67-71 det-76-73 nn-76-74 nn-76-75 nsubjpass-80-76 auxpass-80-79 rcmod-76-80 prep_in-80-82 num-85-84 dep-88-85 dep-88-86 expl-88-87 ccomp-4-88 nsubj-88-89 aux-92-90 infmod-89-92 prep_besides-92-95 dep-95-96 ||| advmod-4-1 xsubj-6-2 cop-4-3 root-0-4 aux-6-5 xcomp-4-6 det-9-7 amod-9-8 dobj-6-9 det-12-11 prep_of-9-12 prepc_without-6-14 det-16-15 dobj-14-16 det-21-18 amod-21-19 amod-21-20 prep_of-16-21 prep_of-21-23 prep_in-21-25 det-29-27 amod-29-28 nsubj-31-29 rcmod-29-31 dobj-31-32 prep_of-32-34 det-38-36 amod-38-37 prep_during-31-38 prep_of-38-40 det-44-42 amod-44-43 nsubj-46-44 rcmod-44-46 dobj-46-47 det-51-49 num-51-50 nsubj-56-51 cop-56-53 det-56-54 amod-56-55 rcmod-51-56 prep_to-56-58 conj_and-16-60 amod-62-61 dep-16-62 det-67-65 nn-67-66 conj_and-62-67 det-70-69 prep_of-67-70 partmod-67-71 det-76-73 nn-76-74 nn-76-75 nsubjpass-80-76 auxpass-80-79 rcmod-76-80 prep_in-80-82 num-85-84 dep-88-85 dep-88-86 expl-88-87 ccomp-4-88 nsubj-88-89 aux-92-90 infmod-89-92 prep_besides-92-95 dep-95-96";
  //
  // Configuration conf = new Configuration();
  // Map<String, String> options = ConfFileParser.parse(argv[0]);
  // for (String opt : options.keySet())
  // conf.set("thrax." + opt, options.get(opt));
  //
  // try {
  // long start = System.currentTimeMillis();
  // for (int l = 0; l < 200; ++l) {
  // List<PhraseContext> phrases = new ArrayList<PhraseContext>();
  // PhraseContextExtractor pce = new PhraseContextExtractor(conf);
  // for (PhraseContext pc : pce.extract(input)) {
  // phrases.add(pc);
  // }
  // }
  // long end = System.currentTimeMillis();
  // System.err.println("Time: " + (end - start));
  //
  // } catch (Exception e) {
  // e.printStackTrace();
  // }
  // }

}
