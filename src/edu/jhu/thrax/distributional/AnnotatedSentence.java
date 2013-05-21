package edu.jhu.thrax.distributional;

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.lang3.StringEscapeUtils;

import edu.jhu.thrax.distributional.FeatureTypes.Directionality;
import edu.jhu.thrax.distributional.FeatureTypes.Factor;
import edu.jhu.thrax.distributional.FeatureTypes.Type;
import edu.jhu.thrax.syntax.LatticeArray;
import edu.jhu.thrax.util.FormatUtils;
import edu.jhu.thrax.util.Utils;
import edu.jhu.thrax.util.Vocabulary;
import edu.jhu.thrax.util.exceptions.MalformedInputException;
import edu.jhu.thrax.util.exceptions.NotEnoughFieldsException;

public class AnnotatedSentence {

  public final int length;

  private LatticeArray parse;
  private String[] lemma;
  private String[] ner;

  private DependencyStructure dep;
  private DependencyStructure cdep;
  private DependencyStructure cpdep;

  // String[factor][position][n]
  private String[][][] ngrams;

  // Format is as follows:
  // parse ||| lemma ||| NER ||| basic deps ||| collapsed deps ||| colpp deps
  public AnnotatedSentence(String input, FeatureSet fs) throws MalformedInputException {
    try {
      input = StringEscapeUtils.unescapeXml(input);

      String[] inputs = FormatUtils.P_DELIM.split(input);
      if (inputs.length < 6) throw new NotEnoughFieldsException();

      parse = new LatticeArray(inputs[0].trim(), true);
      lemma = FormatUtils.P_SPACE.split(inputs[1].trim().toLowerCase());

      length = lemma.length;
      if (length != parse.size()) throw new MalformedInputException();

      String[] ner_entries = FormatUtils.P_SPACE.split(inputs[2].trim().toLowerCase());
      ner = new String[ner_entries.length];
      if (ner.length != length)
        throw new MalformedInputException("NER: " + ner.length + " vs. Size: " + length);
      for (int i = 0; i < ner_entries.length; ++i)
        ner[i] = FormatUtils.P_SLASH.split(ner_entries[i])[1];

      generateAllGramFeatures(fs);

      if (fs.use(Type.DEP))
        dep = new DependencyStructure(inputs[3], this, Type.DEP, fs.factors(Type.DEP));
      if (fs.use(Type.CDEP))
        cdep = new DependencyStructure(inputs[4], this, Type.CDEP, fs.factors(Type.CDEP));
      if (fs.use(Type.CPDEP))
        cpdep = new DependencyStructure(inputs[5], this, Type.CPDEP, fs.factors(Type.CPDEP));

    } catch (Exception e) {
      throw new MalformedInputException();
    }
  }

  private void generateAllGramFeatures(FeatureSet fs) {
    if (fs.use(Type.NGRAM)) {
      ngrams = new String[Factor.values().length][][];
      for (Factor f : Factor.values()) {
        ngrams[f.code] = buildGramFeatures(this.get(f), fs.gram(f));
      }
    }
  }

  private String[][] buildGramFeatures(String[] sentence, int N) {
    if (N == 0) return new String[0][0];

    String[][] cache = new String[length][];
    for (int i = 0; i <= length - N; i++)
      cache[i] = new String[N];
    for (int i = 1; i < N; i++)
      cache[length - N + i] = new String[N - i];

    StringBuilder sb = new StringBuilder();
    for (int cf = 0; cf < length; cf++) {
      sb.delete(0, sb.length());
      for (int l = 0; l < Math.min(N, length - cf); l++) {
        sb.append("_" + sentence[cf + l]);
        cache[cf][l] = sb.toString();
      }
    }
    return cache;
  }

  public Map<String, Integer> getFeatures(FeatureSet fs, int from, int to) {
    Map<String, Integer> features = new HashMap<String, Integer>();

    addNgramFeatures(features, from, to, fs);
    addDependencyFeatures(features, from, to, fs);

    if (fs.use(Type.SYN)) addSyntaxFeatures(features, from, to);

    return features;
  }

  private void addNgramFeatures(Map<String, Integer> features, int from, int to, FeatureSet fs) {
    for (Factor f : Factor.values()) {
      String left_prefix = Directionality.LEFT.name + f.name;
      for (int cf = Math.max(0, from - fs.context(f)); cf < from; cf++)
        for (int l = 0; l < Math.min(fs.gram(f), from - cf); l++)
          Utils.increment(features, left_prefix + ngrams[f.code][cf][l] + (from - cf));

      String right_prefix = Directionality.RIGHT.name + f.name;
      final int right_boundary = Math.min(length, to + fs.context(f));
      for (int cf = to; cf < right_boundary; cf++)
        for (int l = 0; l < Math.min(fs.gram(f), right_boundary - cf); l++)
          Utils.increment(features, right_prefix + ngrams[f.code][cf][l] + (cf - to + 1));
    }
  }

  private void addDependencyFeatures(Map<String, Integer> features, int from, int to, FeatureSet fs) {
    if (fs.use(Type.DEP)) dep.addFeatures(features, this, from, to, fs.factors(Type.DEP));
    if (fs.use(Type.CDEP)) cdep.addFeatures(features, this, from, to, fs.factors(Type.CDEP));
    if (fs.use(Type.CPDEP)) cpdep.addFeatures(features, this, from, to, fs.factors(Type.CPDEP));
  }

  private void addSyntaxFeatures(Map<String, Integer> features, int from, int to) {
    Collection<Integer> constituents = parse.getConstituentLabels(from, to);
    for (int c : constituents)
      Utils.increment(features,
          Directionality.CENTER.name + Type.SYN.name + "span" + Vocabulary.word(c));

    Collection<Integer> ccg = parse.getCcgLabels(from, to);
    for (int c : ccg) {
      String label = Vocabulary.word(c);
      if (label.contains("/")) {
        String[] parts = FormatUtils.P_SLASH.split(label);
        Utils.increment(features, Directionality.RIGHT.name + Type.SYN.name + "pref" + parts[0]);
        Utils.increment(features, Directionality.RIGHT.name + Type.SYN.name + "miss" + parts[1]);
      } else {
        String[] parts = FormatUtils.P_BSLASH.split(label);
        Utils.increment(features, Directionality.LEFT.name + Type.SYN.name + "suff" + parts[0]);
        Utils.increment(features, Directionality.LEFT.name + Type.SYN.name + "miss" + parts[1]);
      }
    }
  }

  public String getPhrase(int from, int to) {
    return parse.getTerminalPhrase(from, to);
  }

  public String[] get(Factor f) {
    switch (f) {
      case LEX:
        return parse.getTerminals();
      case LEM:
        return lemma;
      case POS:
        return parse.getPOS();
      case NER:
        return ner;
      default:
        return null;
    }
  }

  public String get(Factor f, int index) {
    switch (f) {
      case LEX:
        return parse.getTerminal(index);
      case LEM:
        return lemma[index];
      case POS:
        return parse.getPOS(index);
      case NER:
        return ner[index];
      default:
        return "[null]";
    }
  }

}
