package edu.jhu.thrax.tools;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Logger;

import edu.jhu.jerboa.sim.SLSH;
import edu.jhu.thrax.util.FormatUtils;
import edu.jhu.thrax.util.io.LineReader;

public class SimilarityAnnotator {
  private static Logger logger = Logger.getLogger(SimilarityAnnotator.class.getName());

  private static final int MAX_LENGTH = 4;

  public static boolean labeled = false;
  public static boolean sparse = false;

  private LineReader grammarReader;

  private SLSH slsh;

  private String similarityLabel = "Similarity";

  private String glue = null;

  public SimilarityAnnotator(String grammar_file) throws Exception {
    grammarReader = new LineReader(grammar_file);
    slsh = SLSH.load();
  }

  public void setSimilarityLabel(String label) {
    this.similarityLabel = label;
  }

  @SuppressWarnings("unchecked")
  private void annotate() throws IOException {
    String[] src;
    String[] tgt;
    ArrayList<Integer>[] src_alignment;
    ArrayList<Integer>[] tgt_alignment;
    long counter = 0;

    glue = labeled ? " " + similarityLabel + "=" : " ";

    while (grammarReader.hasNext()) {
      counter++;
      int sim_count = 0;
      double sim_score = 0.0;

      String gline = grammarReader.readLine();
      String[] gfields = FormatUtils.P_DELIM.split(gline.trim());
      src = gfields[1].split(" ");
      tgt = gfields[2].split(" ");

      try {
        String aline = gfields[4];
        
        String[] apoints = aline.split(" ");

        src_alignment = new ArrayList[src.length];
        tgt_alignment = new ArrayList[tgt.length];
        for (int i = 0; i < src_alignment.length; i++)
          src_alignment[i] = new ArrayList<Integer>();
        for (int i = 0; i < tgt_alignment.length; i++)
          tgt_alignment[i] = new ArrayList<Integer>();

        boolean alignment_broken = false;
        for (String apoint : apoints) {
          String[] acoords = apoint.split("-");
          int src_coord = Integer.parseInt(acoords[0]);
          int tgt_coord = Integer.parseInt(acoords[1]);

          // Make sure the alignment coordinates are okay.
          if (src_coord < 0 || src_coord >= src.length || tgt_coord < 0 || tgt_coord >= tgt.length) {
            logger.warning("Skipping alignment overrun in line " + counter + ":\n" + "Grammar: "
                + gline + "\n" + "Aligner: " + aline);
            alignment_broken = true;
            break;
          }
          src_alignment[src_coord].add(tgt_coord);
          tgt_alignment[tgt_coord].add(src_coord);
        }
        if (alignment_broken) continue;

        List<PhrasePair> phrase_pairs = generatePhrasePairs(src, tgt, src_alignment, tgt_alignment);

        for (PhrasePair phrase_pair : phrase_pairs) {
          double sim = getSimilarity(phrase_pair);
          if (sim != -1) {
            sim_score += sim;
            sim_count++;
          }
        }
        if (sim_count != 0) sim_score /= sim_count;

        if (!sparse || sim_score > 0)
          System.out.println(gfields[0] + " ||| " + gfields[1] + " ||| " + gfields[2] + " ||| "
              + gfields[3] + glue + String.format("%.5f", sim_score) + " ||| " + gfields[4]);
        else
          System.out.println(gline);
      } catch (Exception e) {
        e.printStackTrace();
        System.err.println("Line " + counter + ": " + gline);
      }
    }
  }

  private List<PhrasePair> generatePhrasePairs(String[] src, String[] tgt,
      ArrayList<Integer>[] src_alignment, ArrayList<Integer>[] tgt_alignment) {
    // Maximum phrase length we extract from the rule.
    int max_length = Math.min(MAX_LENGTH, src.length);
    List<PhrasePair> phrase_pairs = new ArrayList<PhrasePair>();

    // Source and target from and to indices. Indices are inclusive.
    int sf, st, tf, tt;
    for (sf = 0; sf < src.length; sf++) {
      if (FormatUtils.isNonterminal(src[sf]) || src_alignment[sf].isEmpty()) continue;
      StringBuilder sp = new StringBuilder();
      tf = Integer.MAX_VALUE;
      tt = -1;
      // Extending source-side phrase.
      for (st = sf; st < Math.min(sf + max_length, src.length); st++) {
        // Next word is NT: stop phrase extraction here.
        if (FormatUtils.isNonterminal(src[st])) break;
        // Add source word to source phrase.
        if (sp.length() != 0) sp.append(" ");
        sp.append(src[st]);

        // Expand the target-side phrase.
        tf = expandMin(tf, src_alignment[st]);
        tt = expandMax(tt, src_alignment[st]);

        // Compute back-projection of target-side phrase.
        int spf = Integer.MAX_VALUE;
        int spt = -1;
        for (int t = tf; t <= tt; t++) {
          spf = expandMin(spf, tgt_alignment[t]);
          spt = expandMax(spt, tgt_alignment[t]);
        }
        // Projecting target-side phrase back onto source doesn't match up
        // with seed source phrase.
        if (spf < sf || spt > st) continue;
        // Build target side phrase string.
        StringBuilder tp = new StringBuilder();
        for (int t = tf; t < tt; t++)
          tp.append(tgt[t]).append(" ");
        tp.append(tgt[tt]);

        // Add phrase pair to list.
        phrase_pairs.add(new PhrasePair(sp.toString(), tp.toString()));
      }
    }
    return phrase_pairs;
  }

  private static int expandMin(int index, ArrayList<Integer> aligned) {
    for (int a : aligned)
      index = Math.min(index, a);
    return index;
  }

  private static int expandMax(int index, ArrayList<Integer> aligned) {
    for (int a : aligned)
      index = Math.max(index, a);
    return index;
  }

  // TODO: Weigh or threshold by strength.
  private double getSimilarity(PhrasePair phrase_pair) {
    double score;
    if (phrase_pair.isIdentity())
      score = 1.0;
    else
      score = slsh.score(phrase_pair.src, phrase_pair.tgt);
    return score;
  }

  private void cleanup() throws IOException {
    grammarReader.close();
  }

  public static void usage() {
    System.err.println("Usage: java joshua.tools.SimilarityAnnotator "
        + "-g <grammar file> -a <alignment file> -c <server:port> [-l -s]");
    System.exit(0);
  }

  public static void main(String[] args) throws Exception {
    labeled = false;
    sparse = false;

    String grammar_file = null;
    String similarity_label = null;

    for (int i = 0; i < args.length; i++) {
      if ("-g".equals(args[i]) && (i < args.length - 1)) {
        grammar_file = args[++i];
      } else if ("-f".equals(args[i]) && (i < args.length - 1)) {
        similarity_label = args[++i];
      } else if ("-l".equals(args[i])) {
        labeled = true;
      } else if ("-s".equals(args[i])) {
        sparse = true;
      }
    }

    if (grammar_file == null) {
      logger.severe("No grammar specified.");
      return;
    }
    if (!labeled && sparse) {
      logger.severe("I cannot condone grammars that are both sparse and unlabeled.");
      return;
    }
    if (args.length < 2) usage();

    SimilarityAnnotator annotator = new SimilarityAnnotator(grammar_file);
    if (similarity_label != null) {
      logger.info("Setting label: " + similarity_label);
      annotator.setSimilarityLabel(similarity_label);
    }
    annotator.annotate();
    annotator.cleanup();
  }

  class PhrasePair {
    private String src;
    private String tgt;

    public PhrasePair(String src, String tgt) {
      this.src = src;
      this.tgt = tgt;
    }

    public String toString() {
      return src + "\t" + tgt;
    }

    public boolean isIdentity() {
      return (src.equals(tgt));
    }
  }
}
