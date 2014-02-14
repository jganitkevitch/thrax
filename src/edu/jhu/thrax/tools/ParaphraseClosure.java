package edu.jhu.thrax.tools;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.logging.Logger;

import edu.jhu.jerboa.util.FileManager;
import edu.jhu.thrax.util.FormatUtils;
import edu.jhu.thrax.util.Vocabulary;
import edu.jhu.thrax.util.io.LineReader;

public class ParaphraseClosure {

  private static final Logger logger = Logger.getLogger(ParaphraseClosure.class.getName());

  private static float max_cost = 50;

  private static HashMap<Integer, HashMap<Integer, Float>> pp =
      new HashMap<Integer, HashMap<Integer, Float>>();

  public static long pack(int x, int y) {
    long xPacked = ((long) x) << 32;
    long yPacked = y & 0xFFFFFFFFL;
    return xPacked | yPacked;
  }

  public static int unpackX(long packed) {
    return (int) (packed >> 32);
  }

  public static int unpackY(long packed) {
    return (int) (packed & 0xFFFFFFFFL);
  }

  private static float search(int current, int goal, float cost) {
    if (pp.get(current).containsKey(goal)) {
      float step = pp.get(current).get(goal);
      if (step + cost > max_cost)
        return -1;
      else
        return step + cost;
    } else {
      for (int pivot : pp.get(current).keySet()) {
        float step = Math.max(pp.get(current).get(pivot), 5.0f);
        if (step + cost < max_cost) return search(pivot, goal, step + cost);
      }
      return -1;
    }
  }


  public static void main(String[] args) {

    String grammar_file = null;
    String reference_file = null;
    String weight_file = null;
    String output_file = null;

    for (int i = 0; i < args.length; i++) {
      if ("-g".equals(args[i]) && (i < args.length - 1)) {
        grammar_file = args[++i];
      } else if ("-r".equals(args[i]) && (i < args.length - 1)) {
        reference_file = args[++i];
      } else if ("-w".equals(args[i]) && (i < args.length - 1)) {
        weight_file = args[++i];
      } else if ("-o".equals(args[i]) && (i < args.length - 1)) {
        output_file = args[++i];
      } else if ("-m".equals(args[i]) && (i < args.length - 1)) {
        max_cost = Float.parseFloat(args[++i]);
      }
    }

    if (grammar_file == null) {
      logger.severe("No grammar specified.");
      return;
    }
    if (reference_file == null) {
      logger.severe("No reference file specified.");
      return;
    }
    if (weight_file == null) {
      logger.severe("No weight file specified.");
      return;
    }
    if (output_file == null) {
      logger.severe("No output file specified.");
      return;
    }

    HashSet<Long> reference = new HashSet<Long>();
    HashSet<Integer> ref_vocab = new HashSet<Integer>();
    HashMap<String, Float> weights = new HashMap<String, Float>();

    try {
      LineReader reference_reader = new LineReader(reference_file);
      while (reference_reader.hasNext()) {
        String line = reference_reader.next().trim();
        String[] fields = FormatUtils.P_TAB.split(line);
        int x = Vocabulary.id(fields[0]);
        int y = Vocabulary.id(fields[1]);
        ref_vocab.add(x);
        ref_vocab.add(y);
        reference.add(pack(x, y));
        reference.add(pack(y, x));
      }
      reference_reader.close();

      LineReader weights_reader = new LineReader(weight_file);
      while (weights_reader.hasNext()) {
        String line = weights_reader.next().trim();
        if (line.isEmpty()) continue;
        String[] fields = FormatUtils.P_SPACE.split(line);
        weights.put(fields[0], Float.parseFloat(fields[1]));
      }
      weights_reader.close();

      List<Paraphrase> paraphrases = new ArrayList<Paraphrase>();

      LineReader reader = new LineReader(grammar_file);
      System.err.print("[");
      int rule_count = 0;
      while (reader.hasNext()) {
        String rule_line = reader.next().trim();

        String[] fields = FormatUtils.P_DELIM.split(rule_line);

        if (fields[1].equals(fields[2]) || fields[1].contains(",1]"))
          continue;
        
        float score = 0;
        String[] features = FormatUtils.P_SPACE.split(fields[3]);
        for (String f : features) {
          String[] parts = FormatUtils.P_EQUAL.split(f);
          if (weights.containsKey(parts[0]))
            score += weights.get(parts[0]) * Float.parseFloat(parts[1]);
        }
        Paraphrase p = new Paraphrase(fields[1], fields[2], score);

        if (++rule_count % 100000 == 0) System.err.print("-");

        if (p.score < 0) p.score *= -1;

        if (p.score < max_cost) paraphrases.add(p);
      }
      System.err.println("]");
      reader.close();

      for (Paraphrase sp : paraphrases) {
        if (!pp.containsKey(sp.key)) pp.put(sp.key, new HashMap<Integer, Float>());
        HashMap<Integer, Float> map = pp.get(sp.key);
        if (map.containsKey(sp.paraphrase)) {
          if (map.get(sp.paraphrase) > sp.score) map.put(sp.paraphrase, sp.score);
        } else {
          map.put(sp.paraphrase, sp.score);
        }
      }
      // paraphrases.clear();

      // Turning closure off.
      boolean done = true;
      while (!done) {
        done = true;
        int added = 0;
        int updated = 0;
        for (int x : pp.keySet()) {
          HashMap<Integer, Float> add = new HashMap<Integer, Float>();
          HashMap<Integer, Float> xmap = pp.get(x);
          for (int p : xmap.keySet()) {
            float first = xmap.get(p);
            HashMap<Integer, Float> pmap = pp.get(p);
            if (pmap == null) continue;
            for (int y : pmap.keySet()) {
              float cost = first + pmap.get(y);
              if (cost < max_cost) {
                if (add.containsKey(y)) {
                  if (add.get(y) > cost) add.put(y, cost);
                } else {
                  add.put(y, cost);
                }
              }
            }
          }
          for (int y : add.keySet()) {
            float cost = add.get(y);
            if (xmap.containsKey(y)) {
              if (xmap.get(y) > cost) {
                xmap.put(y, cost);
                done = false;
                updated++;
              }
            } else {
              xmap.put(y, cost);
              done = false;
              added++;
            }
          }
        }
        System.err.println("Added " + added + ", updated " + updated);
      }

      // for (int x : pp.keySet()) {
      // if (ref_vocab.contains(x)) {
      // for (int y : pp.get(x).keySet()) {
      // if (ref_vocab.contains(y)) paraphrases.add(new Paraphrase(x, y, pp.get(x).get(y)));
      // }
      // }
      // }

      int added = 0;
      for (long r : reference) {
        int x = unpackX(r);
        int y = unpackY(r);
        if (pp.containsKey(x)) {
          float cost = search(x, y, 0);
          if (cost > 0) {
            paraphrases.add(new Paraphrase(x, y, cost));
            added++;
          }
        }
      }
      System.err.println("Added " + added);

      Collections.sort(paraphrases);

      int count = 0;
      int right = 0;
      int total = reference.size();
      BufferedWriter output = FileManager.getWriter(output_file);
      for (Paraphrase p : paraphrases) {
        boolean print = false;
        if (!ref_vocab.contains(p.key) || !ref_vocab.contains(p.paraphrase))
          continue;
        long c = pack(p.key, p.paraphrase);
        if (reference.contains(c)) {
          right++;
          if (right % 10 == 0) print = true;
        }
        count++;
        // Print state for plotter.
        if (print)
          output.write(total + "\t" + count + "\t" + right + "\t" + ((float) right / count) + "\t"
              + ((float) right / total) + "\t" + p.score + "\n");
      }
      output.close();
    } catch (IOException e) {
      logger.severe(e.getMessage());
    }
  }

  class ScoredParaphrase implements Comparable<ScoredParaphrase> {
    int paraphrase;
    float score;

    public ScoredParaphrase(int p, float s) {
      paraphrase = p;
      score = s;
    }

    @Override
    public int compareTo(ScoredParaphrase that) {
      return Float.compare(this.score, that.score);
    }
  }

  static class Paraphrase implements Comparable<Paraphrase> {
    int key;
    int paraphrase;
    float score;

    public Paraphrase(String k, String p, float s) {
      this(Vocabulary.id(k), Vocabulary.id(p), s);
    }

    public Paraphrase(int k, int p, float s) {
      key = k;
      paraphrase = p;
      score = s;
    }

    @Override
    public int compareTo(Paraphrase that) {
      return Float.compare(this.score, that.score);
    }
  }
}
