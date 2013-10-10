package edu.jhu.thrax.tools;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;
import java.util.logging.Logger;

import edu.jhu.jerboa.util.FileManager;
import edu.jhu.thrax.util.SimpleRule;
import edu.jhu.thrax.util.io.LineReader;

public class IntersectGrammars {

  private static final Logger logger = Logger.getLogger(IntersectGrammars.class.getName());

  public static void main(String[] args) {

    String grammar_one = null;
    String grammar_two = null;
    String output_file = null;

    for (int i = 0; i < args.length; i++) {
      if ("-1".equals(args[i]) && (i < args.length - 1)) {
        grammar_one = args[++i];
      } else if ("-2".equals(args[i]) && (i < args.length - 1)) {
        grammar_two = args[++i];
      } else if ("-o".equals(args[i]) && (i < args.length - 1)) {
        output_file = args[++i];
      }
    }

    if (grammar_one == null) {
      logger.severe("No first grammar specified.");
      return;
    }
    if (grammar_two == null) {
      logger.severe("No second grammar specified.");
      return;
    }
    if (output_file == null) {
      logger.severe("No output file specified.");
      return;
    }

    try {
      LineReader read_one = new LineReader(grammar_one);
      LineReader read_two = new LineReader(grammar_two);
      BufferedWriter write_out = FileManager.getWriter(output_file);

      BufferedWriter write_one =
          FileManager.getWriter("intersect-with-" + grammar_two.replaceAll(".gz", "") + "-"
              + grammar_one);
      BufferedWriter write_two =
          FileManager.getWriter("intersect-with-" + grammar_one.replaceAll(".gz", "") + "-"
              + grammar_two);

      SimpleRule rule_one = new SimpleRule(read_one.next().trim());
      SimpleRule rule_two = new SimpleRule(read_two.next().trim());

      TreeMap<String, SimpleRule> pp_one = new TreeMap<String, SimpleRule>();
      TreeMap<String, SimpleRule> pp_two = new TreeMap<String, SimpleRule>();

      System.err.print("[");
      long count = 0;
      while (read_one.hasNext() && read_two.hasNext()) {
        int cmp = rule_one.head().compareTo(rule_two.head());
        if (cmp < 0) rule_one = new SimpleRule(read_one.readLine());
        if (cmp > 0) rule_two = new SimpleRule(read_two.readLine());
        if (cmp == 0) {
          String head = rule_one.head();
          rule_one = fill(read_one, rule_one, pp_one);
          rule_two = fill(read_two, rule_two, pp_two);

          if (pp_one.size() != 1 || pp_two.size() != 1) {
            double kl_one = getKLDivergence(pp_one, pp_two);
            double kl_two = getKLDivergence(pp_two, pp_one);
            double h_one = getEntropy(pp_one);
            double h_two = getEntropy(pp_two);

            write_out.write(String.format("%s ||| %.3f ||| %.3f ||| %.3f ||| %.3f ||| %.3f\n",
                head, kl_one, kl_two, h_one, h_two, h_one - h_two));

            for (SimpleRule r : pp_one.values())
              write_one.write(r + "\n");
            for (SimpleRule r : pp_two.values())
              write_two.write(r + "\n");
          }
        }
        if (++count % 250000 == 0) System.err.print("-");
      }
      System.err.println("]");
      read_one.close();
      read_two.close();
      write_one.close();
      write_two.close();
      write_out.close();
    } catch (IOException e) {
      logger.severe(e.getMessage());
    }
  }

  private static SimpleRule fill(LineReader reader, SimpleRule first, Map<String, SimpleRule> pp) {
    pp.clear();
    pp.put(first.target(), first);
    while (reader.hasNext()) {
      SimpleRule r = new SimpleRule(reader.next());
      if (!r.head().equals(first.head())) return r;
      pp.put(r.target(), r);
    }
    return null;
  }

  private static double getKLDivergence(Map<String, SimpleRule> p, Map<String, SimpleRule> q) {
    double kld = 0;
    for (String i : p.keySet()) {
      double p_i = -p.get(i).features().get("p(e|f,LHS)");
      if (q.containsKey(i))
        kld += (p_i + q.get(i).features().get("p(e|f,LHS)")) * Math.exp(p_i);
      else
        kld += (p_i + 16) * Math.exp(p_i);
    }
    return kld;
  }

  private static double getEntropy(Map<String, SimpleRule> p) {
    double ent = 0;
    for (String i : p.keySet()) {
      double p_i = p.get(i).features().get("p(e|f,LHS)");
      ent += p_i * Math.exp(-p_i);
    }
    return ent;
  }
}
