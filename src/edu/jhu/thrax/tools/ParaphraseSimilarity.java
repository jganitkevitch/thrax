package edu.jhu.thrax.tools;

import java.io.BufferedWriter;
import java.util.logging.Logger;

import edu.jhu.jerboa.sim.SLSH;
import edu.jhu.jerboa.util.FileManager;
import edu.jhu.thrax.util.FormatUtils;
import edu.jhu.thrax.util.io.LineReader;

/**
 * Simple grammar scoring tool. Loads signatures and adds a similarity feature to grammar rules by
 * scoring full phrases.
 * 
 * @author jg
 * 
 */
public class ParaphraseSimilarity {

  private static final Logger logger = Logger.getLogger(ParaphraseSimilarity.class.getName());

  public static void main(String[] args) {

    String grammar_file = null;
    String output_file = null;
    String feature_name = "Similarity";

    for (int i = 0; i < args.length; i++) {
      if ("-g".equals(args[i]) && (i < args.length - 1)) {
        grammar_file = args[++i];
      } else if ("-o".equals(args[i]) && (i < args.length - 1)) {
        output_file = args[++i];
      } else if ("-f".equals(args[i]) && (i < args.length - 1)) {
        feature_name = args[++i];
      }
    }

    if (grammar_file == null) {
      logger.severe("No grammar specified.");
      return;
    }
    if (output_file == null) {
      logger.severe("No output file specified.");
      return;
    }

    try {
      SLSH slsh = SLSH.load();

      BufferedWriter output_writer = FileManager.getWriter(output_file);

      LineReader reader = new LineReader(grammar_file);
      int num_scored = 0, num_seen = 0;
      while (reader.hasNext()) {
        String rule_line = reader.next().trim();
        String[] fields = FormatUtils.P_DELIM.split(rule_line);
        double similarity = slsh.score(fields[1], fields[2]);
        if (similarity == -1.0)
          output_writer.write(rule_line + " " + feature_name + "=0\n");
        else {
          output_writer.write(rule_line + " " + feature_name + "="
              + String.format("%.5f", similarity) + "\n");
          ++num_scored;
        }
        ++num_seen;
      }
      reader.close();
      output_writer.close();
      System.err.println("Seen rules:   " + num_seen);
      System.err.println("Scored rules: " + num_scored);

    } catch (Exception e) {
      logger.severe(e.getMessage());
    }
  }
}
