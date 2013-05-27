package edu.jhu.thrax.tools;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.logging.Logger;

import edu.jhu.jerboa.util.FileManager;
import edu.jhu.thrax.util.FormatUtils;
import edu.jhu.thrax.util.io.LineReader;

/**
 * Takes a set of rules (in Joshua format) and finds their occurrences in a Joshua-style grammar.
 * Good for retrieving full paraphrase rules (with features) corresponding to manually judged
 * paraphrases.
 * 
 * @author jg
 * 
 */
public class ParaphraseMatch {

  private static final Logger logger = Logger.getLogger(ParaphraseMatch.class.getName());

  public static void main(String[] args) {

    String grammar_file = null;
    String reference_file = null;
    String output_file = null;

    for (int i = 0; i < args.length; i++) {
      if ("-g".equals(args[i]) && (i < args.length - 1)) {
        grammar_file = args[++i];
      } else if ("-r".equals(args[i]) && (i < args.length - 1)) {
        reference_file = args[++i];
      } else if ("-o".equals(args[i]) && (i < args.length - 1)) {
        output_file = args[++i];
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
    if (output_file == null) {
      logger.severe("No output file specified.");
      return;
    }

    HashMap<String, List<Double>> phrases = new HashMap<String, List<Double>>();

    try {
      LineReader reference_reader = new LineReader(reference_file);
      while (reference_reader.hasNext()) {
        String line = reference_reader.next().trim();
        if (line.contains("\t")) {
          String[] fields = FormatUtils.P_TAB.split(line);
          if (!phrases.containsKey(fields[1])) {
            phrases.put(fields[1], new ArrayList<Double>());
          }
          phrases.get(fields[1]).add(Double.parseDouble(fields[0]));
        } else {
          phrases.put(line, null);
        }
      }
      reference_reader.close();

      BufferedWriter output_writer = FileManager.getWriter(output_file);

      LineReader reader = new LineReader(grammar_file);
      int num_found = 0;
      while (reader.hasNext()) {
        String rule_line = reader.next().trim();
        String[] fields = FormatUtils.P_DELIM.split(rule_line);
        if (fields.length < 4) continue;
        
        String candidate = fields[0] + " ||| " + fields[1] + " ||| " + fields[2];

        if (phrases.containsKey(candidate)) {
          double score = 0.0;
          if (phrases.get(candidate) != null) {
            for (double s : phrases.get(candidate))
              score += s;
            score /= phrases.get(candidate).size();
          }
          output_writer.write(candidate + " ||| " + (score != 0 ? "HumanScore=" + score + " " : "")
              + fields[3] + "\n");
          ++num_found;
        }
      }
      reader.close();
      output_writer.close();

      System.err.println("Requested: " + phrases.size());
      System.err.println("Found:     " + num_found);

    } catch (IOException e) {
      logger.severe(e.getMessage());
    }
  }
}
