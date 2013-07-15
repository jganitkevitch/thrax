package edu.jhu.thrax.tools;

import java.io.IOException;
import java.util.logging.Logger;

import edu.jhu.thrax.util.FormatUtils;
import edu.jhu.thrax.util.io.LineReader;

public class UnSparse {

  private static final Logger logger = Logger.getLogger(UnSparse.class.getName());

  public static void main(String[] args) {

    String grammar_file = null;
    String[] features = null;

    for (int i = 0; i < args.length; i++) {
      if ("-g".equals(args[i]) && (i < args.length - 1)) {
        grammar_file = args[++i];
      } else if ("-f".equals(args[i]) && (i < args.length - 1)) {
        features = args[++i].split(",");
      }
    }

    if (grammar_file == null) {
      logger.severe("No grammar specified.");
      return;
    }
    if (features == null) {
      logger.severe("No filter file specified.");
      return;
    }

    try {
      LineReader reader = new LineReader(grammar_file);

      while (reader.hasNext()) {
        String rule_line = reader.next().trim();

        try {
          String[] fields = FormatUtils.P_DELIM.split(rule_line);

          StringBuilder sb = new StringBuilder(fields[3]);

          for (String feat_name : features) {
            if (!fields[3].contains(feat_name)) sb.append(" " + feat_name + "=0");
          }
          fields[3] = sb.toString();

          System.out.println(fields[0] + FormatUtils.DELIM + fields[1] + FormatUtils.DELIM
              + fields[2] + FormatUtils.DELIM + fields[3] + FormatUtils.DELIM + fields[4]);

        } catch (Exception e) {
          e.printStackTrace();
          logger.warning(e.getMessage());
          logger.warning(rule_line);
          continue;
        }
      }
      reader.close();
    } catch (IOException e) {
      logger.severe(e.getMessage());
    }
  }

}
