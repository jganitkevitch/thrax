package edu.jhu.thrax.tools;

import java.io.BufferedWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.logging.Logger;

import edu.jhu.jerboa.util.FileManager;
import edu.jhu.thrax.hadoop.datatypes.PrimitiveUtils;
import edu.jhu.thrax.util.FormatUtils;
import edu.jhu.thrax.util.io.LineReader;

public class RankPhrases {

  private static final Logger logger = Logger.getLogger(RankPhrases.class.getName());

  public static void main(String[] args) {

    String input_file = null;
    String output_file = null;
    String ranks_string = null;

    for (int i = 0; i < args.length; i++) {
      if ("-i".equals(args[i]) && (i < args.length - 1)) {
        input_file = args[++i];
      } else if ("-o".equals(args[i]) && (i < args.length - 1)) {
        output_file = args[++i];
      } else if ("-r".equals(args[i]) && (i < args.length - 1)) {
        ranks_string = args[++i];
      }
    }

    if (input_file == null) {
      logger.severe("No input specified.");
      return;
    }
    if (ranks_string == null) {
      logger.severe("No ranking fields specified.");
      return;
    }
    if (output_file == null) {
      logger.severe("No output file specified.");
      return;
    }

    String[] ranks = ranks_string.split(",");
    final int rank_num = ranks.length;

    try {
      LineReader reader = new LineReader(input_file);
      BufferedWriter writer = FileManager.getWriter(output_file);

      ArrayList<Entry> entries = new ArrayList<Entry>();

      System.err.print("[");
      long count = 0;
      while (reader.hasNext()) {
        entries.add(new Entry(reader.next(), rank_num + 1));
        if (++count % 25000 == 0) System.err.print("-");
      }
      System.err.println("]");
      reader.close();

      for (int i = 0; i < ranks.length; ++i) {
        final int r = Integer.parseInt(ranks[i]);
        Collections.sort(entries, new Comparator<Entry>() {
          public int compare(Entry a, Entry b) {
            return PrimitiveUtils.compare(a.values[r], b.values[r]);
          }
        });
        for (int j = 0; j < entries.size(); ++j)
          entries.get(j).ranks[i] = j + 1;
      }
      for (Entry e : entries) {
        int sum = 0;
        for (int j = 0; j < rank_num; ++j)
          sum += e.ranks[j];
        e.ranks[rank_num] = sum;
      }

      Collections.sort(entries, new Comparator<Entry>() {
        public int compare(Entry a, Entry b) {
          return PrimitiveUtils.compare(a.ranks[rank_num], b.ranks[rank_num]);
        }
      });
      for (Entry e : entries)
        writer.write(e.toString() + "\n");
      writer.close();
    } catch (IOException e) {
      logger.severe(e.getMessage());
    }
  }

  private static class Entry {
    String head;
    float[] values;
    int[] ranks;

    public Entry(String line, int r) {
      String[] fields = FormatUtils.P_DELIM.split(line);
      head = fields[0] + FormatUtils.DELIM + fields[1];
      values = new float[fields.length - 2];
      for (int i = 2; i < fields.length; ++i)
        values[i - 2] = Float.parseFloat(fields[i]);
      ranks = new int[r];
    }

    public String toString() {
      StringBuilder sb = new StringBuilder();
      for (float f : values)
        sb.append(FormatUtils.DELIM + f);
      for (float r : ranks)
        sb.append(FormatUtils.DELIM + r);
      return head + sb.toString();
    }
  }
}
