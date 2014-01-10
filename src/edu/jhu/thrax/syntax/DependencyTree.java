package edu.jhu.thrax.syntax;

import edu.jhu.thrax.util.FormatUtils;
import edu.jhu.thrax.util.Vocabulary;

public class DependencyTree {

  private byte[] links;
  private int[] types;

  public DependencyTree(String input) {
    String[] entries = FormatUtils.P_SPACE.split(input.trim());

    links = new byte[entries.length * 2];
    types = new int[entries.length];
    for (int i = 0; i < entries.length; ++i) {
      String[] fields = FormatUtils.P_DASH.split(entries[i]);
      types[i] = Vocabulary.id(fields[0]);
      links[2 * i] = (byte) (Integer.parseInt(fields[1]) - 1);
      links[2 * i + 1] = (byte) (Integer.parseInt(fields[2]) - 1);
    }
  }
}
