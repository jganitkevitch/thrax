package edu.jhu.thrax.util.io;

import java.util.ArrayList;

import edu.jhu.thrax.util.Vocabulary;
import edu.jhu.thrax.util.exceptions.MalformedInputException;

/**
 * Methods for validating user input. These should be used anywhere user input is received.
 */
public class InputUtilities {
  /**
   * Returns an array of the leaves of a parse tree, reading left to right.
   * 
   * @param parse a representation of a parse tree (Penn Treebank style)
   * @return an array of String giving the labels of the tree's leaves
   * @throws MalformedInputException if the parse tree is not well-formed
   */
  public static int[] parseYield(String input) throws MalformedInputException {
    if (input == null || input.isEmpty()) return new int[0];
    if (input.charAt(0) != '(') throw new MalformedInputException("malformed parse");
    
    ArrayList<Integer> words = new ArrayList<Integer>();
    
    int from = 0, to = 0;
    boolean seeking = true;
    boolean nonterminal = false;
    char current;
    // Run through entire (potentially parsed) sentence.
    while (from < input.length() && to < input.length()) {
      if (seeking) {
        // Seeking mode: looking for the start of the next symbol.
        current = input.charAt(from);
        // We skip brackets and spaces.
        if (current == '(' || current == ')' || current == ' ') {
          ++from;
          // Found a non spacing symbol, go into word filling mode.
        } else {
          to = from + 1;
          seeking = false;
          nonterminal = (input.charAt(from - 1) == '(');
        }
      } else {
        // Word filling mode. Advance to until we hit the end or spacing.
        current = input.charAt(to);
        if (current == ' ' || current == ')' || current == '(') {
          // Word ended.
            if (!nonterminal)
              words.add(Vocabulary.id(input.substring(from, to)));
          from = to + 1;
          seeking = true;
        } else {
          ++to;
        }
      }
    }
    int[] result = new int[words.size()];
    for (int i = 0; i < result.length; ++i)
      result[i] = words.get(i);
    return result;
  }
}
