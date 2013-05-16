package edu.jhu.thrax.distributional;

import java.util.HashMap;
import java.util.Map;

public class InputFormats {

  public enum Format {
    NGRAM(0, "text"), PARSE(1, "parse"), DEP(2, "dep"), NER(3, "ner");

    private static Map<Integer, Format> map;

    static {
      map = new HashMap<Integer, Format>();
      for (Format t : Format.values())
        map.put(t.code, t);
    }

    public static Format get(int code) {
      return map.get(code);
    }

    public final int code;
    public final String name;

    Format(int code, String name) {
      this.code = code;
      this.name = name;
    }
  }
}
