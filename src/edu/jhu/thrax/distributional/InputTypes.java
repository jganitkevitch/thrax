package edu.jhu.thrax.distributional;

import java.util.HashMap;
import java.util.Map;

public class InputTypes {

  public enum Type {
    NGRAM(0, "text"), SYN(1, "parse"), DEP(2, "dep"), CDEP(3, "cdep"), CPDEP(4, "cpdep"), NER(5,
        "ner");

    private static Map<Integer, Type> map;

    static {
      map = new HashMap<Integer, Type>();
      for (Type t : Type.values())
        map.put(t.code, t);
    }

    public static Type get(int code) {
      return map.get(code);
    }

    public final int code;
    public final String name;

    Type(int code, String name) {
      this.code = code;
      this.name = name;
    }
  }
}
