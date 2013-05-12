package edu.jhu.thrax.util;

import java.util.Map;

public final class Utils {

  public final static void increment(Map<String, Integer> map, String key) {
    if (map.containsKey(key))
      map.put(key, map.get(key) + 1);
    else
      map.put(key, 1);
  }
  
}
