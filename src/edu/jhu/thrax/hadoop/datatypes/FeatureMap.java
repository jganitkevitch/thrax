package edu.jhu.thrax.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import edu.jhu.thrax.util.Vocabulary;

public class FeatureMap implements Writable {

  private Map<Integer, FeatureValue> map;

  public FeatureMap() {
    map = new HashMap<Integer, FeatureValue>();
  }

  public FeatureMap(FeatureMap fm) {
    this();
    for (int key : fm.map.keySet())
      this.map.put(key, fm.map.get(key));
  }

  public Writable get(int key) {
    return map.get(key).get();
  }

  public Writable get(String key) {
    return map.get(Vocabulary.id(key)).get();
  }

  public void put(int key, FeatureValue val) {
    map.put(key, val);
  }

  public void put(String key, FeatureValue val) {
    map.put(Vocabulary.id(key), val);
  }

  public boolean containsKey(int key) {
    return map.containsKey(key);
  }

  public Set<Integer> keySet() {
    return map.keySet();
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    map.clear();
    int size = WritableUtils.readVInt(in);
    for (int i = 0; i < size; ++i) {
      int key = 0;
      FeatureValue val = new FeatureValue();
      key = WritableUtils.readVInt(in);
      val.readFields(in);
      map.put(key, val);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, map.size());
    for (int key : map.keySet()) {
      WritableUtils.writeVInt(out, key);
      map.get(key).write(out);
    }
  }
}
