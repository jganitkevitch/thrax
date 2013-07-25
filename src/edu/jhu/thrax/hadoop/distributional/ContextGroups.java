package edu.jhu.thrax.hadoop.distributional;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import edu.jhu.jerboa.sim.SLSH;

public class ContextGroups implements Writable {

  private ContextWritable[] groups;

  public ContextGroups() {
    groups = null;
  }

  public ContextGroups(ContextWritable[] contexts) {
    groups = contexts;
  }

  public ContextGroups(ContextWritable[] contexts, int num, int k) {
    groups = new ContextWritable[num];
    for (int i = 0; i < groups.length; ++i)
      groups[i] = contexts[k + i];
  }

  public ContextGroups(List<ContextWritable> contexts) {
    groups = new ContextWritable[contexts.size()];
    for (int i = 0; i < groups.length; ++i)
      groups[i] = contexts.get(i);
  }

  public void merge(ContextGroups that, SLSH slsh) {
    if (groups == null) {
      this.groups = that.groups;
      return;
    }
    if (that.groups.length != this.groups.length)
      throw new RuntimeException("Mismatched number of context groups: " + this.groups.length
          + " versus " + that.groups.length + ".");
    for (int i = 0; i < this.groups.length; ++i)
      this.groups[i].merge(that.groups[i], slsh);
  }

  public int strength() {
    if (groups != null && groups.length > 0) return groups[0].strength;
    return 0;
  }

  public float[] sums(int i) {
    return groups[i].sums;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    groups = new ContextWritable[WritableUtils.readVInt(in)];
    for (int i = 0; i < groups.length; ++i) {
      groups[i] = new ContextWritable();
      groups[i].readFields(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, groups.length);
    for (int i = 0; i < groups.length; ++i) {
      groups[i].write(out);
    }
  }

}
