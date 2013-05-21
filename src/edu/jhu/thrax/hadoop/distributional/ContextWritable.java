package edu.jhu.thrax.hadoop.distributional;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import edu.jhu.jerboa.sim.SLSH;
import edu.jhu.jerboa.sim.Signature;
import edu.jhu.thrax.hadoop.datatypes.PrimitiveUtils;

/**
 * A writable for LSH sums.
 * 
 * @author Juri Ganitkevitch
 * 
 */
public class ContextWritable implements Writable {
  public int strength;
  public float[] sums;

  public ContextWritable() {
    this.strength = 0;
    this.sums = null;
  }

  public ContextWritable(int s) {
    this.strength = s;
    this.sums = null;
  }

  public ContextWritable(int s, float[] sums) {
    this.strength = s;
    this.sums = sums;
  }

  public ContextWritable(Map<String, Integer> features, SLSH slsh) {
    sums = new float[slsh.numBits];
    for (Map.Entry<String, Integer> e : features.entrySet())
      slsh.updateSums(sums, e.getKey(), e.getValue());
    strength = 1;
  }

  public void merge(ContextWritable that, SLSH slsh) {
    this.mergeSums(that, slsh);
    this.strength += that.strength;
  }

  private void mergeSums(ContextWritable that, SLSH slsh) {
    Signature this_signature = new Signature();
    Signature that_signature = new Signature();

    this_signature.sums = sums;
    that_signature.sums = that.sums;
    slsh.updateSignature(this_signature, that_signature);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    strength = WritableUtils.readVInt(in);
    sums = PrimitiveUtils.readFloatArray(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, strength);
    PrimitiveUtils.writeFloatArray(out, sums);
  }
}
