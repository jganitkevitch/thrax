package edu.jhu.thrax.hadoop.distributional;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.Partitioner;

import edu.jhu.jerboa.sim.Signature;
import edu.jhu.thrax.hadoop.comparators.FieldComparator;
import edu.jhu.thrax.hadoop.datatypes.PrimitiveUtils;

public class SignatureWritable implements WritableComparable<SignatureWritable> {
  public Text key;
  public Text group;
  public byte[] bytes;
  public int strength;

  public SignatureWritable() {
    this.key = new Text();
    this.group = new Text();
    this.bytes = null;
    this.strength = 0;
  }

  public SignatureWritable(Text key, Text group, Signature signature, int strength) {
    this.key = new Text(key);
    this.group = new Text(group);
    // TODO: deep copy?
    this.bytes = signature.bytes;
    this.strength = strength;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    strength = WritableUtils.readVInt(in);
    group.readFields(in);
    key.readFields(in);
    bytes = PrimitiveUtils.readByteArray(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeVInt(out, strength);
    group.write(out);
    key.write(out);
    PrimitiveUtils.writeByteArray(out, bytes);
  }

  @Override
  public int compareTo(SignatureWritable that) {
    int cmp = group.compareTo(that.group);
    if (cmp != 0) return cmp;

    cmp = PrimitiveUtils.compare(this.strength, that.strength);
    // Flip sign for descending sort order.
    if (cmp != 0) return -cmp;

    return key.compareTo(that.key);
  }

  static {
    WritableComparator.define(SignatureWritable.class, new GroupComparator());
  }

  public static class GroupComparator extends WritableComparator {
    private static final WritableComparator COMP = new Text.Comparator();
    private static final FieldComparator GROUP_COMP = new FieldComparator(0, COMP);
    private static final FieldComparator KEY_COMP = new FieldComparator(1, COMP);

    public GroupComparator() {
      super(SignatureWritable.class);
    }

    public int compare(byte[] b1, int s1, int l1, byte[] b2, int s2, int l2) {
      try {
        int c1 = WritableComparator.readVInt(b1, s1);
        int c2 = WritableComparator.readVInt(b2, s2);

        int h1 = WritableUtils.decodeVIntSize(b1[s1]);
        int h2 = WritableUtils.decodeVIntSize(b2[s2]);

        int cmp;
        cmp = GROUP_COMP.compare(b1, s1 + h1, l1 - h1, b2, s2 + h2, l2 - h2);
        if (cmp != 0) return cmp;

        cmp = PrimitiveUtils.compare(c1, c2);
        if (cmp != 0) return -cmp;

        cmp = KEY_COMP.compare(b1, s1 + h1, l1 - h1, b2, s2 + h2, l2 - h2);

        // TODO: Doesn't take into account actual signature – possible bug?
        return cmp;
      } catch (IOException e) {
        throw new IllegalArgumentException(e);
      }
    }
  }

  public static class SignaturePartitioner extends Partitioner<SignatureWritable, Writable> {
    public int getPartition(SignatureWritable signature, Writable value, int num_partitions) {
      return (signature.group.hashCode() & Integer.MAX_VALUE) % num_partitions;
    }
  }
}
