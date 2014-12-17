package edu.jhu.thrax.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class Annotation implements Writable {

  // Source-to-target alignment.
  private AlignmentWritable f2e;

  // Rule occurrence count.
  private float count;
  private int support;

  public Annotation() {
    count = 0;
    support = 0;
    f2e = new AlignmentWritable();
  }

  public Annotation(int c, int s) {
    count = (float) c;
    support = s;
    f2e = new AlignmentWritable();
  }

  public Annotation(float c, int s) {
    count = c;
    support = s;
    f2e = new AlignmentWritable();
  }

  public Annotation(Annotation a) {
    count = a.count;
    support = a.support;
    this.f2e = new AlignmentWritable(a.f2e);
  }

  public Annotation(int c, int s, AlignmentWritable f2e) {
    count = (float) c;
    support = s;
    this.f2e = f2e;
  }

  public Annotation(float c, int s, AlignmentWritable f2e) {
    count = c;
    support = s;
    this.f2e = f2e;
  }

  public void merge(Annotation that) {
    this.count += that.count;
    this.support += that.support;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    FloatWritable f = new FloatWritable();
    f.readFields(in);
    count = f.get();
    support = WritableUtils.readVInt(in);
    f2e.readFields(in);
  }

  @Override
  public void write(DataOutput out) throws IOException {
    FloatWritable f = new FloatWritable(count);
    f.write(out);
    WritableUtils.writeVInt(out, support);
    f2e.write(out);
  }

  public AlignmentWritable e2f() {
    return f2e.flip();
  }

  public AlignmentWritable f2e() {
    return f2e;
  }

  public void setAlignment(AlignmentWritable a) {
    f2e = a;
  }

  public float count() {
    return count;
  }

  public void setCount(float c) {
    this.count = c;
  }

  public int support() {
    return support;
  }

  public void setSupport(int s) {
    support = s;
  }
}
