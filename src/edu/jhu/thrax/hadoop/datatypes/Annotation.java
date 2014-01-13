package edu.jhu.thrax.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class Annotation implements Writable {

  // Source-to-target alignment.
  private AlignmentWritable f2e = null;

  // Rule occurrence count.
  private int count;
  // Fractional occurrence count.
  private float fractional;

  public Annotation() {
    count = 0;
    fractional = 0;
  }

  public Annotation(int c) {
    count = c;
    fractional = 0;
  }
  
  public Annotation(float f) {
    count = 1;
    fractional = f;
  }

  public Annotation(Annotation a) {
    count = a.count;
    fractional = a.fractional;
    this.f2e = new AlignmentWritable(a.f2e);
  }
  
  public Annotation(AlignmentWritable f2e) {
    count = 1;
    fractional = 0;
    this.f2e = f2e;
  }

  public void merge(Annotation that) {
    this.count += that.count;
    this.fractional += that.fractional;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    FloatWritable f = new FloatWritable();
    f.readFields(in);
    fractional = f.get();
    
    count = WritableUtils.readVInt(in);
    
    BooleanWritable b = new BooleanWritable();
    b.readFields(in);
    if (b.get()) {
      f2e = new AlignmentWritable();
      f2e.readFields(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    FloatWritable f = new FloatWritable(fractional);
    f.write(out);
    WritableUtils.writeVInt(out, count);
    BooleanWritable b = new BooleanWritable(f2e != null);
    b.write(out);
    if (f2e != null)
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
    if (fractional == 0)
      return count;
    return fractional;
  }
  
  public void setFractional(float f) {
    this.fractional = f;
  }
}
