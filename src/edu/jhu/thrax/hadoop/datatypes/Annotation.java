package edu.jhu.thrax.hadoop.datatypes;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.Writable;

public class Annotation implements Writable {

  // Source-to-target alignment.
  private AlignmentWritable f2e = null;

  // Rule occurrence count.
  private float count;

  public Annotation(int c) {
    count = (float) c;
  }
  
  public Annotation(float c) {
    count = c;
  }

  public Annotation(Annotation a) {
    count = a.count;
    this.f2e = new AlignmentWritable(a.f2e);
  }
  
  public Annotation(int c, AlignmentWritable f2e) {
    count = (float) c;
    this.f2e = f2e;
  }
  
  public Annotation(float c, AlignmentWritable f2e) {
    count = c;
    this.f2e = f2e;
  }

  public void merge(Annotation that) {
    this.count += that.count;
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    FloatWritable f = new FloatWritable();
    f.readFields(in);
    count = f.get();
    
    BooleanWritable b = new BooleanWritable();
    b.readFields(in);
    if (b.get()) {
      f2e = new AlignmentWritable();
      f2e.readFields(in);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    FloatWritable f = new FloatWritable(count);
    f.write(out);
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
    return count;
  }
  
  public void setCount(float c) {
    this.count = c;
  }
}
