package edu.jhu.thrax.hadoop.distributional;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.jhu.jerboa.sim.SLSH;
import edu.jhu.jerboa.sim.Signature;

public class DistributionalContextReducer
    extends Reducer<Text, ContextGroups, SignatureWritable, NullWritable> {

  private String[] groups;
  private int minCount;
  private SLSH slsh;

  public void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    minCount = conf.getInt("thrax.min-phrase-count", 3);
    slsh = CommonLSH.getSLSH(conf);
    groups = conf.getStrings("thrax.contexts", "context");
  }

  protected void reduce(Text key, Iterable<ContextGroups> values, Context context)
      throws IOException, InterruptedException {
    ContextGroups reduced = new ContextGroups();
    for (ContextGroups input : values)
      reduced.merge(input, slsh);

    if (reduced.strength() >= minCount) {
      for (int i = 0; i < groups.length; ++i) {
        Signature reduced_signature = new Signature();
        reduced_signature.sums = reduced.sums(i);
        slsh.buildSignature(reduced_signature, false);
        context.write(
            new SignatureWritable(key, new Text(groups[i]), reduced_signature, reduced.strength()),
            NullWritable.get());
      }
    }
  }
}
