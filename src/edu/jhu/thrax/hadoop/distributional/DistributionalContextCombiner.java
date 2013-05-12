package edu.jhu.thrax.hadoop.distributional;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

import edu.jhu.jerboa.sim.SLSH;

public class DistributionalContextCombiner
    extends Reducer<Text, ContextGroups, Text, ContextGroups> {

  private SLSH slsh;

  public void setup(Context context) throws IOException, InterruptedException {
    Configuration conf = context.getConfiguration();
    slsh = CommonLSH.getSLSH(conf);
  }

  protected void reduce(Text key, Iterable<ContextGroups> values, Context context)
      throws IOException, InterruptedException {
    ContextGroups combined = new ContextGroups();
    for (ContextGroups input : values)
      combined.merge(input, slsh);
    context.write(key, combined);
  }
}
