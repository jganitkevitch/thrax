package edu.jhu.thrax.hadoop.distributional;

import org.apache.hadoop.conf.Configuration;

import edu.jhu.jerboa.sim.SLSH;

public class CommonLSH {

  public static SLSH getSLSH(Configuration conf) {
    SLSH slsh = null;
    try {
      slsh = new SLSH(false);
      slsh.initialize(conf.getInt("thrax.lsh-num-bits", 256),
          conf.getInt("thrax.lsh-pool-size", 17), conf.getInt("thrax.lsh-random-seed", 42));
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }
    return slsh;
  }

}
