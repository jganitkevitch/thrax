package edu.jhu.thrax.input;

import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;

import edu.jhu.thrax.syntax.DependencyTree;
import edu.jhu.thrax.syntax.ParseTree;
import edu.jhu.thrax.util.FormatUtils;
import edu.jhu.thrax.util.Vocabulary;
import edu.jhu.thrax.util.exceptions.MalformedInputException;
import edu.jhu.thrax.util.io.InputUtilities;

public class ThraxInputParser {

  private Format[] types;
  private String[] names;

  private boolean reverse;

  public ThraxInputParser(Configuration conf) {
    this(conf, conf.getBoolean("thrax.reverse", false));
  }

  public ThraxInputParser(Configuration conf, boolean r) {
    reverse = r;
    String format = conf.get("thrax.input-format", "src ||| tgt ||| align");

    String[] fields = FormatUtils.P_DELIM.split(format);
    types = new Format[fields.length];
    names = new String[fields.length];
    for (int i = 0; i < fields.length; ++i) {
      if (fields[i].contains(":")) {
        String[] parts = FormatUtils.P_COLON.split(fields[i]);
        types[i] = Format.get(parts[0]);
        names[i] = parts[1];
      } else {
        types[i] = Format.get(fields[i]);
      }
    }
    if (reverse) {
      for (int i = 0; i < types.length; ++i)
        types[i] = types[i].flip();
    }
  }

  public ThraxInput parse(String line) throws MalformedInputException {
    ThraxInput input = new ThraxInput();
    String[] fields = FormatUtils.P_DELIM.split(line);
    if (fields.length < types.length) throw new MalformedInputException("not enough fields");
    for (int i = 0; i < types.length; ++i)
      types[i].fill(input, names[i], fields[i]);
    input.sanityCheck();
    return input;
  }


  public enum Format {

    SKIP("skip", "skip") {
      public void fill(ThraxInput in, String name, String field) {}
    },

    SRC("src", "tgt") {
      public void fill(ThraxInput in, String name, String field) {
        in.source = Vocabulary.addAll(field);
      }
    },

    TGT("tgt", "src") {
      public void fill(ThraxInput in, String name, String field) {
        in.target = Vocabulary.addAll(field);
      }
    },

    SRC_PARSE("src_parse", "tgt_parse") {
      public void fill(ThraxInput in, String name, String field) throws MalformedInputException {
        in.src_parse = ParseTree.fromPennFormat(field);
        in.source = InputUtilities.parseYield(field);
      }
    },

    TGT_PARSE("tgt_parse", "src_parse") {
      public void fill(ThraxInput in, String name, String field) throws MalformedInputException {
        in.src_parse = ParseTree.fromPennFormat(field);
        in.target = InputUtilities.parseYield(field);
      }
    },

    SRC_DEP("src_dep", "tgt_dep") {
      public void fill(ThraxInput in, String name, String field) {
        if (in.src_deps == null) in.src_deps = new HashMap<String, DependencyTree>();
        in.src_deps.put(name, new DependencyTree(field));
      }
    },

    TGT_DEP("tgt_dep", "src_dep") {
      public void fill(ThraxInput in, String name, String field) {
        if (in.tgt_deps == null) in.tgt_deps = new HashMap<String, DependencyTree>();
        in.tgt_deps.put(name, new DependencyTree(field));
      }
    },

    SRC_TAG("src_tag", "tgt_tag") {
      public void fill(ThraxInput in, String name, String field) {
        if (in.src_tags == null) in.src_tags = new HashMap<String, int[]>();
        in.src_tags.put(name, Vocabulary.addAll(field));
      }
    },

    TGT_TAG("tgt_tag", "src_tag") {
      public void fill(ThraxInput in, String name, String field) {
        if (in.tgt_tags == null) in.tgt_tags = new HashMap<String, int[]>();
        in.tgt_tags.put(name, Vocabulary.addAll(field));
      }
    },

    ALIGN("align", "ralign") {
      public void fill(ThraxInput in, String name, String field) {
        in.alignment = ArrayAlignment.fromString(field, false);
      }
    },

    RALIGN("ralign", "align") {
      public void fill(ThraxInput in, String name, String field) {
        in.alignment = ArrayAlignment.fromString(field, true);
      }
    },

    WEIGHTS("weights", "weights") {
      public void fill(ThraxInput in, String name, String field) {
        String[] strings = FormatUtils.P_SPACE.split(field);
        in.weights = new float[strings.length];
        for (int i = 0; i < strings.length; ++i)
          in.weights[i] = Float.parseFloat(strings[i]);
      }
    },

    LABELS("labels", "labels") {
      public void fill(ThraxInput in, String name, String field) {
        in.labels = Vocabulary.addAll(field);
      }
    };

    private String keyword;
    private String flipside;

    private Format(String keyword, String flipside) {
      this.keyword = keyword;
      this.flipside = flipside;
    }
    
    private Format flip() {
      return get(this.flipside);
    }

    public abstract void fill(ThraxInput in, String name, String field)
        throws MalformedInputException;

    public static Format get(String keyword) {
      for (Format f : values())
        if (f.keyword.equals(keyword)) return f;
      return null;
    }
  }
}
