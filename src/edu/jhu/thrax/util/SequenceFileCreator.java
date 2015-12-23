package edu.jhu.thrax.util;

import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.io.Text;

public class SequenceFileCreator
{
	public static void main(String [] argv) throws Exception
	{
		LongWritable k = new LongWritable();
		Text v = new Text();

		Configuration conf = new Configuration();
		Path path = new Path(argv[0]);
		SequenceFile.Writer writer = SequenceFile.createWriter(conf, 
		    SequenceFile.Writer.file(path), 
		    SequenceFile.Writer.keyClass(LongWritable.class), 
		    SequenceFile.Writer.valueClass(Text.class));

		long current = 0;
		Scanner scanner = new Scanner(System.in, "UTF-8");
		while (scanner.hasNextLine()) {
			String line = scanner.nextLine();
			k.set(current);
			v.set(line);
			writer.append(k, v);
			current++;
		}
		scanner.close();
		writer.close();
		return;
	}
}

