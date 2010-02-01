package edu.jhu.thrax;

import edu.jhu.thrax.util.getopt.OptionMissingArgumentException;

import java.io.IOException;

public class Thrax {


	public static void main(String [] argv)
	{
		try {
			ThraxConfig.configure(argv);

		}
		catch (OptionMissingArgumentException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		catch (IOException e) {
			System.err.println(e.getMessage());
			System.exit(1);
		}
		return;
	}

}
