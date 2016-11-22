package org.powertac.logtool.russell;

import org.powertac.common.Broker;
import org.powertac.common.CashPosition;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;

import java.io.*;
import java.util.HashMap;

/**
  * Learning Curve produces an output of final game score for each broker for every game in a set
  *
  * @author Russell Folk
  */
public class LearningCurve extends LogtoolContext implements Analyzer
{
	//static private Logger log = Logger.getLogger(CustomerStats.class.getName());

	// needed data
	private HashMap<Broker, Double> brokerBalance = null;

	/**
	 * Constructor does nothing. Call setup() before reading a file to
	 * get this to work.
	 */
	private LearningCurve() {
		super();
	}

	/**
	 * Main method just creates an instance and passes command-line args to
	 * its inherited cli() method.
	 */
	public static void main(String[] args) {
		new LearningCurve().cli(args);
	}

	/**
	 * Takes two args, input filename and output filename
	 */
	private void cli(String[] args)
	{
		if (args.length != 2) {
			System.out.println("Usage: <analyzer> input-file output-file");
			return;
		}
		super.cli(args[0], this);
	}

	/**
	  * Creates data structures, opens output file. It would be nice to dump
	  * the broker names at this point, but they are not known until we hit the
	  * first timeslotUpdate while reading the file.
	  */
	@Override
	public void setup()
	{
		DomainObjectReader dor;

		dor = (DomainObjectReader) SpringApplicationContext.getBean("reader");
		dor.registerNewObjectListener(new LearningCurve.CashPositionHandler(), CashPosition.class);

		brokerBalance = new HashMap<>();
	}

	@Override
	public void report()
	{
		for (Broker broker : brokerBalance.keySet())
		{
			FileWriter fw = null;
			BufferedWriter bw = null;
			PrintWriter pw = null;
			boolean newFile = false;
			File file = new File(broker.getUsername());
			if (!(file.exists() && !file.isDirectory()))
				newFile = true;
			try
			{
				fw = new FileWriter(file, true);
				bw = new BufferedWriter(fw);
				pw = new PrintWriter(bw);
				if (newFile)
					pw.print(broker.getUsername());
				pw.print("," + brokerBalance.get(broker));
			}
			catch (FileNotFoundException e)
			{
				System.out.println("File not found: " + e.getStackTrace());
			}
			catch (IOException e)
			{
				System.out.println("IO Exception: " + e.getStackTrace());
			}
			finally
			{
				if (pw != null)
					pw.close();
				try
				{
					if (bw != null)
						bw.close();
				}
				catch (IOException e)
				{

				}
				try
				{
					if (fw != null)
						fw.close();
				}
				catch (IOException e)
				{

				}
			}
		}
	}

	// -----------------------------------
	// catch CashPosition events
	class CashPositionHandler implements NewObjectListener
	{
		@Override
		public void handleNewObject (Object thing)
		{
			CashPosition cp = (CashPosition) thing;
			Broker broker = cp.getBroker();
			double balance = cp.getBalance();
			brokerBalance.put(broker, balance);
		}
	}
}