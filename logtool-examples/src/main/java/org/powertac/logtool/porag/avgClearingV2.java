package org.powertac.logtool.porag;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.TreeMap;







//import org.apache.log4j.Logger;
import org.powertac.common.Broker;
import org.powertac.common.ClearedTrade;
import org.powertac.common.Competition;
import org.powertac.common.MarketTransaction;
import org.powertac.common.Orderbook;
import org.powertac.common.TimeService;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.repo.OrderbookRepo;
import org.powertac.common.repo.TimeslotRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.porag.avgClearing.MarketTransactionHandler;
import org.powertac.logtool.porag.avgClearing.OrderbookHandler;
import org.powertac.logtool.porag.avgClearing.TimeslotHandler;
import org.powertac.logtool.porag.avgClearing.TimeslotUpdateHandler;
import org.powertac.logtool.ifc.Analyzer;
import org.powertac.common.Timeslot;

/**
 * Logtool Analyzer that reads ClearedTrade instances as they arrive and builds
 * an array for each timeslot giving all the market clearings for that
 * timeslot,s indexed by leadtime. The output data file has one line/timeslot
 * formatted as<br>
 * [mwh price] [mwh price] ...<br>
 * Each line has 24 entries, assuming that each timeslot is open for trading 24
 * times.
 * 
 * Usage: MktPriceStats state-log-filename output-data-filename
 * 
 * @author John Collins
 */
public class avgClearingV2 extends LogtoolContext implements Analyzer {
	//static private Logger log = Logger.getLogger(MktPriceStats.class.getName());

	// service references
	private TimeslotRepo timeslotRepo;
	private TimeService timeService;
	private OrderbookRepo OrderbookRepo;
	private DomainObjectReader dor;
	private Timeslot timeslot;
	private BrokerRepo brokerRepo;

	// Data
	double clearingPrice[][] = new double[10][25];
	double auctionCount[][] = new double[10][25];
	double clearedVolumeCountSurplus[][] = new double[10][25];
	double clearedVolumeCountDeficit[][] = new double[10][25];
	double sumVolumePricePay[][] = new double[10][25];
	double sumVolumePriceGain[][] = new double[10][25];
	
	double clearedTradeCount[][] = new double[10][25];
	double ordbookClearingPrice[] = new double[25];
	double ordbookAuctionCount[] = new double[25];

	int currentTimeslotflag[] = new int [10]; //0;
	int currentHourAheadflag[] = new int [10];//-1;
	
	int time = 0;
	
	private int ignoreInitial = 0; // timeslots to ignore at the beginning
	private int ignoreCount = 0;
									// Competition.deactivateTimeslotsAhead - 1
	private PrintWriter output = null;
	private PrintWriter debug = null;
	private String dataFilename = "clearedTrades.arff";
	int counter = 0;
	
	ArrayList<Long> arrBrokerIds = new ArrayList<Long>();
	ArrayList<String> arrBrokerNames = new ArrayList<String>();

	/**
	 * Main method just creates an instance and passes command-line args to its
	 * inherited cli() method.
	 */
	public static void main(String[] args) {
		System.out.println("I am running");
		new avgClearingV2().cli(args);
	}

	/**
	 * Takes two args, input filename and output filename
	 */
	private void cli(String[] args) {
		if (args.length != 2) {
			System.out.println("Usage: <analyzer> input-file output-file");
			return;
		}
		dataFilename = args[1];
		super.cli(args[0], this);
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.powertac.logtool.ifc.Analyzer#setup()
	 */
	@Override
	public void setup() {
		
		Arrays.fill(currentTimeslotflag,0);
		Arrays.fill(currentHourAheadflag,-1);
		
		dor = (DomainObjectReader) SpringApplicationContext.getBean("reader");
		timeslotRepo = (TimeslotRepo) getBean("timeslotRepo");
		timeService = (TimeService) getBean("timeService");
		brokerRepo = (BrokerRepo) SpringApplicationContext
				.getBean("brokerRepo");
		registerNewObjectListener(new CompetitionHandler(), Competition.class);
		registerNewObjectListener(new BrokerHandler(), Broker.class);
		registerNewObjectListener(new MarketTransactionHandler(), MarketTransaction.class);
		registerNewObjectListener(new TimeslotUpdateHandler(), TimeslotUpdate.class);
		registerNewObjectListener(new OrderbookHandler(), Orderbook.class);
	
		ignoreCount = ignoreInitial;
		try {
			//output = new PrintWriter(new File(dataFilename));
			//FileWriter fw = new FileWriter(dataFilename, true);
			output = new PrintWriter(new File("PRICE_"+dataFilename));
			debug =  new PrintWriter(new File("VOLUME_"+dataFilename));
		} catch (Exception e) {
			//log.error("Cannot open file " + dataFilename);
		}
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see org.powertac.logtool.ifc.Analyzer#report()
	 */
	@Override
	public void report() {
		double totalVolumeSurplus[] = new double[10];
		double totalVolumeDeficit[] = new double[10];
		double unitVolumePricePay[] = new double[10];
		double unitVolumePriceGain[] = new double[10];
		
		output.print("hourAhead ,");
		debug.print("hourAhead ,");
		
		for(int bid = 0; bid < arrBrokerIds.size(); bid++){
			output.print(arrBrokerNames.get(bid)+", ");
			debug.print(arrBrokerNames.get(bid)+"_Surplus, " + arrBrokerNames.get(bid)+"_Deficit, ");
		}
		output.print("Actual Clearing Price");
		output.println();
		debug.println();

		for(int x = 0; x < clearedVolumeCountSurplus[0].length-1;x++){
			output.format("%d ,",(x+1));
			debug.format("%d ,",(x+1));
			for(int bid = 0; bid < arrBrokerIds.size(); bid++)
			{
				if(auctionCount[bid][x] == 0)
					auctionCount[bid][x] = 1;
				
				clearingPrice[bid][x]/=auctionCount[bid][x];
				unitVolumePricePay[bid]+=sumVolumePricePay[bid][x];
				unitVolumePriceGain[bid]+=sumVolumePriceGain[bid][x];
				totalVolumeSurplus[bid] += clearedVolumeCountSurplus[bid][x];
				totalVolumeDeficit[bid] += clearedVolumeCountDeficit[bid][x];
				output.format("%.2f, ", clearingPrice[bid][x]);
				debug.format("%.2f, %.2f,", clearedVolumeCountSurplus[bid][x], clearedVolumeCountDeficit[bid][x]);
				
			}
		
			if(ordbookAuctionCount[x] == 0)
				ordbookAuctionCount[x] = 1;
			
			ordbookClearingPrice[x]/=ordbookAuctionCount[x];
			output.format("%.2f, ", ordbookClearingPrice[x]);
		
			output.println();
			debug.println();
		}
		
		output.println("Broker, UnitPrice");
		for(int bid = 0; bid < arrBrokerIds.size(); bid++)
		{
			unitVolumePricePay[bid] /= totalVolumeDeficit[bid];
			unitVolumePriceGain[bid] /= totalVolumeSurplus[bid];
			
			if(totalVolumeSurplus[bid] == 0)
				unitVolumePriceGain[bid] = 0;
			if(totalVolumeDeficit[bid] == 0)
				unitVolumePricePay[bid] = 0;
			
			output.println(arrBrokerNames.get(bid) + " , UnitPay:, " + unitVolumePricePay[bid] + ", UnitGain:, " + unitVolumePriceGain[bid]);
		}
		
		output.close(); 
		debug.close();
		System.out.println("Finished");
	}

	class CompetitionHandler implements NewObjectListener{
		@Override
		public void handleNewObject(Object comp){
			Competition competition = (Competition) comp;
			MktPriceStats.numberofbrokers = competition.getBrokers().size();
			System.out.println("Number of brokers : " + competition.getBrokers().size() + " tostring " + competition.toString());
		}
	}

	// -----------------------------------
	// catch MarketTransaction messages
	class MarketTransactionHandler implements NewObjectListener {
		@Override
		public void handleNewObject(Object thing) {
			if (ignoreCount > 0) {
				return; // nothing to do yet
			}
			MarketTransaction mt = (MarketTransaction) thing;
			for(int bid = 0; bid < arrBrokerIds.size(); bid++){
				if (mt.getBroker().getId() == arrBrokerIds.get(bid)) {
					int currenttimeslot = timeslotRepo.currentSerialNumber();
					int timeslotSerial = mt.getTimeslot().getSerialNumber();
					int hourAhead = Math.abs(timeslotSerial - currenttimeslot);
					//System.out.println(" Currentts: " + currenttimeslot + " HourAhead : " + hourAhead + " ClearingV : " + mt.getMWh() + " ClearP : " + mt.getPrice() + " " + arrBrokerNames.get(bid));
					if(mt.getPrice()<0){
						clearedVolumeCountDeficit[bid][hourAhead]+= mt.getMWh();
						sumVolumePricePay[bid][hourAhead] += (mt.getPrice()*mt.getMWh());
					}
					else{
						clearedVolumeCountSurplus[bid][hourAhead]+= mt.getMWh();
						sumVolumePriceGain[bid][hourAhead] += (mt.getPrice()*mt.getMWh());
					}
					if(currentTimeslotflag[bid] != currenttimeslot || currentHourAheadflag[bid] != hourAhead)
					{
						clearingPrice[bid][hourAhead]+= mt.getPrice();
						auctionCount[bid][hourAhead]++;
						//System.out.println("Offset : " + offset + "Trade counted "+ clearedTradeCount[offset]);
						currentHourAheadflag[bid] = hourAhead;
						currentTimeslotflag[bid] = currenttimeslot;
					}
				}
			}
		}
	}

	// --s---------------------------------
		// catch OrderbookHandler events
		class OrderbookHandler implements NewObjectListener {

			@Override
			public void handleNewObject(Object thing) {
				Orderbook ob = (Orderbook) thing;

				if (ignoreCount-- <= 0) {
					if (ob.getClearingPrice() == null) {
						ordbookClearingPrice[counter] += 0;
					} else {
						ordbookAuctionCount[counter]++;
						ordbookClearingPrice[counter] += ob.getClearingPrice();
					}
					//System.out.println(" Currentts: " + ob.getTimeslotIndex() + " HourAhead : " + counter + " ClearP : " + ob.getClearingPrice() + " Orderbook");
					
					counter++;
				}
			}
		}

		// -----------------------------------
		// catch TimeslotUpdate events
		class TimeslotUpdateHandler implements NewObjectListener {

			@Override
			public void handleNewObject(Object thing) {
				// averages cmt;
							
				if (ignoreCount-- <= 0) {
					counter = 0;
				}
			}
		}
	
	// -----------------------------------
	// catch Broker events
	class BrokerHandler implements NewObjectListener {
		@Override
		public void handleNewObject(Object thing) {
			Broker broker = (Broker) thing;
			String username = broker.getUsername().toUpperCase();
			arrBrokerIds.add(broker.getId());
			arrBrokerNames.add(username);
		}
	}

}