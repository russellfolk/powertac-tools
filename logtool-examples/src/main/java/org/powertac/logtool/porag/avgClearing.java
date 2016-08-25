package org.powertac.logtool.porag;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
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
public class avgClearing extends LogtoolContext implements Analyzer {
	//static private Logger log = Logger.getLogger(MktPriceStats.class.getName());

	// service references
	private TimeslotRepo timeslotRepo;
	private TimeService timeService;
	private OrderbookRepo OrderbookRepo;
	private DomainObjectReader dor;
	private Timeslot timeslot;
	private BrokerRepo brokerRepo;

	// Data
	private TreeMap<Integer, ClearedTrade[]> data;
	private TreeMap<Integer, averages> marketData;
	TreeMap<Integer, Integer> orderbookCounter = new TreeMap<Integer, Integer>();
	int counter = 0;
	double avgClearingPrice[] = new double[25];
	double auctionCount[] = new double[25];
	double clearedVolumeCount[] = new double[25];
	double clearedTradeCount[] = new double[25];
	int currentTimeslotflag = 0;
	int currentHourAheadflag = -1;
	
	int time = 0;
	
	
	
	private int ignoreInitial = 0; // timeslots to ignore at the beginning
	private int ignoreCount = 0;
	private int indexOffset = 0; // should be
									// Competition.deactivateTimeslotsAhead - 1
	public static int numberofbrokers = 0;
	private PrintWriter output = null;
	private PrintWriter debug = null;
	private String dataFilename = "clearedTrades.arff";
	public double brokerID;
	public String brokername = "SPOT";

	/**
	 * Main method just creates an instance and passes command-line args to its
	 * inherited cli() method.
	 */
	public static void main(String[] args) {
		System.out.println("I am running");
		new avgClearing().cli(args);
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
		dor = (DomainObjectReader) SpringApplicationContext.getBean("reader");
		timeslotRepo = (TimeslotRepo) getBean("timeslotRepo");
		timeService = (TimeService) getBean("timeService");
		brokerRepo = (BrokerRepo) SpringApplicationContext
				.getBean("brokerRepo");
		registerNewObjectListener(new CompetitionHandler(), Competition.class);
		registerNewObjectListener(new BrokerHandler(), Broker.class);
		registerNewObjectListener(new TimeslotUpdateHandler(), TimeslotUpdate.class);
		registerNewObjectListener(new MarketTransactionHandler(), MarketTransaction.class);
	
		registerNewObjectListener(new TimeslotHandler(), Timeslot.class);

		registerNewObjectListener(new OrderbookHandler(), Orderbook.class);
		registerNewObjectListener(new ClearedTradeHandler(), ClearedTrade.class);

		
		
		ignoreCount = ignoreInitial;
		data = new TreeMap<Integer, ClearedTrade[]>();
		marketData = new TreeMap<Integer, averages>();
		try {
			output = new PrintWriter(new File(dataFilename));
			FileWriter fw = new FileWriter(dataFilename, true);
			output = new PrintWriter(new BufferedWriter(fw));
			debug =  new PrintWriter(new File(brokername+"_volume.txt"));
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
		averages yesterdayData;
		averages prevOneWeekData;
		averages prevHourData;
		
		//averages yesterdayAvg;
		time = time - 360;
		for(int i = 0; i < 24; i++){
			avgClearingPrice[i] = avgClearingPrice[i] / auctionCount[i];
			clearedVolumeCount[i] = clearedVolumeCount[i] / clearedTradeCount[i];
		}
		for(int x = 0; x < avgClearingPrice.length-1;x++){
			output.format("%d, %.2f",(x+1), avgClearingPrice[x]);
			output.println();
			debug.format("%d, %.2f",(x+1), clearedVolumeCount[x]);
			debug.println();
		}
		
		
		output.close(); 
		debug.close();
		System.out.println("Finished");

	}

	// -----------------------------------
	// catch ClearedTrade messages
	class ClearedTradeHandler implements NewObjectListener {

		@Override
		public void handleNewObject(Object thing) {
			if (ignoreCount > 0) {
				return; // nothing to do yet
			}
			ClearedTrade ct = (ClearedTrade) thing;
			
			int target = ct.getTimeslot().getSerialNumber();
			int now = timeslotRepo.getTimeslotIndex(timeService
					.getCurrentTime());
			int offset = target - now - indexOffset;
			if (offset < 0 || offset > 23) {
				// problem
				//log.error("ClearedTrade index error: " + offset);
			} else {
						
				ClearedTrade[] targetArray = data.get(target);
				if (null == targetArray) {
					targetArray = new ClearedTrade[24];
					data.put(target, targetArray);
				}
				targetArray[offset] = ct;
			}
		}
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
			//System.out.println("brokerid " + mt.getBroker().getId());
			if (mt.getBroker().getId() == brokerID) {
				int target = mt.getPostedTimeslot().getSerialNumber();// getTimeslot().getSerialNumber();
				
				averages cmt = marketData.get(target);
				if (null == cmt) {
					cmt = new averages();
				}
				
				int currenttimeslot = timeslotRepo.currentSerialNumber();
				int timeslotSerial = mt.getTimeslot().getSerialNumber();
				int offset = Math.abs(timeslotSerial - currenttimeslot);
				System.out.println("MT timeslotSerial : " + timeslotSerial+ " Currenttimeslot: " + currenttimeslot + "Counter : " + offset + " PostedT : " + mt.getPostedTimeslot().getSerialNumber() + " ClearingV : " + mt.getMWh() + " ClearP : " + mt.getPrice());
				
				clearedVolumeCount[offset]+= mt.getMWh();
				
				if(currentTimeslotflag != currenttimeslot || currentHourAheadflag != offset)
				{
					clearedTradeCount[offset]++;
					//System.out.println("Offset : " + offset + "Trade counted "+ clearedTradeCount[offset]);
					currentHourAheadflag = offset;
					currentTimeslotflag = currenttimeslot;
				}
				cmt.timeslotIndex = target;
				// trade count
				cmt.count += 1;
				if (mt.getMWh() > 0) {
					cmt.energyCleared += mt.getMWh();
				}
				
				//System.out.println("Posted timeslot: " + mt.getPostedTimeslot());
				marketData.put(target, cmt);
				
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
				int timeslotSerial = timeslotRepo.currentSerialNumber();	
				averages cmt = marketData.get(timeslotSerial);
				int dayOfWeek = timeslotRepo.currentTimeslot().dayOfWeek();
				int dayHour = timeslotRepo.currentTimeslot().slotInDay();
				counter = 0;
				if (null == cmt) {
					cmt = new averages();
				}
				cmt.day = dayOfWeek;
				cmt.hour = dayHour;
				marketData.put(timeslotSerial, cmt);

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
				int timeslotSerial = ob.getTimeslotIndex();
				
				int currenttimeslot = timeslotRepo.currentSerialNumber();
				
				averages cmt = marketData.get(timeslotSerial);
				if (null == cmt) {
					cmt = new averages();		
				}
				if (ob.getClearingPrice() == null) {
					avgClearingPrice[counter] += 0;
				} else {
					auctionCount[counter]++;
					avgClearingPrice[counter] += ob.getClearingPrice();
					}
				time = timeslotSerial;
				
				//System.out.println("Orderbook timeslotSerial : " + timeslotSerial+ " Currenttimeslot: " + currenttimeslot + "Counter : " + counter + " Clearingprice : " + ob.getClearingPrice());
				
				counter++;
				
				orderbookCounter.put(timeslotSerial, counter);
				marketData.put(timeslotSerial, cmt);
				

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
			if (username.equalsIgnoreCase(brokername)) {
				brokerID = broker.getId();
				System.out.println("brokerid "+ brokerID);
			}
		}
	}

	
	class TimeslotHandler implements NewObjectListener {

		public void handleNewObject(Object thing) {
			if (ignoreCount > 0) {
				return;
			}
			Timeslot ts = (Timeslot) thing;
			int target = ts.getSerialNumber();
			averages cmt = marketData.get(target);
			if (null == cmt) {
				cmt = new averages();

			}
			marketData.put(target, cmt);
		}
	}

}

class averages {
	int timeslotIndex;
	int count;
	double rate;
	int day;
	int hour;
	double energyCleared;

	averages() {
		timeslotIndex = 0;
		count = 0;
		rate = 0.0;
		day = 0;
		hour = 0;
		energyCleared = 0.0;
	}
}