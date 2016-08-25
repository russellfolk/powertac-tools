/*
 * Copyright (c) 2015 by the original author
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.powertac.logtool.example;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

import org.apache.log4j.Logger;
import org.powertac.common.Broker;
import org.powertac.common.CustomerInfo;
import org.powertac.common.TariffTransaction;
import org.powertac.common.enumerations.PowerType;
import org.powertac.common.msg.TimeslotUpdate;
import org.powertac.common.repo.BrokerRepo;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;

/**
 * This class will analyze tariffs by general type (Consumption, Electric
 * Vehicle, etc.) and determine their profitability. Output is broken down by
 * broker with the tariff type, usage, money made, and customers serviced given.
 *
 * First line outputs the type of statistics: customer counts, profit, usage
 * Continuing Lines break out the statistics by broker and power type.
 *
 * Note that customer count shows the total number of customers that signed up
 * over the course of a game. This will not show customers per time-slot.
 *
 * @author Russell Folk
 */
public class ProfitPerTariffType
        extends LogtoolContext
        implements Analyzer
{
    /**
      * This class merely gives a storage machanism by which to contain the
      * desired statistics.
      */
    private class StatsTracked
    {
        int customers;
        double profit;
        double usage;

        StatsTracked()
        {
            this.customers = 0;
            this.profit = 0.0;
            this.usage = 0.0;
        }

        void updateCustomers(int customers) { this.customers += customers; }
        void updateProfit(double profit) { this.profit += profit; }
        void updateUsage(double usage) { this.usage += usage; }

        boolean isUntouched()
        {
            return customers == 0 && profit == 0.0 && usage == 0.0;
        }

        public String toString()
        {
            return customers + "," + usage + "," + profit; 
        }
    }
    static private Logger log = Logger.getLogger(ProfitPerTariffType.class.getName());

    private BrokerRepo brokerRepo;

    // list of TariffTransactions for current timeslot
    private ArrayList<TariffTransaction> ttx;
    private HashMap<Broker, HashMap<PowerType, StatsTracked>> stats;

    // customer count by powertype
    private HashMap<PowerType, Integer> customerByType;

    private ArrayList<PowerType> powerTypes;

    // output array, indexed by timeslot
    private ArrayList<Broker> brokers = null;

    // data output file
    private PrintWriter data = null;
    private String dataFilename = "data.txt";

    /**
     * Constructor does nothing. Call setup() before reading a file to
     * get this to work.
     */
    private ProfitPerTariffType ()
    {
        super();
    }

    /**
     * Main method just creates an instance and passes command-line args to
     * its inherited cli() method.
     */
    public static void main (String[] args)
    {
        new ProfitPerTariffType().cli(args);
    }

    /**
     * Takes two args, input filename and output filename
     */
    private void cli (String[] args)
    {
        if (args.length != 2) {
            System.out.println("Usage: <analyzer> input-file output-file");
            return;
        }
        dataFilename = args[1];
        super.cli(args[0], this);
    }

    /**
     * Creates data structures, opens output file. It would be nice to dump
     * the broker names at this point, but they are not known until we hit the
     * first timeslotUpdate while reading the file.
     */
    @Override
    public void setup ()
    {
        DomainObjectReader dor;

        dor = (DomainObjectReader) SpringApplicationContext.getBean("reader");
        brokerRepo = (BrokerRepo) SpringApplicationContext.getBean("brokerRepo");
        ttx = new ArrayList<>();

        dor.registerNewObjectListener(new TimeslotUpdateHandler(),
                TimeslotUpdate.class);
        dor.registerNewObjectListener(new TariffTxHandler(),
                TariffTransaction.class);
        dor.registerNewObjectListener(new CustomerInfoHandler(), CustomerInfo.class);

        try {
            data = new PrintWriter(new File(dataFilename));
        }
        catch (FileNotFoundException e) {
            log.error("Cannot open file " + dataFilename);
        }

        powerTypes = preparePowerTypes();
        customerByType = new HashMap<>();
        for (PowerType type : powerTypes)
            customerByType.put(type, 0);
    }

    @Override
    public void report ()
    {
        for (Broker broker : brokers)
        {
            for (PowerType type : powerTypes)
            {
                StatsTracked theseStats = stats.get(broker).get(type);
                if (!theseStats.isUntouched())
                {
                    data.print(broker + ",");
                    data.print(type + ",");
                    data.println(theseStats);
                }
            }
        }

        for (PowerType type : powerTypes)
        {
            data.print(type + ",");
            int customerCount = customerByType.get(type);
            data.print(customerCount + ",");
            data.println(",,");
        }

        data.close();
    }

    // Called on timeslotUpdate. Note that there are two of these before
    // the first "real" timeslot. Incoming tariffs are published at the end of
    // the second timeslot (the third call to this method), and so customer
    // consumption against non-default broker tariffs first occurs after
    // four calls.
    private void summarizeTimeslot ()
    {
        if (null == brokers) {
            // first time through
            brokers = new ArrayList<>();
            stats = new HashMap<>();

            data.print("Broker Name,");
            data.print("Power Type,");
            data.print("Customers,");
            data.print("Usage (kWh),");
            data.println("Income Gained");

            // set up the maps of brokers, and stats by power type
            for (Broker broker : brokerRepo.findRetailBrokers())
            {
                brokers.add(broker);
                stats.put(broker, new HashMap<>());
                for (PowerType type : powerTypes)
                {
                    StatsTracked theseStats = new StatsTracked();
                    HashMap<PowerType, StatsTracked> map = stats.get(broker);
                    map.put(type, theseStats);
                    stats.put(broker, map);
                }
            }

            // store customer statistics

        }

        if (ttx.size() > 0) {
            // there are some signups and withdraws here
            for (TariffTransaction tx : ttx) {
                Broker broker = tx.getBroker();
                PowerType type = tx.getTariffSpec().getPowerType();
                HashMap<PowerType, StatsTracked> brokerMap = stats.get(broker);
                StatsTracked theseStats = brokerMap.get(type);
                if (tx.getTxType() == TariffTransaction.Type.CONSUME ||
                        tx.getTxType() == TariffTransaction.Type.PRODUCE) {
                    theseStats.updateUsage(tx.getKWh());
                    theseStats.updateProfit(tx.getCharge());
                }
                else if (tx.getTxType() == TariffTransaction.Type.SIGNUP)
                    theseStats.updateCustomers(tx.getCustomerCount());

                // reupdate the tracking...
                brokerMap.put(type, theseStats);
                stats.put(broker, brokerMap);
            }
        }
        ttx.clear();
    }

    // -----------------------------------
    // catch TariffTransactions
    private class TariffTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject (Object thing)
        {
            TariffTransaction tx = (TariffTransaction)thing;
            // only include SIGNUP and WITHDRAW
            if (tx.getTxType() == TariffTransaction.Type.SIGNUP ||
                    tx.getTxType() == TariffTransaction.Type.CONSUME ||
                    tx.getTxType() == TariffTransaction.Type.PRODUCE) {
                ttx.add(tx);
            }
        }
    }

    // -----------------------------------
    // catch TimeslotUpdate events
    class TimeslotUpdateHandler implements NewObjectListener
    {

        @Override
        public void handleNewObject (Object thing)
        {
            summarizeTimeslot();
        }
    }

    private class CustomerInfoHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject (Object thing)
        {
            CustomerInfo ci = (CustomerInfo) thing;
            PowerType pt = ci.getPowerType();
            int cc = ci.getPopulation() + customerByType.get(pt);
            customerByType.put(pt, cc);
            System.out.println("Processing: " + ci.getId() + "--" + ci + " power " + pt + " count " + cc);
        }
    }

    private ArrayList<PowerType> preparePowerTypes()
    {
        ArrayList<PowerType> types = new ArrayList<>();
        types.add(PowerType.BATTERY_STORAGE);
        types.add(PowerType.CHP_PRODUCTION);
        types.add(PowerType.CONSUMPTION);
        types.add(PowerType.ELECTRIC_VEHICLE);
        types.add(PowerType.FOSSIL_PRODUCTION);
        types.add(PowerType.INTERRUPTIBLE_CONSUMPTION);
        types.add(PowerType.PRODUCTION);
        types.add(PowerType.PUMPED_STORAGE_PRODUCTION);
        types.add(PowerType.RUN_OF_RIVER_PRODUCTION);
        types.add(PowerType.SOLAR_PRODUCTION);
        types.add(PowerType.STORAGE);
        types.add(PowerType.THERMAL_STORAGE_CONSUMPTION);
        types.add(PowerType.WIND_PRODUCTION);
        return types;
    }

}