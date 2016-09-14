package org.powertac.logtool.russell;

import org.apache.log4j.Logger;
import org.powertac.common.*;
import org.powertac.common.spring.SpringApplicationContext;
import org.powertac.logtool.LogtoolContext;
import org.powertac.logtool.common.DomainObjectReader;
import org.powertac.logtool.common.NewObjectListener;
import org.powertac.logtool.ifc.Analyzer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.HashMap;

/**
 * Created by russell on 8/24/16.
 */
public class PerformanceAnalysis extends LogtoolContext implements Analyzer
{
    static private Logger log = Logger.getLogger(PerformanceAnalysis.class.getName());

    // data output file
    private PrintWriter recordEuro;
    private PrintWriter recordEnergy;
    private String filenameEuro = "recordEuro.txt";
    private String filenameEnergy = "recordEnergy.txt";
    private PrintWriter recordEuroNorm;
    private PrintWriter recordEnergyNorm;
    private String filenameEuroNorm = "recordEuro.txt";
    private String filenameEnergyNorm = "recordEnergy.txt";
    private PrintWriter recordPer;
    private String filenamePer = "recordPer.txt";

    // Holds the entire performance
    private HashMap<Broker, BrokerMetrics> performance;
    private ArrayList<Broker> brokers;

    /**
     * Main method just creates an instance and passes command-line args to its
     * inherited cli() method.
     */
    public static void main(String[] args) {
        System.out.println("Starting Analysis");
        new PerformanceAnalysis().cli(args);
    }

    /**
     * Takes two args, input filename and output filename
     */
    private void cli(String[] args) {
        if (args.length != 2) {
            System.out.println("Usage: <analyzer> input-file output-file");
            return;
        }
        filenameEuro = "euro_" + args[1];
        filenameEuroNorm = "norm_euro_" + args[1];
        filenameEnergy = "energy_" + args[1];
        filenameEnergyNorm = "energy_norm_" + args[1];
        filenamePer = "per_" + args[1];
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
        dor.registerNewObjectListener(new BrokerHandler(), Broker.class);
        dor.registerNewObjectListener(new TariffTxHandler(), TariffTransaction.class);
        dor.registerNewObjectListener(new BalancingTxHandler(), BalancingTransaction.class);
        dor.registerNewObjectListener(new MarketTxHandler(), MarketTransaction.class);
        dor.registerNewObjectListener(new CapacityTxHandler(), CapacityTransaction.class);
        dor.registerNewObjectListener(new DistributionTxHandler(), DistributionTransaction.class);
        dor.registerNewObjectListener(new BankTxHandler(), BankTransaction.class);
        try {
            recordEuro = new PrintWriter(new File(filenameEuro));
            recordEuroNorm = new PrintWriter(new File(filenameEuroNorm));
            recordEnergy = new PrintWriter(new File(filenameEnergy));
            recordEnergyNorm = new PrintWriter(new File(filenameEnergyNorm));
            recordPer = new PrintWriter(new File(filenamePer));
        }
        catch (FileNotFoundException e) {
            log.error("Cannot open file " + filenameEuro);
            log.error("Cannot open file " + filenameEuroNorm);
            log.error("Cannot open file " + filenameEnergy);
            log.error("Cannot open file " + filenameEnergyNorm);
            log.error("Cannot open file" + filenamePer);
        }
        brokers = new ArrayList<>();
        performance = new HashMap<>();
    }

    @Override
    public void report ()
    {
        printOutput(recordEnergy, false, BrokerMetrics.ValueType.ENERGY);
        printOutput(recordEnergyNorm, true, BrokerMetrics.ValueType.ENERGY);
        printOutput(recordEuro, false, BrokerMetrics.ValueType.MONEY);
        printOutput(recordEuroNorm, true, BrokerMetrics.ValueType.MONEY);
        printOutput(recordPer);
    }

    private void printOutput(PrintWriter pw, boolean normalize, BrokerMetrics.ValueType v)
    {
        // Print first line explanation...
        if (normalize)
            pw.print("Normalized (log_10) ");
        switch (v)
        {
            case ENERGY:
                pw.print("Energy");
                break;
            case MONEY:
                pw.print("Monetary");
                break;
        }
        pw.print(" Statistics for Brokers");

        int numEntries = 1 + 18 * 2 + 1;
        for (int i = 0; i < numEntries; ++i)
            pw.print(",");
        pw.println();

        // print header row
        pw.print("Broker Name,");
        pw.print("Ending Balance,");
        pw.println(performance.get(brokers.get(0)).getPrintHeader());

        // print statistics per broker...
        for (Broker broker : performance.keySet())
        {
            pw.print(broker.getUsername() + ",");
            pw.print(performance.get(broker).getTotal(v) + ",");
            pw.println(performance.get(broker).getBrokerMetrics(normalize, v));
        }
        pw.close();
    }

    private void printOutput(PrintWriter pw)
    {
        pw.println("Price per kWh Statistics for Brokers");
        int numEntries = 1 + 18 * 2 + 1;
        for (int i = 0; i < numEntries; ++i)
            pw.print(",");
        pw.println();

        // print header row
        pw.print("Broker Name,");
        pw.print("Ending Balance,");
        pw.println(performance.get(brokers.get(0)).getPrintHeader());

        // print statistics per broker...
        for (Broker broker : performance.keySet())
        {
            pw.print(broker.getUsername() + ",");
            pw.print((performance.get(broker).getTotal(BrokerMetrics.ValueType.MONEY) /
                      performance.get(broker).getTotal(BrokerMetrics.ValueType.ENERGY)) + ",");
            pw.println(performance.get(broker).getBrokerMetricPerkWh(true));
        }
        pw.close();
    }

    private class BrokerHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject(Object thing)
        {
            Broker broker = (Broker) thing;
            if (!broker.getUsername().toLowerCase().equals("lmp"))
            {
                brokers.add(broker);
                performance.put(broker, new BrokerMetrics());
            }
        }
    }

    private class TariffTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject(Object thing)
        {
            TariffTransaction tx = (TariffTransaction)thing;
            Broker broker = tx.getBroker();
            BrokerMetrics metrics = performance.get(broker);
            metrics.updateTariff(tx.getTariffSpec().getPowerType(), tx.getKWh(), tx.getCharge());
            performance.put(broker, metrics);
        }
    }

    private class MarketTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject(Object thing)
        {
            MarketTransaction tx = (MarketTransaction)thing;
            Broker broker = tx.getBroker();
            // check if a valid broker...
            if (performance.containsKey(broker) && !broker.getUsername().toLowerCase().equals("lmp"))
            {
                BrokerMetrics metrics = performance.get(broker);
                metrics.updateWholesale(tx.getMWh(), tx.getPrice());
                performance.put(broker, metrics);
            }
        }
    }

    private class BalancingTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject (Object thing)
        {
            BalancingTransaction tx = (BalancingTransaction)thing;
            Broker broker = tx.getBroker();
            BrokerMetrics metrics = performance.get(broker);
            metrics.updateBalancing(tx.getKWh(), tx.getCharge());
            performance.put(broker, metrics);
        }
    }

    private class DistributionTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject(Object thing)
        {
            DistributionTransaction tx = (DistributionTransaction)thing;
            Broker broker = tx.getBroker();
            BrokerMetrics metrics = performance.get(broker);
            metrics.updateDistribution(tx.getCharge());
            performance.put(broker, metrics);
        }
    }

    private class CapacityTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject(Object thing)
        {
            CapacityTransaction tx = (CapacityTransaction)thing;
            Broker broker = tx.getBroker();
            BrokerMetrics metrics = performance.get(broker);
            metrics.updateCapacity(tx.getCharge());
            performance.put(broker, metrics);
        }
    }

    private class BankTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject(Object thing)
        {
            BankTransaction tx = (BankTransaction)thing;
            Broker broker = tx.getBroker();
            if (performance.containsKey(broker) && !broker.getUsername().toLowerCase().equals("lmp"))
            {
                BrokerMetrics metrics = performance.get(broker);
                metrics.updateBank(tx.getAmount());
                performance.put(broker, metrics);
            }
        }
    }
}
