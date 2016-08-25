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
import java.util.HashMap;

/**
 * Created by russell on 8/24/16.
 */
public class LumpPerformanceAnalysis extends LogtoolContext implements Analyzer
{
    static private Logger log = Logger.getLogger(LumpPerformanceAnalysis.class.getName());

    // data output file
    private PrintWriter recordEuro;
    private PrintWriter recordEnergy;
    private String filenameEuro = "recordEuro.txt";
    private String filenameEnergy = "recordEnergy.txt";
    private PrintWriter recordEuroNorm;
    private PrintWriter recordEnergyNorm;
    private String filenameEuroNorm = "recordEuro.txt";
    private String filenameEnergyNorm = "recordEnergy.txt";


    /**
     * Structure that holds the information of a single brokers performance
     */
    private class PerformanceByCategory {
        private double wholesaleGainEnergy;
        private double wholesaleGainEuros;
        private double wholesaleLossEnergy;
        private double wholesaleLossEuros;
        private double tariffGainEnergy;
        private double tariffGainEuros;
        private double tariffLossEnergy;
        private double tariffLossEuros;
        private double tariffPeriodicEuros;
        private double balanceGainEnergy;
        private double balanceGainEuros;
        private double balanceLossEnergy;
        private double balanceLossEuros;
        private double capacityEnergy;
        private double capacityEuros;
        private double distributionEnergy;
        private double distributionEuros;

        private PerformanceByCategory()
        {
            wholesaleGainEnergy = 0.0;
            wholesaleGainEuros = 0.0;
            wholesaleLossEnergy = 0.0;
            wholesaleLossEuros = 0.0;
            tariffGainEnergy = 0.0;
            tariffGainEuros = 0.0;
            tariffLossEnergy = 0.0;
            tariffLossEuros = 0.0;

            balanceGainEnergy = 0.0;
            balanceGainEuros = 0.0;
            balanceLossEnergy = 0.0;
            balanceLossEuros = 0.0;

            capacityEnergy = 0.0;
            capacityEuros = 0.0;
            distributionEnergy = 0.0;
            distributionEuros = 0.0;
        }

        private void updateBalancingGain(double amount, double money)
        {
            balanceGainEnergy += amount;
            balanceGainEuros += money;
        }

        private void updateBalancingLoss(double amount, double money)
        {
            balanceLossEnergy += amount;
            balanceLossEuros += money;
        }

        private void updateWholesaleGain(double amount, double money)
        {
            wholesaleGainEnergy += (amount * 1000);
            wholesaleGainEuros += money;
        }

        private void updateWholesaleLoss(double amount, double money)
        {
            wholesaleLossEnergy += (amount * 1000);
            wholesaleLossEuros += money;
        }

        private void updateTariffConsumption(double amount, double money)
        {
            tariffGainEnergy += amount;
            tariffGainEuros += money;
        }

        private void updateTariffPeriodic(double money)
        {
            tariffPeriodicEuros += money;
        }

        private void updateTariffProduction(double amount, double money)
        {
            tariffLossEnergy += amount;
            tariffLossEuros += money;
        }

        private void updateCapacity(double amount, double money)
        {
            capacityEnergy += amount;
            capacityEuros += money;
        }

        private void updateDistribution(double amount, double money)
        {
            distributionEnergy += amount;
            distributionEuros += money;
        }

        private String getPerformanceEuros()
        {
            String euros = "";
            euros += tariffGainEuros + ",";
            euros += tariffPeriodicEuros + ",";
            euros += tariffLossEuros + ",";
            euros += wholesaleGainEuros + ",";
            euros += wholesaleLossEuros + ",";
            euros += balanceGainEuros + ",";
            euros += balanceLossEuros + ",";
            euros += capacityEuros + ",";
            euros += distributionEuros + ",";
            return euros;
        }

        private String getPerformanceEnergy()
        {
            String energy = "";
            energy += tariffGainEnergy + ",";
            energy += tariffLossEnergy + ",";
            energy += wholesaleGainEnergy + ",";
            energy += wholesaleLossEnergy + ",";
            energy += balanceGainEnergy + ",";
            energy += balanceLossEnergy + ",";
            return energy;
        }

        private String getNormalizedEuros()
        {
            String euros = "";
            euros += Math.log(tariffGainEuros) + ",";
            euros += Math.log(tariffPeriodicEuros) + ",";
            euros += Math.log(tariffLossEuros) + ",";
            euros += Math.log(wholesaleGainEuros) + ",";
            euros += Math.log(wholesaleLossEuros) + ",";
            euros += Math.log(balanceGainEuros) + ",";
            euros += Math.log(balanceLossEuros) + ",";
            euros += Math.log(capacityEuros) + ",";
            euros += Math.log(distributionEuros) + ",";
            return euros;
        }

        private String getNormalizedEnergy()
        {
            String energy = "";
            energy += normalize(tariffGainEnergy) + ",";
            energy += normalize(tariffLossEnergy) + ",";
            energy += normalize(wholesaleGainEnergy) + ",";
            energy += normalize(wholesaleLossEnergy) + ",";
            energy += normalize(balanceGainEnergy) + ",";
            energy += normalize(balanceLossEnergy) + ",";
            return energy;
        }

        private double normalize(double n)
        {
            if (n > 0.0)
                return Math.log(n);
            else if (n < 0.0)
                return -Math.log(-n);
            else
                return 0.0;
        }
    }

    // Holds the entire performance
    private HashMap<Broker, PerformanceByCategory> performance;

    /**
     * Main method just creates an instance and passes command-line args to its
     * inherited cli() method.
     */
    public static void main(String[] args) {
        System.out.println("Starting Analysis");
        new LumpPerformanceAnalysis().cli(args);
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
        try {
            recordEuro = new PrintWriter(new File(filenameEuro));
            recordEuroNorm = new PrintWriter(new File(filenameEuroNorm));
            recordEnergy = new PrintWriter(new File(filenameEnergy));
            recordEnergyNorm = new PrintWriter(new File(filenameEnergyNorm));
        }
        catch (FileNotFoundException e) {
            log.error("Cannot open file " + filenameEuro);
            log.error("Cannot open file " + filenameEuroNorm);
            log.error("Cannot open file " + filenameEnergy);
            log.error("Cannot open file " + filenameEnergyNorm);
        }
        performance = new HashMap<>();
    }

    @Override
    public void report ()
    {
        printOutput(recordEnergy, "kWh", false);
        printOutput(recordEnergyNorm, "kWh", true);
        printOutput(recordEuro, "$", false);
        printOutput(recordEuroNorm, "$", true);
    }

    private void printOutput(PrintWriter pw, String u, boolean norm)
    {
        if (norm)
            u = "log_10("+u+")";
        pw.print("Broker Name,");
        pw.print("Tariff Gains ("+u+"),");
        if (u.equals("$"))
            pw.print("Tariff Periodic Gains ("+u+"),");
        pw.print("Tariff Losses ("+u+"),");
        pw.print("Wholesale Gains ("+u+"),");
        pw.print("Wholesale Losses ("+u+"),");
        pw.print("Balancing Gains ("+u+"),");
        pw.print("Balancing Losses ("+u+"),");
        if (u.equals("$"))
        {
            pw.print("Capacity Losses (" + u + "),");
            pw.print("Distribution Losses (" + u + "),");
        }
        pw.println();

        for (Broker broker : performance.keySet())
        {
            pw.print(broker + ",");
            String results;
            if (u.equals("$"))
                if (norm)
                    results = performance.get(broker).getNormalizedEuros();
                else
                    results = performance.get(broker).getPerformanceEuros();
            else
            if (norm)
                results = performance.get(broker).getNormalizedEnergy();
            else
                results = performance.get(broker).getPerformanceEnergy();
            pw.print(results);
            pw.println();
        }
        pw.close();
    }

    private class BrokerHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject(Object thing)
        {
            Broker broker = (Broker) thing;
            performance.put(broker, new PerformanceByCategory());
        }
    }

    private class TariffTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject(Object thing)
        {
            TariffTransaction tx = (TariffTransaction)thing;
            Broker broker = tx.getBroker();
            double amount = tx.getKWh();
            double money = tx.getCharge();

            // only concerned with consumption / production
            PerformanceByCategory perf = performance.get(broker);
            if (tx.getTxType() == TariffTransaction.Type.PRODUCE)
                perf.updateTariffProduction(amount, money);
            else if (tx.getTxType() == TariffTransaction.Type.CONSUME)
                perf.updateTariffConsumption(amount, money);
            else if (tx.getTxType() == TariffTransaction.Type.PERIODIC)
                perf.updateTariffPeriodic(money);
            performance.put(broker, perf);
        }
    }

    private class BalancingTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject (Object thing)
        {
            BalancingTransaction tx = (BalancingTransaction)thing;
            Broker broker = tx.getBroker();
            double amount = tx.getKWh();
            double money = tx.getCharge();
            PerformanceByCategory perf = performance.get(broker);
            if (money < 0.0)
                perf.updateBalancingLoss(amount, money);
            else
                perf.updateBalancingGain(amount, money);
            performance.put(broker, perf);
        }
    }

    private class MarketTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject(Object thing)
        {
            MarketTransaction tx = (MarketTransaction)thing;
            Broker broker = tx.getBroker();
            double amount = tx.getMWh();
            double money = tx.getPrice();
            // check if a valid broker...
            if (performance.containsKey(broker))
            {
                PerformanceByCategory perf = performance.get(broker);
                if (money < 0.0)
                    perf.updateWholesaleLoss(amount, money);
                else
                    perf.updateWholesaleGain(amount, money);
                performance.put(broker, perf);
            }
        }
    }

    private class DistributionTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject(Object thing)
        {
            DistributionTransaction tx = (DistributionTransaction)thing;
            Broker broker = tx.getBroker();
            double amount = tx.getKWh();
            double money = tx.getCharge();
            PerformanceByCategory perf = performance.get(broker);
            perf.updateDistribution(amount, money);
            performance.put(broker, perf);
        }
    }

    private class CapacityTxHandler implements NewObjectListener
    {
        @Override
        public void handleNewObject(Object thing)
        {
            CapacityTransaction tx = (CapacityTransaction)thing;
            Broker broker = tx.getBroker();
            double amount = tx.getKWh();
            double money = tx.getCharge();
            PerformanceByCategory perf = performance.get(broker);
            perf.updateCapacity(amount, money);
            performance.put(broker, perf);
        }
    }
}
