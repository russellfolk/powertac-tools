package org.powertac.logtool.russell;

import org.powertac.common.enumerations.PowerType;

/**
 * This class will hold all the metrics for a broker: energy in, energy out, incomes, and expenses per transaction type.
 * @author Russell Folk
 */
public class BrokerMetrics
{
    /**
     * This enum specifies if we want the energy values of the monetary values.
     */
    public enum ValueType
    {
        ENERGY, MONEY
    }

    /**
     * This subclass will hold a single type of metric and will be used by all the types: e.g. each tariff type,
     * wholesale, balancing, etc.
     */
    public class Metric
    {
        private String name;
        private double income;
        private double expense;
        private double energyIn;
        private double energyOut;

        public Metric(String n)
        {
            name = n;
            income = 0.0;
            expense = 0.0;
            energyIn = 0.0;
            energyOut = 0.0;
        }

        public void updateValues(double energy, double amount)
        {
            // Sold energy, made money
            if (amount > 0)
                income += amount;
            else
                expense += amount;
            if (energy > 0)
                energyIn += energy;
            else
                energyOut += energy;
        }

        public void updateValues(double amount)
        {
            if (amount > 0)
                income += amount;
            else
                expense += amount;
        }

        /**
         * Returns a string formatted for CSV in the form GAINS,LOSS, for use in printing
         * @param v         the value type desired, either energy or money
         * @param normalize whether or not to normalize a value in log_10
         * @return          GAIN,LOSS,
         */
        public String getValues(ValueType v, boolean normalize)
        {
            String result = "";
            switch (v)
            {
                case ENERGY:
                    if (normalize)
                        result += norm(energyIn) + "," +  norm(energyOut) + ",";
                    else
                        result += energyIn + "," + energyOut + ",";
                    break;
                case MONEY:
                    if (normalize)
                        result += norm(income) + "," +  norm(expense) + ",";
                    else
                        result += income + "," + expense + ",";
                    break;
            }
            return result;
        }
        public String getValues(ValueType v) { return getValues(v, false); }

        public String getPerValues(double divisor)
        {
            String result = "";
            result += (income / divisor) + "," + (expense / divisor) + ",";
            return result;
        }

        public double getTotal(ValueType v)
        {
            switch (v)
            {
                case ENERGY:
                    return energyIn + energyOut;
                case MONEY:
                    return income + expense;
            }
            return 0;
        }

        public String getName() { return name; }

        private double norm(double n)
        {
            if (n > 0)
                return Math.log(n);
            else if (n < 0)
                return -Math.log(-n);
            else
                return 0;
        }
    }

    // Tariff Metrics
    Metric tariffConsumption;
    Metric tariffInterruptible;
    Metric tariffElectricVehicle;
    Metric tariffThermalStorage;
    Metric tariffBatteryStorage;
    Metric tariffStorage;
    Metric tariffProduction;
    Metric tariffWind;
    Metric tariffSolar;
    Metric tariffRiver;
    Metric tariffPumped;
    Metric tariffFossil;
    Metric tariffCHP;

    // Other Metrics
    Metric wholesale;
    Metric balancing;
    Metric bank;
    Metric distribution;
    Metric capacity;

    public BrokerMetrics()
    {
        tariffConsumption = new Metric("Generic Consumption Tariff");
        tariffInterruptible = new Metric("Interruptible Consumption Tariff");
        tariffElectricVehicle = new Metric("Electric Vehicle Tariff");
        tariffThermalStorage = new Metric("Thermal Storage (Consumption) Tariff");
        tariffBatteryStorage = new Metric("Battery Storage Tariff");
        tariffStorage = new Metric("Storage Tariff");
        tariffProduction = new Metric("Generic Production Tariff");
        tariffWind = new Metric("Wind Production Tariff");
        tariffSolar = new Metric("Solar Production Tariff");
        tariffRiver = new Metric("Run of River Production Tariff");
        tariffPumped = new Metric("Pumped Storage Production Tariff");
        tariffFossil = new Metric("Fossil Fuel Production Tariff");
        tariffCHP = new Metric("CHP Production Tariff");

        wholesale = new Metric("Wholesale Market");
        balancing = new Metric("Balancing Market");
        bank = new Metric("Bank Interest");
        distribution = new Metric("Energy Distribution Charges");
        capacity = new Metric("Energy Capacity Fees");
    }

    /**
     * This will update the appropriate fields in the specific metric based on the type of tariff.
     *
     * @param type      A type of tariff from the PowerType class
     * @param energy    The amount of energy (kWh) to be updated
     * @param amount    The amount of money (Euros) to be updated
     */
    public void updateTariff(PowerType type, double energy, double amount)
    {
        if (type == PowerType.CONSUMPTION)
            tariffConsumption.updateValues(energy, amount);
        else if (type == PowerType.INTERRUPTIBLE_CONSUMPTION)
            tariffInterruptible.updateValues(energy, amount);
        else if (type == PowerType.ELECTRIC_VEHICLE)
            tariffElectricVehicle.updateValues(energy, amount);
        else if (type == PowerType.THERMAL_STORAGE_CONSUMPTION)
            tariffThermalStorage.updateValues(energy, amount);
        else if (type == PowerType.BATTERY_STORAGE)
            tariffBatteryStorage.updateValues(energy, amount);
        else if (type == PowerType.STORAGE)
            tariffStorage.updateValues(energy, amount);
        else if (type == PowerType.PRODUCTION)
            tariffProduction.updateValues(energy, amount);
        else if (type == PowerType.WIND_PRODUCTION)
            tariffWind.updateValues(energy, amount);
        else if (type == PowerType.SOLAR_PRODUCTION)
            tariffSolar.updateValues(energy, amount);
        else if (type == PowerType.RUN_OF_RIVER_PRODUCTION)
            tariffRiver.updateValues(energy, amount);
        else if (type == PowerType.PUMPED_STORAGE_PRODUCTION)
            tariffPumped.updateValues(energy, amount);
        else if (type == PowerType.FOSSIL_PRODUCTION)
            tariffFossil.updateValues(energy, amount);
        else if (type == PowerType.CHP_PRODUCTION)
            tariffCHP.updateValues(energy, amount);
    }

    public void updateWholesale(double energy, double amount)
    {
        // need to convert energy from MWh to kWh
        // need to get net amount of money $ * MWh
        // need to not cancel out any negatives...
        wholesale.updateValues(energy * 1000, amount * Math.abs(energy));
    }

    public void updateBalancing(double energy, double amount)
    {
        balancing.updateValues(energy, amount);
    }

    public void updateBank(double amount)
    {
        bank.updateValues(amount);
    }

    public void updateDistribution(double amount)
    {
        distribution.updateValues(amount);
    }

    public void updateCapacity(double amount)
    {
        capacity.updateValues(amount);
    }

    public String getPrintHeader()
    {
        String result = "";
        result += tariffConsumption.getName() + " Gains,";
        result += tariffConsumption.getName() + " Losses,";
        result += tariffInterruptible.getName() + " Gains,";
        result += tariffInterruptible.getName() + " Losses,";
        result += tariffElectricVehicle.getName() + " Gains,";
        result += tariffElectricVehicle.getName() + " Losses,";
        result += tariffThermalStorage.getName() + " Gains,";
        result += tariffThermalStorage.getName() + " Losses,";
        result += tariffBatteryStorage.getName() + " Gains,";
        result += tariffBatteryStorage.getName() + " Losses,";
        result += tariffStorage.getName() + " Gains,";
        result += tariffStorage.getName() + " Losses,";
        result += tariffProduction.getName() + " Gains,";
        result += tariffProduction.getName() + " Losses,";
        result += tariffWind.getName() + " Gains,";
        result += tariffWind.getName() + " Losses,";
        result += tariffSolar.getName() + " Gains,";
        result += tariffSolar.getName() + " Losses,";
        result += tariffRiver.getName() + " Gains,";
        result += tariffRiver.getName() + " Losses,";
        result += tariffPumped.getName() + " Gains,";
        result += tariffPumped.getName() + " Losses,";
        result += tariffFossil.getName() + " Gains,";
        result += tariffFossil.getName() + " Losses,";
        result += tariffCHP.getName() + " Gains,";
        result += tariffCHP.getName() + " Losses,";
        result += wholesale.getName() + " Gains,";
        result += wholesale.getName() + " Losses,";
        result += balancing.getName() + " Gains,";
        result += balancing.getName() + " Losses,";
        result += bank.getName() + " Gains,";
        result += bank.getName() + " Losses,";
        result += distribution.getName() + " Gains,";
        result += distribution.getName() + " Losses,";
        result += capacity.getName() + " Gains,";
        result += capacity.getName() + " Losses,";
        return result;
    }
    public String getBrokerMetrics(boolean normalize, ValueType v)
    {
        String result = "";
        result += tariffConsumption.getValues(v, normalize);
        result += tariffInterruptible.getValues(v, normalize);
        result += tariffElectricVehicle.getValues(v, normalize);
        result += tariffThermalStorage.getValues(v, normalize);
        result += tariffBatteryStorage.getValues(v, normalize);
        result += tariffStorage.getValues(v, normalize);
        result += tariffProduction.getValues(v, normalize);
        result += tariffWind.getValues(v, normalize);
        result += tariffSolar.getValues(v, normalize);
        result += tariffRiver.getValues(v, normalize);
        result += tariffPumped.getValues(v, normalize);
        result += tariffFossil.getValues(v, normalize);
        result += tariffCHP.getValues(v, normalize);
        result += wholesale.getValues(v, normalize);
        result += balancing.getValues(v, normalize);
        result += bank.getValues(v, normalize);
        result += distribution.getValues(v, normalize);
        result += capacity.getValues(v, normalize);
        return result;
    }

    public String getBrokerMetricPerkWh(boolean normalize)
    {
        String result = "";
        // Calculate tariff Net
        double netEnergyTariff = 0.0;
        netEnergyTariff += tariffConsumption.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffInterruptible.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffElectricVehicle.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffThermalStorage.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffBatteryStorage.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffStorage.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffProduction.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffWind.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffSolar.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffRiver.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffPumped.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffFossil.getTotal(ValueType.ENERGY);
        netEnergyTariff += tariffCHP.getTotal(ValueType.ENERGY);

        // Calculate Energy Out Net
        double netEnergyOut = 0.0;
        netEnergyOut += tariffConsumption.energyOut;
        netEnergyOut += tariffInterruptible.energyOut;
        netEnergyOut += tariffElectricVehicle.energyOut;
        netEnergyOut += tariffThermalStorage.energyOut;
        netEnergyOut += tariffBatteryStorage.energyOut;
        netEnergyOut += tariffStorage.energyOut;
        netEnergyOut += tariffProduction.energyOut;
        netEnergyOut += tariffWind.energyOut;
        netEnergyOut += tariffSolar.energyOut;
        netEnergyOut += tariffRiver.energyOut;
        netEnergyOut += tariffPumped.energyOut;
        netEnergyOut += tariffFossil.energyOut;
        netEnergyOut += tariffCHP.energyOut;
        netEnergyOut += wholesale.energyOut;
        netEnergyOut += balancing.energyOut;
        netEnergyOut *= -1; // needs to be positive...

        result += tariffConsumption.getPerValues(netEnergyOut);
        result += tariffInterruptible.getPerValues(netEnergyOut);
        result += tariffElectricVehicle.getPerValues(netEnergyOut);
        result += tariffThermalStorage.getPerValues(netEnergyOut);
        result += tariffBatteryStorage.getPerValues(netEnergyOut);
        result += tariffStorage.getPerValues(netEnergyOut);
        result += tariffProduction.getPerValues(netEnergyOut);
        result += tariffWind.getPerValues(netEnergyOut);
        result += tariffSolar.getPerValues(netEnergyOut);
        result += tariffRiver.getPerValues(netEnergyOut);
        result += tariffPumped.getPerValues(netEnergyOut);
        result += tariffFossil.getPerValues(netEnergyOut);
        result += tariffCHP.getPerValues(netEnergyOut);
        result += wholesale.getPerValues(wholesale.energyIn + wholesale.energyOut);
        result += balancing.getPerValues(balancing.energyIn + balancing.energyOut);
        result += bank.getPerValues(netEnergyOut);
        result += distribution.getPerValues(netEnergyOut);
        result += capacity.getPerValues(netEnergyTariff);
        return result;
    }

    public double getTotal(ValueType v)
    {
        double result = 0;
        result += tariffConsumption.getTotal(v);
        result += tariffInterruptible.getTotal(v);
        result += tariffElectricVehicle.getTotal(v);
        result += tariffThermalStorage.getTotal(v);
        result += tariffBatteryStorage.getTotal(v);
        result += tariffStorage.getTotal(v);
        result += tariffProduction.getTotal(v);
        result += tariffWind.getTotal(v);
        result += tariffSolar.getTotal(v);
        result += tariffRiver.getTotal(v);
        result += tariffPumped.getTotal(v);
        result += tariffFossil.getTotal(v);
        result += tariffCHP.getTotal(v);
        result += wholesale.getTotal(v);
        result += balancing.getTotal(v);
        result += bank.getTotal(v);
        result += distribution.getTotal(v);
        result += capacity.getTotal(v);
        return result;
    }
}
