package com.prediction;

public class Instance {
    private double[] attributeValue;
    private double label;

    public Instance(String line) {
        String[] values = line.split("\\t");
        attributeValue = new double[values.length - 1];
        for (int i = 0; i < attributeValue.length; i++) {
            attributeValue[i] = Double.parseDouble(values[i]);
        }
        label = Double.parseDouble(values[values.length - 1]);
    }

    public double[] getAttributeValues() {
        return attributeValue;
    }

    public double getLabel() {
        return label;
    }
}
