package alexp.macrobase.outlier.mcod;

import alexp.macrobase.outlier.mcod.mtree.DistanceFunctions;

import java.util.Arrays;
import java.util.Objects;

public class Data implements DistanceFunctions.EuclideanCoordinate, Comparable<Data> {

    public double[] values;
    private final int hashCode;
    public double criterion;

    private int arrivalTime;


    public Data(double... values) {
        this(0, values);
    }

    public Data(int arrivalTime, double... values) {
        this.arrivalTime = arrivalTime;
        this.values = values;

        int hashCode2 = Objects.hash(arrivalTime);
        hashCode2 = 31 * hashCode2 + Arrays.hashCode(values);
        this.hashCode = hashCode2;
    }

    public int arrivalTime() {
        return arrivalTime;
    }


    public void setArrivalTime(int arrivalTime){
        this.arrivalTime = arrivalTime;
    }

    @Override
    public int dimensions() {
        return values.length;
    }

    @Override
    public double get(int index) {
        return values[index];
    }

    @Override
    public int hashCode() {
        return hashCode;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Data data = (Data) o;
        return arrivalTime == data.arrivalTime &&
                Arrays.equals(values, data.values);
    }

    @Override
    public int compareTo(Data that) {
        int dimensions = Math.min(this.dimensions(), that.dimensions());
        for (int i = 0; i < dimensions; i++) {
            double v1 = this.values[i];
            double v2 = that.values[i];
            if (v1 > v2) {
                return +1;
            }
            if (v1 < v2) {
                return -1;
            }
        }

        if (this.dimensions() > dimensions) {
            return +1;
        }

        if (that.dimensions() > dimensions) {
            return -1;
        }

        return 0;
    }

}