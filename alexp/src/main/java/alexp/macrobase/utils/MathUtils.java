package alexp.macrobase.utils;

public class MathUtils {

    public static double middle(double[] sortedArr) {
        if (sortedArr.length % 2 == 0) {
            return (sortedArr[sortedArr.length / 2 - 1] + sortedArr[sortedArr.length / 2]) / 2;
        } else {
            return sortedArr[(int) Math.ceil(sortedArr.length / 2)];
        }
    }
}
