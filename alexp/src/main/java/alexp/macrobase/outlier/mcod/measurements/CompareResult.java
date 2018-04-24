package alexp.macrobase.outlier.mcod.measurements;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.util.HashSet;

public class CompareResult {

    public static void main(String[] args) throws IOException {

        double[] result = compare("C:\\Users\\Luan\\workspace\\MTree\\mesiWithHash500dimension.txt", "C:\\Users\\Luan\\workspace\\MTree\\micro500dimension.txt");
        System.out.println("Precison: " + result[0]);
        System.out.println("Recall: " + result[1]);

//        result = compare("C:\\Users\\Luan\\MTree\\abstractC50_20.txt", "C:\\Users\\Luan\\MTree\\trueErrorDetection_1000.txt");
//        System.out.println("Precison: "+result[0]);
//        System.out.println("Recall: "+result[1]);
    }

    public static double[] compare(String filename1, String filename2) throws IOException {

        BufferedReader approx = new BufferedReader(new FileReader(new File(filename1)));

        BufferedReader exact = new BufferedReader(new FileReader(new File(filename2)));

        HashSet<Integer> approxValues = new HashSet<Integer>();

        HashSet<Integer> exactValues = new HashSet<Integer>();

        String line = "";
        while ((line = approx.readLine()) != null) {
            approxValues.add(Integer.valueOf(line.trim()));

        }
        line = "";
        while ((line = exact.readLine()) != null) {
            exactValues.add(Integer.valueOf(line.trim()));

        }

        double precision = 0;
        double recall = 0;

        for (Integer i : approxValues) {

            if (exactValues.contains(i))
                precision++;
        }
        for (Integer i : exactValues) {
            if (approxValues.contains(i))
                recall++;
        }


        /**
         * print confusion matrix
         */

        double cf2 = precision / exactValues.size();
        double cf1 = 1 - cf2;
        double cf4 = (approxValues.size() - precision) / (1302 - exactValues.size());
        double cf3 = 1 - cf4;
        System.out.println(String.format("%.2f", cf1) + " " + String.format("%.2f", cf2));

        System.out.println(String.format("%.2f", cf3) + " " + String.format("%.2f", cf4));

        precision = precision * 1.0 / approxValues.size();
        recall = recall * 1.0 / exactValues.size();
        System.out.println("F1 = " + (2 * precision * recall) / (precision + recall));
        return new double[]{precision, recall};
    }
}
