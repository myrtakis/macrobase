package alexp.macrobase.outlier.lof.chen;

import alexp.macrobase.outlier.MultiMetricClassifier;
import com.github.chen0040.data.frame.DataFrame;
import com.github.chen0040.data.frame.DataRow;

import java.util.ArrayList;
import java.util.List;
import java.util.function.BiFunction;

import alexp.macrobase.utils.DataFrameUtils;
import java.util.Arrays;


/**
 * Created by xschen on 23/8/15.
 */
public class LOCI extends MultiMetricClassifier {
    private double r_max;

    private double alpha = 0.5;
    private double kSigma = 3;

    private double[][] distanceMatrix;

    private BiFunction<DataRow, DataRow, Double> distanceMeasure;

    private edu.stanford.futuredata.macrobase.datamodel.DataFrame output;

    public LOCI(String columnName) {
        super(columnName);
    }

    public LOCI(String[] columns) {
        super(columns);
    }


    @Override
    public void process(edu.stanford.futuredata.macrobase.datamodel.DataFrame input) throws Exception {
        DataFrame inputRows = DataFrameUtils.toRowDataFrame(input, columns);

        fitAndTransform(inputRows);

        output = input.copy();

        double[] resultColumn = new double[input.getNumRows()];
        Arrays.fill(resultColumn, 0.0);

        List<List<Integer>> D = new ArrayList<>();

        for (int i = 0; i < input.getNumRows(); ++i) {
            List<Integer> D_i = get_r_neighbors(i, r_max, distanceMatrix);
            D.add(D_i);
        }

        for (int i = 0; i < input.getNumRows(); ++i) {
            List<Integer> D_i = D.get(i);
            int n = D_i.size();
            for (int j = 0; j < n; ++j) {
                double r = distanceMatrix[i][D_i.get(j)];
                double alphar = alpha * r;
                int n_pi_alphar = get_alphar_neighbor_count(i, alphar, D_i, distanceMatrix);
                double nhat_pi_r_alpha = get_nhat_pi_r_alpha(i, alpha, r, D, distanceMatrix);
                double sigma_nhat_pi_r_alpha = get_sigma_nhat_pi_r_alpha(i, alpha, r, D, distanceMatrix, nhat_pi_r_alpha);
                double MDEF = 1 - n_pi_alphar / nhat_pi_r_alpha;
                double sigma_MDEF = sigma_nhat_pi_r_alpha / nhat_pi_r_alpha;

                if (MDEF > kSigma * sigma_MDEF) {
                    resultColumn[i] = 1.0;
                    break;
                }
            }
        }

        output.addColumn(outputColumnName, resultColumn);
    }

    @Override
    public edu.stanford.futuredata.macrobase.datamodel.DataFrame getResults() {
        return output;
    }

    public void fitAndTransform(DataFrame batch) {
        int m = batch.rowCount();

        distanceMatrix = new double[m][];
        for (int i = 0; i < m; ++i) {
            distanceMatrix[i] = new double[m];
        }

        double maxDistance = Double.MIN_VALUE;
        for (int i = 0; i < m; ++i) {
            DataRow tuple_i = batch.row(i);
            for (int j = i + 1; j < m; ++j) {
                DataRow tuple_j = batch.row(j);
                double distance = DistanceMeasureService.getDistance(batch, tuple_i, tuple_j, distanceMeasure);
                distanceMatrix[i][j] = distance;
                distanceMatrix[j][i] = distance;
                maxDistance = Math.max(maxDistance, distance);
            }
        }

        r_max = maxDistance / alpha;

    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public void setkSigma(double kSigma) {
        this.kSigma = kSigma;
    }

    private double get_sigma_nhat_pi_r_alpha(int i, double alpha, double r, List<List<Integer>> D, double[][] distanceMatrix, double n_hat) {
        List<Integer> D_i = D.get(i);
        int n_pi_r = D_i.size() + 1; // including i itself
        double alphar = alpha * r;
        double sum = 0;
        for (Integer j : D_i) {
            sum += Math.pow(get_alphar_neighbor_count(j, alphar, D.get(j), distanceMatrix) - n_hat, 2);
        }
        return Math.sqrt(sum / n_pi_r);
    }

    private double get_nhat_pi_r_alpha(int i, double alpha, double r, List<List<Integer>> D, double[][] distanceMatrix) {
        List<Integer> D_i = D.get(i);
        int n_pi_r = D_i.size() + 1; // including i itself
        double alphar = alpha * r;
        double sum = 0;
        for (Integer j : D_i) {
            sum += get_alphar_neighbor_count(j, alphar, D.get(j), distanceMatrix);
        }
        return sum / n_pi_r;
    }

    private int get_alphar_neighbor_count(int i, double alphar, List<Integer> d_i, double[][] distanceMatrix) {
        int count = 1; // including i itself
        for (Integer j : d_i) {
            double distance = distanceMatrix[i][j];
            if (distance < alphar) {
                count++;
            }
        }

        return count;
    }

    private List<Integer> get_r_neighbors(int i, double r, double[][] distanceMatrix) {
        int m = distanceMatrix.length;
        List<Integer> rnn = new ArrayList<Integer>();
        for (int j = 0; j < m; ++j) {
            if (i == j) continue;
            double distance = distanceMatrix[i][j];
            if (distance < r) {
                rnn.add(j);
            }
        }

        return rnn;
    }
}
