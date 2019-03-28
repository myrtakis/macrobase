package alexp.macrobase.outlier;

import alexp.macrobase.utils.DataFrameUtils;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import org.apache.commons.math3.distribution.ChiSquaredDistribution;
import org.apache.commons.math3.distribution.NormalDistribution;
import org.apache.commons.math3.linear.*;

import java.util.*;

public class MinCovDet extends MultiMetricClassifier implements Trainable {
    // H = alpha*(n+p+1), p == dataset dimension
    private double alpha = 0.5;
    private double stoppingDelta = 0.001;

    private RealMatrix cov;
    private RealMatrix inverseCov;

    private RealVector mean;

    private Random random = new Random();

    private DataFrame output;

    private int trainSize = 10000;

    public MinCovDet(String[] columns) {
        super(columns);

        // for now, only handle multivariate case...
        assert (columns.length > 1);
    }

    @Override
    public void train(DataFrame input) {
        train(DataFrameUtils.toRowsRealVector(input, columns));
    }

    private void train(List<RealVector> inputRows) {
        inputRows = inputRows.subList(0, Math.min(trainSize, inputRows.size()));

        int h = (int) Math.floor((inputRows.size() + columns.length + 1) * alpha);

        List<RealVector> initialSubset = chooseKRandom(inputRows, h);

        mean = getMean(initialSubset);

        cov = getCovariance(initialSubset);
        updateInverseCovariance();

        double det = getDeterminant(cov);

        while (true) {
            List<RealVector> newH = findKClosest(h, inputRows);

            mean = getMean(newH);

            cov = getCovariance(newH);
            updateInverseCovariance();

            double newDet = getDeterminant(cov);

            double delta = det - newDet;

            if (newDet == 0 || delta < stoppingDelta) {
                break;
            }

            det = newDet;
        }
    }

    @Override
    public void process(DataFrame input) throws Exception {
        ArrayList<RealVector> inputRows = DataFrameUtils.toRowsRealVector(input, columns);

        if (cov == null) {
            train(inputRows);
        }

        output = input.copy();

        double[] resultColumn = new double[input.getNumRows()];
        for (int i = 0; i < input.getNumRows(); i++) {
            resultColumn[i] = score(inputRows.get(i));
        }

        output.addColumn(outputColumnName, resultColumn);
    }

    @Override
    public DataFrame getResults() {
        return output;
    }

    public double score(RealVector row) {
        return getMahalanobis(mean, inverseCov, row);
    }

    public double getAlpha() {
        return alpha;
    }

    public void setAlpha(double alpha) {
        this.alpha = alpha;
    }

    public double getStoppingDelta() {
        return stoppingDelta;
    }

    public void setStoppingDelta(double stoppingDelta) {
        this.stoppingDelta = stoppingDelta;
    }

    public int getTrainSize() {
        return trainSize;
    }

    public void setTrainSize(int trainSize) {
        this.trainSize = trainSize;
    }

    public void setRandom(Random random) {
        this.random = random;
    }

    public RealMatrix getCovariance() {
        return cov;
    }

    public RealMatrix getInverseCovariance() {
        return inverseCov;
    }

    public RealVector getMean() {
        return mean;
    }

    public double getZScoreEquivalent(double zscore) {
        // compute zscore to CDF
        double cdf = (new NormalDistribution()).cumulativeProbability(zscore);
        // for normal distribution, mahalanobis distance is chi-squared
        // https://en.wikipedia.org/wiki/Mahalanobis_distance#Normal_distributions
        return (new ChiSquaredDistribution(columns.length)).inverseCumulativeProbability(cdf);
    }

    // efficient only when k << allData.size()
    private List<RealVector> chooseKRandom(List<RealVector> allData, final int k) {
        assert (k < allData.size());

        ArrayList<RealVector> ret = new ArrayList<>();
        Set<Integer> chosenIndexes = new HashSet<>();
        while (chosenIndexes.size() < k) {
            int idx = random.nextInt(allData.size());
            if (!chosenIndexes.contains(idx)) {
                chosenIndexes.add(idx);
                ret.add(allData.get(idx));
            }
        }

        assert (ret.size() == k);
        return ret;
    }

    private static Double getMahalanobis(RealVector mean, RealMatrix inverseCov, RealVector vec) {
        final int dim = mean.getDimension();
        double[] vecMinusMean = new double[dim];

        for (int d = 0; d < dim; ++d) {
            vecMinusMean[d] = vec.getEntry(d) - mean.getEntry(d);
        }

        double diagSum = 0, nonDiagSum = 0;

        for (int d1 = 0; d1 < dim; ++d1) {
            for (int d2 = d1; d2 < dim; ++d2) {
                double v = vecMinusMean[d1] * vecMinusMean[d2] * inverseCov.getEntry(d1, d2);
                if (d1 == d2) {
                    diagSum += v;
                } else {
                    nonDiagSum += v;
                }
            }
        }

        return Math.sqrt(diagSum + 2 * nonDiagSum);
    }

    private RealVector getMean(List<RealVector> data) {
        RealVector vec = null;

        for (RealVector d : data) {
            RealVector dvec = new ArrayRealVector(d);
            if (vec == null) {
                vec = dvec;
            } else {
                vec = vec.add(dvec);
            }
        }

        return vec.mapDivide(data.size());
    }

    private List<RealVector> findKClosest(int k, List<RealVector> data) {
        if (data.size() < k) {
            return data;
        }

        Map<RealVector, Double> scoreMap = new HashMap<>(data.size());
        for (RealVector d : data) {
            scoreMap.put(d, getMahalanobis(mean, inverseCov, d));
        }

        data.sort((a, b) -> scoreMap.get(a).compareTo(scoreMap.get(b)));

        return data.subList(0, k);
    }

    private static RealMatrix getCovariance(List<RealVector> data) {
        int rank = data.get(0).getDimension();

        RealMatrix ret = new Array2DRowRealMatrix(data.size(), rank);
        int index = 0;
        for (RealVector d : data) {
            ret.setRow(index, d.toArray());
            index += 1;
        }

        return (new org.apache.commons.math3.stat.correlation.Covariance(ret)).getCovarianceMatrix();
    }

    private static double getDeterminant(RealMatrix cov) {
        return (new LUDecomposition(cov)).getDeterminant();
    }

    private void updateInverseCovariance() {
        try {
            inverseCov = new LUDecomposition(cov).getSolver().getInverse();
        } catch (SingularMatrixException e) {
            inverseCov = new SingularValueDecomposition(cov).getSolver().getInverse();
        }
    }
}
