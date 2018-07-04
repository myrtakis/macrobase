package alexp.macrobase.outlier.lof.chen;


import alexp.macrobase.outlier.MultiMetricClassifier;
import alexp.macrobase.utils.DataFrameUtils;
import com.github.chen0040.data.frame.DataFrame;
import com.github.chen0040.data.frame.DataRow;
import com.github.chen0040.data.utils.TupleTwo;
import org.apache.commons.math3.linear.RealVector;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.BiFunction;
import java.util.logging.Level;
import java.util.logging.Logger;


/**
 * Created by xschen on 17/8/15.
 * Link:
 */
public class LOF extends MultiMetricClassifier {

    private double threshold = 0.5;

    // min number for minPts;
    private int minPtsLB = 3;
    // max number for minPts;
    private int minPtsUB = 10;

    private boolean parallel = true;
    private boolean automaticThresholding = false;
    private double automaticThresholdingRatio = 0.05;

    private static final Logger logger = Logger.getLogger(String.valueOf(LOF.class));

    private BiFunction<DataRow, DataRow, Double> distanceMeasure;

    private double minScore;
    private double maxScore;

    private DataFrame model;

    private edu.stanford.futuredata.macrobase.datamodel.DataFrame output;

    private int trainSize = 10000;

    public LOF(String columnName) {
        super(columnName);
    }

    public LOF(String[] columns) {
        super(columns);
    }

    @Override
    public void process(edu.stanford.futuredata.macrobase.datamodel.DataFrame input) throws Exception {
        DataFrame inputRows = DataFrameUtils.toRowDataFrame(input, columns);

        DataFrame trainingRows = inputRows;
        if (trainSize < inputRows.rowCount()) {
            trainingRows = DataFrameUtils.toRowDataFrame(input.limit(trainSize), columns);
        }

        fitAndTransform(trainingRows);

        output = input.copy();

        double[] resultColumn = new double[input.getNumRows()];
        for (int i = 0; i < input.getNumRows(); i++) {
            resultColumn[i] = evaluate(inputRows.row(i));
        }

        output.addColumn(outputColumnName, resultColumn);
    }

    @Override
    public edu.stanford.futuredata.macrobase.datamodel.DataFrame getResults() {
        return output;
    }

    protected void adjustThreshold(DataFrame batch){
        int m = batch.rowCount();

        List<Integer> orders = new ArrayList<>();
        List<Double> probs = new ArrayList<>();

        for(int i=0; i < m; ++i){
            DataRow tuple = batch.row(i);
            double prob = evaluate(tuple);
            probs.add(prob);
            orders.add(i);
        }

        final List<Double> probs2 = probs;
        // sort descendingly by probability values
        orders.sort((h1, h2) -> {
            double prob1 = probs2.get(h1);
            double prob2 = probs2.get(h2);
            return Double.compare(prob2, prob1);
        });

        int selected_index = autoThresholdingCaps(orders.size());
        if(selected_index >= orders.size()){
            threshold = probs.get(orders.get(orders.size() - 1));
        }
        else{
            threshold = probs.get(orders.get(selected_index));
        }

    }

    protected int autoThresholdingCaps(int m){
        return Math.max(1, (int) (automaticThresholdingRatio * m));
    }

    public void setSearchRange(int minPtsLB, int minPtsUB) {
        this.minPtsLB = minPtsLB;
        this.minPtsUB = minPtsUB;
    }

    public double getThreshold() {
        return threshold;
    }

    public void setThreshold(double threshold) {
        this.threshold = threshold;
    }

    public boolean isParallel() {
        return parallel;
    }

    public void setParallel(boolean parallel) {
        this.parallel = parallel;
    }

    public boolean isAutomaticThresholding() {
        return automaticThresholding;
    }

    public void setAutomaticThresholding(boolean automaticThresholding) {
        this.automaticThresholding = automaticThresholding;
    }

    public double getAutomaticThresholdingRatio() {
        return automaticThresholdingRatio;
    }

    public void setAutomaticThresholdingRatio(double automaticThresholdingRatio) {
        this.automaticThresholdingRatio = automaticThresholdingRatio;
    }

    public int getTrainSize() {
        return trainSize;
    }

    public void setTrainSize(int trainSize) {
        this.trainSize = trainSize;
    }

    public BiFunction<DataRow, DataRow, Double> getDistanceMeasure() {
        return distanceMeasure;
    }

    public boolean isAnomaly(DataRow tuple) {
        double score_lof = evaluate(tuple);
        return score_lof > threshold;
    }

    private class ScoreTask implements Callable<Double>{
        private DataFrame batch;
        private DataRow tuple;
        public ScoreTask(DataFrame batch, DataRow tuple){
            this.batch = batch;
            this.tuple = tuple;
        }

        public Double call() throws Exception {
            double score = score_lof_sync(batch, tuple);
            return score;
        }
    }

    public DataFrame fitAndTransform(DataFrame batch) {
        this.model = batch.makeCopy();

        int m = model.rowCount();

        minScore = Double.MAX_VALUE;
        maxScore = Double.NEGATIVE_INFINITY;



        if(parallel) {
            ExecutorService executor = Executors.newFixedThreadPool(10);
            List<ScoreTask> tasks = new ArrayList<>();
            for (int i = 0; i < m; ++i) {
                tasks.add(new ScoreTask(model, model.row(i)));
            }

            try {
                List<Future<Double>> results = executor.invokeAll(tasks);
                executor.shutdown();
                for (int i = 0; i < m; ++i) {
                    double score = results.get(i).get();
                    if(Double.isNaN(score)) continue;
                    if(Double.isInfinite(score)) continue;
                    minScore = Math.min(score, minScore);
                    maxScore = Math.max(score, maxScore);
                }
            } catch (InterruptedException | ExecutionException e) {
                e.printStackTrace();
            }
        }else{
            for(int i=0; i < m; ++i){
                double score = score_lof_sync(model, model.row(i));
                if(Double.isNaN(score)) continue;
                if(Double.isInfinite(score)) continue;
                minScore = Math.min(score, minScore);
                maxScore = Math.max(score, maxScore);
            }
        }

        if(automaticThresholding){
            adjustThreshold(model);
        }

        for(int i=0; i < m; ++i){
            DataRow tuple = model.row(i);
            tuple.setCategoricalTargetCell("anomaly", isAnomaly(tuple) ? "1" : "0");
        }

        return this.model;
    }

    private class LOFTask implements Callable<Double>{
        private DataFrame batch;
        private DataRow tuple;
        private int minPts;

        public LOFTask(DataFrame batch, DataRow tuple, int minPts){
            this.batch = batch;
            this.tuple = tuple;
            this.minPts = minPts;
        }

        public Double call() throws Exception {
            double lof = local_outlier_factor(batch, tuple, minPts);
            return lof;
        }
    }

    private double score_lof_sync(DataFrame batch, DataRow tuple){
        double maxLOF = Double.NEGATIVE_INFINITY;

        for(int minPts = minPtsLB; minPts <= minPtsUB; ++minPts) { // the number of nearest neighbors used in defining the local neighborhood of the object.
            double lof = local_outlier_factor(batch, tuple, minPts);
            if(Double.isNaN(lof)) continue;
            maxLOF = Math.max(maxLOF, lof);
        }


        return maxLOF;
    }

    private double score_lof_async(DataFrame batch, DataRow tuple){
        if(!parallel){
            return score_lof_sync(batch, tuple);
        }

        double maxLOF = 0;

        ExecutorService executor = Executors.newFixedThreadPool(Math.min(8, minPtsUB - minPtsLB + 1));

        List<LOFTask> tasks = new ArrayList<>();
        for(int minPts = minPtsLB; minPts <= minPtsUB; ++minPts) { // the number of nearest neighbors used in defining the local neighborhood of the object.
            tasks.add(new LOFTask(batch, tuple, minPts));
        }

        try {
            List <Future<Double>> results = executor.invokeAll(tasks);
            executor.shutdown();
            for(int i=0; i < results.size(); ++i){
                double lof = results.get(i).get();
                if(Double.isNaN(lof)) continue;
                if(Double.isInfinite(lof)) continue;
                maxLOF = Math.max(maxLOF, lof);
            }
        } catch (InterruptedException | ExecutionException e) {
            logger.log(Level.SEVERE, "score_lof_async failed", e);
        }

        return maxLOF;
    }

    public double evaluate(DataRow tuple){
        double score = score_lof_async(model, tuple);

        //logger.info(String.format("score: %f minScore: %f, maxScore: %f", score, minScore, maxScore));

        score -= minScore;
        if(score < 0) score = 0;

        score /= (maxScore - minScore);

        if(score > 1) score = 1;

        return score;
    }

    public double k_distance(DataFrame batch, DataRow o, int k){
        TupleTwo<DataRow, Double> kth = DistanceMeasureService.getKthNearestNeighbor(batch, o, k, distanceMeasure);
        return kth._2();
    }

    private double reach_dist(DataFrame batch, DataRow p, DataRow o, int k){
        double distance_p_o = DistanceMeasureService.getDistance(batch, p, o, distanceMeasure);
        double distance_k_o = k_distance(batch, o, k);
        return Math.max(distance_k_o, distance_p_o);
    }

    private double local_reachability_density(DataFrame batch, DataRow p, int k){
        List<TupleTwo<DataRow, Double>> knn_p = DistanceMeasureService.getKNearestNeighbors(batch, p, k, distanceMeasure);
        double density = local_reachability_density(batch, p, k, knn_p);
        return density;
    }

    private double local_reachability_density(DataFrame batch, DataRow p, int k, List<TupleTwo<DataRow, Double>> knn_p){
        double sum_reach_dist = 0;
        for(TupleTwo<DataRow, Double> o : knn_p){
            sum_reach_dist += reach_dist(batch, p, o._1(), k);
        }
        double density = 1 / (sum_reach_dist / knn_p.size());
        return density;
    }

    // the higher this value, the more likely the point is an outlier
    public double local_outlier_factor(DataFrame batch, DataRow p, int k){

        List<TupleTwo<DataRow, Double>> knn_p = DistanceMeasureService.getKNearestNeighbors(batch, p, k, distanceMeasure);
        double lrd_p = local_reachability_density(batch, p, k, knn_p);
        double sum_lrd = 0;
        for(TupleTwo<DataRow,Double> o : knn_p){
            sum_lrd += local_reachability_density(batch, o._1(), k);
        }

        if(Double.isInfinite(sum_lrd) && Double.isInfinite(lrd_p)){
            return 1.0 / knn_p.size();
        }

        double lof = (sum_lrd / lrd_p) / knn_p.size();

        return lof;
    }
}
