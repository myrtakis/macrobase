package alexp.macrobase.pipeline.benchmark.result;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

public class ResultHolder {
    DataFrame resultsDf;
    long trainingTime, classificationTime, explanationTime, maxMemoryUsage;

    public ResultHolder(DataFrame resultsDf, long trainingTime, long classificationTime, long maxMemoryUsage) {
        this.resultsDf = resultsDf;
        this.trainingTime = trainingTime;
        this.classificationTime = classificationTime;
        this.maxMemoryUsage = maxMemoryUsage;
    }

    public ResultHolder(DataFrame resultsDf, long explanationTime, long maxMemoryUsage) {
        this.resultsDf = resultsDf;
        this.explanationTime = explanationTime;
        this.maxMemoryUsage = maxMemoryUsage;
    }

    public long getExlplanationTime() {
        return explanationTime;
    }

    public DataFrame getResultsDf() {
        return resultsDf;
    }

    public long getTrainingTime() {
        return trainingTime;
    }

    public long getClassificationTime() {
        return classificationTime;
    }

    public long getMaxMemoryUsage() {
        return maxMemoryUsage;
    }
}
