package alexp.macrobase.outlier;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

import java.util.Map;

public interface ParametersAutoTuner {
    Map<String, Object> tuneParameters(DataFrame trainSet);
}
