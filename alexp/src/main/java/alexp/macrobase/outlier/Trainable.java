package alexp.macrobase.outlier;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

public interface Trainable {
    void train(DataFrame input) throws Exception;
}
