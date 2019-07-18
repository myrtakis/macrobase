package alexp.macrobase.outlier;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

public interface Updatable {
    void update(DataFrame input) throws Exception;
}
