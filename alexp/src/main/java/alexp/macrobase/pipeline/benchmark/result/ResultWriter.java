package alexp.macrobase.pipeline.benchmark.result;

import edu.stanford.futuredata.macrobase.datamodel.DataFrame;

public interface ResultWriter {
    void write(DataFrame outputData, ExecutionResult result) throws Exception;
}
