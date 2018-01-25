package alexp.macrobase.ingest;

import alexp.macrobase.utils.ThrowingConsumer;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import java.util.Map;

public interface StreamingDataFrameLoader {
    StreamingDataFrameLoader setColumnTypes(Map<String, Schema.ColType> types);
    void load(ThrowingConsumer<DataFrame> resultCallback) throws Exception;
}
