package alexp.macrobase.pipeline;

import alexp.macrobase.ingest.SqlDataFrameReader;
import alexp.macrobase.ingest.Uri;
import alexp.macrobase.ingest.XlsxDataFrameReader;
import edu.stanford.futuredata.macrobase.datamodel.DataFrame;
import edu.stanford.futuredata.macrobase.datamodel.Schema;
import edu.stanford.futuredata.macrobase.pipeline.PipelineConfig;
import edu.stanford.futuredata.macrobase.pipeline.PipelineUtils;

import java.util.List;
import java.util.Map;

public class Pipelines {
    public static DataFrame loadDataFrame(
            Uri inputURI,
            Map<String, Schema.ColType> colTypes,
            List<String> requiredColumns,
            PipelineConfig conf) throws Exception {
        switch (inputURI.getType()) {
            case XLSX:
                return new XlsxDataFrameReader(inputURI.getPath(), requiredColumns, 0).load();
            case JDBC:
                return new SqlDataFrameReader(inputURI.getPath(), requiredColumns, conf.get("query")).load();
            default:
                return PipelineUtils.loadDataFrame(inputURI.getOriginalString(), colTypes, requiredColumns);
        }
    }
}
