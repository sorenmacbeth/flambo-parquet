package flambo.parquet.hadoop;

import parquet.hadoop.api.WriteSupport;
import parquet.hadoop.ParquetOutputFormat;

import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class ParquetDirectOutputFormat<T> extends ParquetOutputFormat<T> {

  OutputCommitter committer;

  public <S extends WriteSupport<T>> ParquetDirectOutputFormat(S writeSupport) {
    super(writeSupport);
  }

  public <S extends WriteSupport<T>> ParquetDirectOutputFormat() {
    super();
  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
    throws IOException {
    if (committer == null) {
      Path output = getOutputPath(context);
      committer = new ParquetDirectOutputCommitter(output, context);
    }
    return committer;
  }
}
