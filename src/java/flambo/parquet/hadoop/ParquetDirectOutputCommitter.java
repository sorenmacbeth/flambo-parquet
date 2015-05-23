package flambo.parquet.hadoop;

import parquet.Log;
import parquet.hadoop.util.ContextUtil;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.ParquetOutputCommitter;
import parquet.hadoop.Footer;
import parquet.hadoop.ParquetFileWriter;
import parquet.hadoop.ParquetFileReader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;

import java.io.IOException;
import java.util.List;

public class ParquetDirectOutputCommitter extends ParquetOutputCommitter {
  private static final Log LOG = Log.getLog(ParquetDirectOutputCommitter.class);

  Path outputPath;

  public ParquetDirectOutputCommitter(Path outputPath, TaskAttemptContext context) throws IOException {
    super(outputPath, context);
    this.outputPath = outputPath;
  }

  @Override
  public Path getWorkPath() {
    return outputPath;
  }

  @Override
  public void abortTask(TaskAttemptContext taskContext) {
    LOG.warn("abortTask or nah");
  }

  @Override
  public void commitTask(TaskAttemptContext taskContext) {
    LOG.warn("commitTask or nah");
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext taskContext) {
    return true;
  }

  @Override
  public void setupJob(JobContext jobContext) {
    LOG.warn("setupJob or nah");
  }

  @Override
  public void setupTask(TaskAttemptContext taskContext) {
    LOG.warn("setupTask or nah");
  }

  @Override
  public void commitJob(JobContext jobContext) throws IOException {
    Configuration configuration = ContextUtil.getConfiguration(jobContext);
    FileSystem fileSystem = outputPath.getFileSystem(configuration);

    if (configuration.getBoolean(ParquetOutputFormat.ENABLE_JOB_SUMMARY, true)) {
      try {
        FileStatus outputStatus = fileSystem.getFileStatus(outputPath);
        List<Footer> footers = ParquetFileReader.readAllFootersInParallel(configuration, outputStatus);
        try {
          ParquetFileWriter.writeMetadataFile(configuration, outputPath, footers);
        } catch(Exception e) {
          LOG.warn("could not write summary file for " + outputPath, e);
          Path metadataPath = new Path(outputPath, ParquetFileWriter.PARQUET_METADATA_FILE);
          if (fileSystem.exists(metadataPath)) {
            fileSystem.delete(metadataPath, true);
          }
        }
      } catch(Exception e) {
        LOG.warn("could not write summary file for " + outputPath, e);
      }

      if (configuration.getBoolean("mapreduce.fileoutputcommitter.marksuccessfuljobs", true)) {
        try {
          Path successPath = new Path(outputPath, FileOutputCommitter.SUCCEEDED_FILE_NAME);
          fileSystem.create(successPath).close();
        } catch(Exception e) {
          LOG.warn("could not write success file for " + outputPath, e);
        }
      }
    }
  }
}
