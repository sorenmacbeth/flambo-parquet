package flambo.parquet.hadoop.thrift;

import flambo.parquet.hadoop.ParquetDirectOutputFormat;

import org.apache.hadoop.mapreduce.Job;
import org.apache.thrift.TBase;

import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.util.ContextUtil;
import parquet.hadoop.thrift.ThriftWriteSupport;

public class ParquetThriftDirectOutputFormat<T extends TBase<?,?>> extends ParquetDirectOutputFormat<T> {

  public static void setThriftClass(Job job, Class<? extends TBase<?,?>> thriftClass) {
    ThriftWriteSupport.setThriftClass(ContextUtil.getConfiguration(job), thriftClass);
  }

  public static Class<? extends TBase<?,?>> getThriftClass(Job job) {
    return ThriftWriteSupport.getThriftClass(ContextUtil.getConfiguration(job));
  }

  public ParquetThriftDirectOutputFormat() {
    super(new ThriftWriteSupport<T>());
  }

}
