(ns flambo.parquet.thrift
  (:import [parquet.hadoop ParquetOutputFormat]
           [parquet.hadoop.thrift
            ParquetThriftOutputFormat
            ParquetThriftInputFormat
            ThriftReadSupport]
           [parquet.hadoop.metadata CompressionCodecName]
           [org.apache.hadoop.mapreduce Job]
           [org.apache.hadoop.io NullWritable]))

;; # InputFormat configuration
(defn unbound-record-filter! [job filter-klass]
  (doto job
    (ParquetThriftInputFormat/setUnboundRecordFilter (Class/forName filter-klass))))

;; # OutputFormat configuration
(defn thrift-class! [job klass]
  (doto job
    (ParquetThriftOutputFormat/setThriftClass klass)))

(defn compression! [job codec-name]
  (doto job
    (ParquetThriftOutputFormat/setCompression codec-name)))

(defn column-filter! [conf filter]
  (doto conf
    (.set ThriftReadSupport/THRIFT_COLUMN_FILTER_KEY filter)))

(defn parquet-thrift-file
  "Create an RDD from a directory of parquet-thrift files
  where `klass` is the thrift class used to create the parquet files.

  Accepts an optional :conf keyword argument for a hadoop configuration object."
  [spark-context path klass & {:keys [job filter-klass]
                               :or {job (Job.)}}]
  (let [job (when filter-klass
              (set-unbound-record-filter job filter-klass))
        conf (.getConfiguration job)]
    (.newAPIHadoopFile spark-context
                       path
                       ParquetThriftInputFormat
                       NullWritable
                       klass
                       conf)))

(defn save-as-parquet-thrift-file [rdd path klass & {:keys [job]
                                                     :or {job (Job.)}}]
  (let [conf (.getConfiguration job)]
    (.saveAsNewAPIHadoopFile rdd
                             path
                             Void
                             klass
                             ParquetThriftOutputFormat
                             conf)))

(comment
  (require '[flambo.tuple :as ft])
  (require '[flambo.parquet.thrift :as p])
  (require '[slurm.event :as event])
  (require '[flambo.api :as f])
  (import slurm.event.PageviewEvent)
  (import org.apache.hadoop.mapreduce.Job)
  (import parquet.hadoop.thrift.ParquetThriftOutputFormat)
  (import parquet.hadoop.ParquetOutputFormat)
  (def job (Job.))
  (ParquetThriftOutputFormat/setThriftClass job PageviewEvent)
  ;; (ParquetOutputFormat/setWriteSupportClass job PageviewEvent)
  (def sc (f/local-spark-context "parquet"))
  (def pvs (f/text-file sc "/tmp/uswest*"))
  (def pairs (-> pvs (f/map-to-pair (f/fn [s]
                                      (when-let [pv (try
                                                      (event/->pageview-event (event/parse-pageview-json s))
                                                      (catch Exception e nil))]
                                        (ft/tuple nil pv)))) f/cache))
  (p/save-as-parquet-thrift-file pairs "/tmp/parquet-events" PageviewEvent :job job))
