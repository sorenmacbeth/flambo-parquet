(defproject flambo-parquet "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.7.0-alpha5"]
                 [yieldbot/flambo "0.5.0-SNAPSHOT"]
                 [com.twitter/parquet-thrift "1.6.0rc3"]
                 [com.twitter.elephantbird/elephant-bird-core "4.5"
                  :exclusions [com.google.guava/guava]]
                 [com.twitter.elephantbird/elephant-bird-pig "4.5"]]
  :profiles {:dev
             {:dependencies [[yieldbot/slurm "0.9.3-SNAPSHOT"]
                             [cheshire "5.4.0"]
                             [org.apache.thrift/libthrift "0.9.2"]]
              :aot [flambo.function]}
             :provided
             {:dependencies [[org.apache.spark/spark-core_2.10 "1.2.0"]
                             [org.apache.hadoop/hadoop-client "2.5.0-mr1-cdh5.3.0"]]}}
  :jvm-opts ^replace ["-server" "-Xmx4g" "-Xms4g"])
