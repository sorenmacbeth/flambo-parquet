(defproject yieldbot/flambo-parquet "0.1.1"
  :description "Functions for reading and writing parquet file with flambo."
  :url "https://github.com/yieldbot/flambo-parquet"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"cloudera" "https://repository.cloudera.com/artifactory/cloudera-repos/"
                 "snapshots" {:url "s3p://maven.yieldbot.com/snapshots/"
                              :username :env :passphrase :env}
                 "releases" {:url "s3p://maven.yieldbot.com/releases/"
                             :username :env :passphrase :env}}
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [yieldbot/flambo "0.5.0-SNAPSHOT"]
                 [com.twitter/parquet-thrift "1.6.0rc3"]
                 [com.twitter.elephantbird/elephant-bird-core "4.5"
                  :exclusions [com.google.guava/guava]]
                 [com.twitter.elephantbird/elephant-bird-pig "4.5"]]
  :profiles {:dev
             {:dependencies [[yieldbot/slurm "0.9.3-SNAPSHOT"]
                             [org.apache.thrift/libthrift "0.9.2"]]
              :aot [flambo.function]}
             :provided
             {:dependencies [[org.apache.spark/spark-core_2.10 "1.2.0"]
                             [org.apache.hadoop/hadoop-client "2.5.0-mr1-cdh5.3.0"]]}}
  :plugins [[s3-wagon-private "1.1.2"]]
  :javac-options ["-source" "1.6" "-target" "1.6"]
  :jvm-opts ^replace ["-server" "-Xmx4g" "-Xms4g"])
