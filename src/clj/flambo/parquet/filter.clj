(ns flambo.parquet.filter
  (:import [parquet.filter
            RecordFilter
            UnboundRecordFilter
            ColumnRecordFilter
            ColumnPredicates$Predicate]))

(def ^:const PARQUET-THRIFT-COLUMN-FILTER "parquet.thrift.column.filter")

(defmacro defrecordfilter
  "Create a named UnboundedRecordFilter class for passing into a hadoop jobconf."
  [name column-path pred]
  (let [classname (str (clojure.string/replace *ns* #"-" "_") ".recordfilter." name)
        prefix (gensym)]
    `(do
       (gen-class
        :name ~classname
        :implements [parquet.filter.UnboundRecordFilter]
        :prefix ~prefix)
       (defn ~(symbol (str prefix "bind"))
         [this# readers#]
         (let [p# (reify ColumnPredicates$Predicate
                    (apply [this# input#]
                      (~pred input#)))]
           (.bind (ColumnRecordFilter/column ~column-path p#) readers#)))
       (def ~name ~classname))))
