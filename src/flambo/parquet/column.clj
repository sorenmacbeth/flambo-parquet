(ns flambo.parquet.column
  (:refer-clojure :exclude [long float double boolean int str])
  (:import [parquet.column ColumnReader]))

(defprotocol ClojureColumnReader
  (int [this])
  (boolean [this])
  (long [this])
  (float [this])
  (double [this])
  (binary [this])
  (str [this]))

(extend-protocol ClojureColumnReader
  ColumnReader
  (int [this]
    (.getInteger this))
  (boolean [this]
    (.getBoolean this))
  (long [this]
    (.getLong this))
  (float [this]
    (.getFloat this))
  (double [this]
    (.getDouble this))
  (binary [this]
    (.getBinary this))
  (str [this]
    (.. this getBinary toStringUsingUTF8)))
