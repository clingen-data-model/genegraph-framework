(ns genegraph.lucene-example
  (:import [org.apache.lucene.analysis Analyzer]
           [org.apache.lucene.analysis.standard StandardAnalyzer]
           [org.apache.lucene.store Directory FSDirectory]
           [org.apache.lucene.index IndexWriter IndexWriterConfig]
           [java.nio.file Path]))


(with-open [iw (IndexWriter. (FSDirectory/open
                              (Path/of "/Users/tristan/data/genegraph-neo/lucene-test"
                                       (make-array String 0)))
                             (IndexWriterConfig. (StandardAnalyzer.)))])
