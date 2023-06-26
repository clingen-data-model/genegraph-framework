(ns genegraph.base.transform.rdf
  (:require [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.event :as event]
            [clojure.java.io :as io]))

(defmethod event/add-model :rdf
  [event]
  (let [file-definition (:value event)]
    (with-open [is (io/input-stream
                    (str (::data-path event)
                         "/"
                         (:target file-definition)))]
      (assoc event
             ::rdf/model (rdf/read-rdf is (:reader-opts file-definition))
             ::rdf/iri (:name file-definition)))))
