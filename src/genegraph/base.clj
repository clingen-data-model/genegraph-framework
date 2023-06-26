(ns genegraph.base
  (:require [genegraph.base.transform.rdf]
            [genegraph.framework.event]))

;; These might be part of a common pattern



(defn add-input-stream
  "Decorate the event with an input stream supplying the data
  from the source."
  [event]
  (if (:data-path event)
    ))
