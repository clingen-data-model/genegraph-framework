(ns genegraph.framework.storage.console-writer
  (:require
   [genegraph.framework.protocol :as p]
   [clojure.pprint :refer [pprint]]))

(defrecord ConsoleWriter []
    
  p/Lifecycle
  (start [_])
  (stop [_]))


(defmethod p/init :console-writer [_]
  (->ConsoleWriter))



