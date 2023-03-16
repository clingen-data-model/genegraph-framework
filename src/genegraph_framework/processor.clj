(ns genegraph-framework.processor
  "Logic to handle processing over topics"
  (:require [genegraph-framework.protocol :as p]
            [io.pedestal.interceptor :as interceptor]
            [io.pedestal.interceptor.chain :as interceptor-chain]
            [clojure.spec.alpha :as spec]))

(spec/def ::processor
  (spec/keys :req-un [::name ::subscribe]))

(spec/def ::status #(:running :stopped))

(defrecord Processor [name
                      subscribed-topic
                      storage
                      state
                      interceptors
                      entities]

  p/Lifecycle
  (start [this]
    (reset! (:state this) :running)
    (.start
     (Thread.
      #(while (= :running @(:state this))
         (when-let [event (p/poll (get @entities (:subscribed-topic this)))]
           (interceptor-chain/execute
            (interceptor-chain/enqueue
             event
             (:interceptors this))))))))
  (stop [this]
    (reset! (:state this) :stopped)))

(defmethod p/init :processor [processor-def]
  (-> processor-def
      (assoc :state (atom :stopped))
      (update :interceptors
              #(mapv interceptor/interceptor %))
      map->Processor))
