(ns genegraph-framework.topic
  "Defines the logic for handling topics"
  (:require [clojure.spec.alpha :as spec]
            [genegraph-framework.protocol :as p])
  (:import  [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]
            [java.util List ArrayList]))

(spec/def ::topic
  (spec/keys :req-un [::name]
             :opt-un [::buffer-size ::timeout]))

(spec/def ::name keyword?)
(spec/def ::buffer-size int?)
(spec/def ::timeout int?)

(def topic-defaults
  {:timeout 1000
   :buffer-size 100})

(defrecord Topic [name buffer-size timeout queue entities]
  
  p/Lifecycle
  (start [this])
  (stop [this])

  p/Queue
  (poll [this]
    (.poll (:queue this)
           (:timeout this)
           TimeUnit/MILLISECONDS))
  (offer [this x]
    (.offer (:queue this)
            x
            (:timeout this)
            TimeUnit/MILLISECONDS)))

(defmethod p/init :topic  [topic-definition]
  (let [topic-def-with-defaults (merge topic-defaults
                                       topic-definition)]
    (map->Topic
     (assoc topic-def-with-defaults
            :queue (ArrayBlockingQueue. (:buffer-size topic-def-with-defaults))))))

(comment
  (def t (p/init {:name :test
                  :type :topic
                  :buffer-size 3}))
  (p/offer t {:hi :there})
  (p/poll t))


#_(spec/explain ::topic {::aname :test})
