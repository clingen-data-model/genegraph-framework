(ns genegraph-framework.app
    "Core include refrerencing all necessary dependencies"
    (:require [genegraph-framework.topic :as topic]
              [genegraph-framework.processor :as processor]
              [genegraph-framework.protocol :as p]
              [genegraph-framework.storage]
              [clojure.spec.alpha :as spec]))

(spec/def ::app
  (spec/keys :req-un [::topics]))

(defn start-sequence
  "Sequence of elements in app in the order
  required for initiation"
  [app]
  (concat (vals (:topics app))
          (vals (:processors app))))

(defrecord App [definition entities state]

  p/Lifecycle
  (start [this]
    (run! p/start (vals @entities)))
  (stop [this]
    (run! p/stop (vals @entities))))

(defn init-entities [app-def]
  (let [entity-atom (atom {})]
    (reset!
     entity-atom
     (reduce
      (fn [entity-map entity-def]
        (assoc entity-map
               (:name entity-def)
               (p/init (assoc entity-def :entities entity-atom))))
      {}
      app-def))
    entity-atom))

(defn create
  [app-def]
  (map->App
   {:definition app-def
    :entities (init-entities app-def)
    :state (atom :stopped)}))

(def sample-app
  [{:name :test-topic
    :type :topic}
   {:name :test-processor
    :type :processor
    :interceptors [{:enter
                    (fn [e]
                      (assoc
                       e
                       :effects
                       [{:target :console-writer
                         :effect :write-record
                         :key :akey
                         :value :avalue}]))}]
    :subscribed-topic :test-topic}
   {:name :console-writer
    :type :console-writer}])

(def app (create sample-app))
(p/start app)
(p/stop app)

(p/offer (get @(:entities app) :test-topic) {:key :k :value :v})

