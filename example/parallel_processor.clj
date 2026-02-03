(ns parallel-processor
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.kafka.admin :as kafka-admin]
            [io.pedestal.log :as log]
            [io.pedestal.interceptor :as interceptor]
            [charred.api :as json]
            [portal.api :as portal]))

(comment
  (do
    (def p (portal/open))
    (add-tap #'portal/submit))
  (portal/close)
  (portal/clear)
  )

(defn interceptor-fn [e]
  (tap> (get-in e [::event/data :sequence]))
  e)

(def parallel-interceptor
  (interceptor/interceptor
   {:name :parallel-interceptor
    :enter (fn [e] (interceptor-fn e))}))

(def parallel-processor
  {:name :parallel-processor
   :type :parallel-processor
   :subscribe :test-topic
   :gate-fn :key
   :interceptors [parallel-interceptor]})

(def parallel-test-app-def
  {:name :parallel-test-app
   :type :genegraph-app
   :storage {:test-rocksdb
             {:type :rocksdb
              :name :test-rocksdb
              :path "/Users/tristan/data/genegraph-neo/parallel-rocks-test"}}
   :topics {:test-topic
            {:name :test-topic
             :type :simple-queue-topic
             :serialization :json}}
   :processors {:parallel-processor parallel-processor}})

(comment
  (def parallel-test-app (p/init parallel-test-app-def))
  (p/start parallel-test-app)
  (p/stop parallel-test-app)

  (->> (range 0 20)
       (map (fn [n] {::event/value (json/write-json-str {:key :a :sequence n})
                     ::event/format :json}))
       (run! #(p/publish (get-in parallel-test-app [:topics :test-topic]) %)))

  (let [parallel-test-app (p/init parallel-test-app-def)]
    (p/start parallel-test-app)
    (->> (range 0 100)
         (map (fn [n] {::event/value (json/write-json-str {:key :a :sequence n})
                       ::event/format :json}))
         (run! #(p/publish (get-in parallel-test-app [:topics :test-topic]) %)))
    (Thread/sleep 100)
    (p/stop parallel-test-app))

  (portal/clear)
  
  )
