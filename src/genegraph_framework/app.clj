(ns genegraph-framework.app
    "Core include refrerencing all necessary dependencies"
    (:require [genegraph-framework.topic :as topic]
              [genegraph-framework.processor :as processor]
              [genegraph-framework.protocol :as p]
              [genegraph-framework.storage :as s]
              [genegraph-framework.storage.console-writer]
              [genegraph-framework.storage.rocksdb]
              [clojure.spec.alpha :as spec]))

(spec/def ::app
  (spec/keys :req-un [::topics]))

(defrecord App [definition topics storage processors state]

  p/Lifecycle
  (start [this]
    (run! p/start (concat @storage @topics @processors))
    this)
  (stop [this]
    (run! p/stop (concat @processors @storage @topics))
    this))

(defn- add-type-to-defs [entity-defs type]
  (map #(assoc % :type type) entity-defs))

(defn- add-initialized-entity-atoms [entity-defs]
  (atom (mapv p/init entity-defs)))

(defn- add-topic-and-storage-refs-to-processors [app]
  (update app
          :processors
          (fn [processors]
            (map #(merge % (select-keys app [:storage :topics]))
                 processors))))

(defn create
  [app-def]
  (-> app-def
      (update :topics add-type-to-defs :topic)
      (update :topics add-initialized-entity-atoms)
      (update :storage add-initialized-entity-atoms)
      (update :processors add-type-to-defs :processor)
      add-topic-and-storage-refs-to-processors
      (update :processors add-initialized-entity-atoms)
      (assoc :state (atom :stopped))
      map->App))

  #_(map->App
   {:definition app-def
    :entities (init-entities app-def)
    :state (atom :stopped)})
(comment
  (defn offer-to-test [app]
    (p/offer (get @(:entities app) :test-topic) {:key :k :value :v})
    app)

  (def test-interceptor
    {:enter
     (fn [e]
       (clojure.pprint/pprint e)
       (clojure.pprint/pprint (s/get (get-in e [::s/storage :test-rocksdb]) :test))
       e)})

  (def app-def
    {:topics [{:name :test-topic}]
     :storage [{:name :test-rocksdb
                :type :rocksdb
                :path "/users/tristan/desktop/test-rocks"}]
     :processors [{:name :test-processor
                   :subscribe :test-topic
                   :interceptors [test-interceptor]}]})


  (def a (create app-def))
  (p/start a)
  (-> a :topics deref first (p/offer {:key :k :value :v}))
  a
  (p/stop a))
