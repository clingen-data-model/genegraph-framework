(ns genegraph.control-plane-example
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.rocksdb :as rocksdb]
            [genegraph.framework.event :as event]
            [clojure.java.io :as io]
            [io.pedestal.log :as log]
            [io.pedestal.interceptor :as interceptor])
  (:import [java.io BufferedInputStream BufferedOutputStream]
           [net.jpountz.lz4 LZ4FrameInputStream LZ4FrameOutputStream]))

;; TODO need references to storage container objects in event
;; delivered from processor, this will allow the control plane
;; topic to trigger creation of a snapshot.

;; Question: should there be a default control plane interceptor,
;; with the ability to do basic things (like create storage snapshots)
;; without it being defined in a target app?



(def test-base-path
  "/users/tristan/data/genegraph-neo/control-plane-test/")

(def store-data-interceptor
  (interceptor/interceptor
   {:name ::store-jena-data-interceptor
    :leave (fn [{k ::event/key
                 v ::event/data
                 :as e}]
             (log/info :interceptor ::store-data-interceptor
                       :k k
                       :v v)
             (-> e 
                 (event/store :test-jena k v)
                 (event/store :test-rocks k v)))}))

(defn command-interceptor-fn [e]
  (log/info :fn ::command-interceptor-fn)
  e)

(def command-interceptor
  (interceptor/interceptor
   {:name ::command-interceptor
    :enter #(command-interceptor-fn %)}))

(def control-plane-app-def
  {:type :genegraph-app
   :storage {:test-rocks
             {:name :test-rocks
              :type :rocksdb
              :path (str test-base-path "test-rocks")
              :load-snapshot true
              :snapshot-handle {:type :file
                                :base test-base-path
                                :path "rocks-snapshot.tar.lz4"}}
             :test-jena
             {:name :test-jena
              :type :rdf
              :path (str test-base-path "test-jena")
              :load-snapshot true
              :snapshot-handle {:type :file
                                :base test-base-path
                                :path "jena-snapshot.nq.gz"}}}
   :topics {:control-plane
            {:name :control-plane
             :type :simple-queue-topic}
            :test-data
            {:name :test-data
             :type :simple-queue-topic}}
   :processors {:test-data-processor
                {:name :test-data-processor
                 :type :processor
                 :subscribe :test-data
                 :interceptors
                 [store-data-interceptor]}
                :control-plane-processor
                {:name :control-plane-processor
                 :type :processor
                 :subscribe :control-plane
                 :interceptors
                 [command-interceptor]}}})

(comment
  (def control-plane-app
    (p/init control-plane-app-def))

  (p/start control-plane-app)
  (p/stop control-plane-app)

  (storage/store-snapshot (get-in control-plane-app [:storage :test-jena]))

  (rocksdb/rocks-store-snapshot (get-in control-plane-app
                                  [:storage :test-rocks]))

  (rocksdb/rocks-restore-snapshot (get-in control-plane-app
                                  [:storage :test-rocks]))

  (p/publish (get-in control-plane-app [:topics :control-plane])
             {::event/key "http://example.com/test"
              ::event/data (rdf/statements->model
                            [[(rdf/resource "http://example.com/test")
                              "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
                              (rdf/resource "http://www.w3.org/2000/01/rdf-schema#Class")]])})


    (p/publish (get-in control-plane-app [:topics :control-plane])
             {::event/key "create-snapshots"
              ::event/data {:command :create-snapshots}})

  
  (let [tdb @(get-in control-plane-app [:storage :test-jena :instance])]
    (rdf/tx tdb
      (rdf/to-turtle (storage/read tdb "http://example.com/test"))))

  (storage/read @(get-in control-plane-app [:storage :test-rocks :instance])
                "http://example.com/test")
  
  (with-open [is (-> {:type :file
                      :base test-base-path
                      :path "no-snapshot"}
                     storage/as-handle
                     io/input-stream)]
    (slurp is))

  (-> {:type :file
       :base test-base-path
       :path "no-snapshot"}
      storage/as-handle
      storage/exists?)

  (-> {:type :gcs
       :bucket "genegraph-framework-dev"
       :path "no-snapshot"}
      storage/as-handle
      storage/exists?)

  (with-open [is (-> {:type :gcs
                      :bucket "genegraph-framework-dev"
                      :path "no-snapshot"}
                     storage/as-handle
                     io/input-stream)]
    (slurp is))
  )


