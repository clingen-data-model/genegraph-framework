(ns genegraph.framework.storage.rdf.instance
  "Namespace for handling operations on persistent Jena datasets.
  Specifically designed around handling asychronous writes. "
  (:require [genegraph.framework.storage :as s])
  (:import [org.apache.jena.tdb2 TDB2Factory]
           [org.apache.jena.query Dataset ReadWrite TxnType]
           [org.apache.jena.rdf.model Model Resource ResourceFactory
            ModelFactory]
           [java.util.concurrent BlockingQueue ArrayBlockingQueue TimeUnit]
           [java.util List ArrayList]
           [org.apache.jena.query.text TextDatasetFactory]))

(def most-recent-offset
  (ResourceFactory/createProperty "http://genegraph/mostRecentOffset"))

(defn topic-kw->iri [kw]
  (let [ns-str (if (namespace kw) (str (namespace kw) "/") "")]
    (ResourceFactory/createResource
     (str "http://genegraph/topic/" ns-str (name kw)))))

(defn offset->model
  "Encapsulate an offset for a topic in a Jena model,
  supports the TopicBackingStore interface."
  [topic offset]
  (doto (ModelFactory/createDefaultModel)
    (.add
     (ResourceFactory/createStatement
      (topic-kw->iri topic)
      most-recent-offset
      (ResourceFactory/createTypedLiteral offset)))))

(defn model->offset
  "From the model for a given topic, return the most recent offset."
  [model]
  (->> (.listObjectsOfProperty model most-recent-offset)
       iterator-seq
       (mapv #(-> % .asLiteral .getLong))
       first))

#_(model->offset (offset->model :test-topic 42))

(defrecord PersistentDataset [dataset
                              run-atom
                              complete-promise
                              queue
                              queue-size
                              name]

  
  ;; Setting the behavior of close to cleanly terminate all related
  ;; resources
  org.apache.jena.query.Dataset
  (close [this]
    (reset! run-atom false)
    (if (deref complete-promise (* 5 1000) false)
      (.close dataset)
      (do (throw (ex-info "Timeout closing dataset."
                          (select-keys this [:name])))
          false)))

  s/IndexedWrite
  (write [this k v]
    (s/write this k v nil))
  (write [this k v commit-promise]
    (.put queue {:command :replace
                 :model-name k
                 :model v
                 :commit-promise commit-promise}))

  s/IndexedRead
  (read [this k]
    (.getNamedModel dataset k))

  s/IndexedDelete
  (delete [this k]
    (s/delete this k nil))
  (delete [this k commit-promise]
    (.put queue {:command :remove
                 :model-name k
                 :commit-promise commit-promise}))

  s/TopicBackingStore
  (store-offset [this topic offset]
    (s/write this
             (topic-kw->iri topic)
             (offset->model topic offset)))
  (retrieve-offset [this topic]
    (.begin this ReadWrite/READ)
    (let [o (model->offset (s/read this (topic-kw->iri topic)))]
      (.end this)
      o))

  ;; The rest is just boilerplate to pass through calls to the Dataset
  ;; interface to the underlying Dataset object. Ideally the need for this
  ;; should diminish as we're able to do more refactoring, but for now
  ;; this keeps this implementation consistent with the way datasets
  ;; have been used up to this point.
  ;; https://jena.apache.org/documentation/javadoc/arq/org.apache.jena.arq/org/apache/jena/query/Dataset.html
  ;; note that the inherited Transactional interface is also implemented
  
  (abort [this] (.abort dataset))
  (^Dataset addNamedModel [this ^String uri ^Model model] (.addNamedModel dataset uri model))
  (^Dataset addNamedModel [this ^Resource resource ^Model model] (.addNamedModel dataset resource model))
  (asDatasetGraph [this] (.asDatasetGraph dataset))
  (^void begin [this ^ReadWrite tx-type] (.begin dataset tx-type))
  (^void begin [this] (.begin dataset))
  (^void begin [this ^TxnType t] (.begin dataset t))
  (calc [this txn-type action] (.calc dataset txn-type action))
  (calculate [this supplier] (.calculate dataset supplier))
  (calculateRead [this supplier] (.calculate dataset supplier))
  (calculateWrite [this supplier] (.calculate dataset supplier))
  (commit [this] (.commit dataset))
  (^boolean containsNamedModel [this ^String uri] (.containsNamedModel dataset uri))
  (^boolean containsNamedModel [this ^Resource resource] (.containsNamedModel dataset resource))
  (end [this] (.end dataset))
  (exec [this tx-type action] (.exec dataset tx-type action))
  (execute [this action] (.execute dataset action))
  (executeRead [this action] (.executeRead dataset action))
  (executeWrite [this action] (.executeWrite dataset action))
  (getContext [this] (.getContext dataset))
  (getDefaultModel [this] (.getDefautModel dataset))
  (getLock [this] (.getLock dataset))
  (^Model getNamedModel [this ^String name] (.getNamedModel dataset name))
  (^Model getNamedModel [this ^Resource name] (.getNamedModel dataset name))
  (getPrefixMapping [this] (.getPrefixMapping dataset))
  (getUnionModel [this] (.getUnionModel dataset))
  (isInTransaction [this] (.isInTransaction dataset))
  (listModelNames [this] (.listModelNames dataset))
  (listNames [this] (.listNames dataset))
  (promote [this] (.promote dataset))
  (promote [this mode] (.promote dataset mode))
  (^Dataset removeNamedModel [this ^String name]
   (.removeNamedModel dataset name))
  (^Dataset removeNamedModel [this ^Resource name]
   (.removeNamedModel dataset name))
  (^Dataset replaceNamedModel [this ^String name ^Model model]
   (.replaceNamedModel dataset name model))
  (^Dataset replaceNamedModel [this ^Resource name ^Model model]
   (.replaceNamedModel dataset name model))
  (setDefaultModel [this model] (.setDefaultModel dataset model))
  (supportsTransactionAbort [this] (.supportsTransactionAbort dataset))
  (supportsTransactions [this] (.supportsTransactions dataset))
  (transactionMode [this] (.transactionMode dataset))
  (transactionType [this] (.transactionType dataset)))

;; TODO in case of error 
(defn execute
  "Execute command on dataset"
  [dataset command]
  (case (:command command)
    :replace (.replaceNamedModel dataset
                                 (:model-name command)
                                 (:model command))
    :remove (.removeNamedModel dataset
                               (:model-name command))
    (throw (ex-info "Invalid command" command))))

(defn deliver-commit-promise
  "Deliver promise when command is committed to the database."
  [command]
  (when-let [committed-promise (:commit-promise command)]
    (deliver committed-promise true)))

(defn write-loop
  "Read commands from queue and execute them on dataset. Pulls multiple records
  from the queue if possible given buffer limit and availability"
  [{:keys [queue-size queue run-atom complete-promise dataset] :as dataset-record}]
  (let [buffer (ArrayList. queue-size)]
    (while @run-atom
      (try
        (when-let [first-command (.poll queue 1000 TimeUnit/MILLISECONDS)]
          (.drainTo queue buffer queue-size)
          (let [commands (cons first-command buffer)]
            (.begin dataset ReadWrite/WRITE)
            (run! #(execute dataset %) commands)
            (.clear buffer)
            (.commit dataset)
            (.end dataset)
            (run! deliver-commit-promise commands)))
        (catch Exception e (println e))))
    (deliver complete-promise true)))

(defn execute-async
  "execute command list asynchronously"
  [{:keys [run-atom queue] :as dataset} commands]
  (when @run-atom
    (run! #(.put queue %) commands)))

(defn execute-sync
  "Execute command list synchronously. Will claim write transaction."
  [{:keys [dataset]} commands]
  (.begin dataset ReadWrite/WRITE)
  (run! #(execute dataset %) commands)
  (.commit dataset)
  (.end dataset))

(def defaults
  {:queue-size 100})

(defn start-dataset [storage-def]
  (let [ds (map->PersistentDataset
            (merge
             defaults
             storage-def
             {:dataset (cond
                         (:assembly-path storage-def)
                         (TextDatasetFactory/create (:assembly-path storage-def))
                         (:path storage-def)
                         (TDB2Factory/connectDataset (:path storage-def))
                         :else (TDB2Factory/createDataset))
              :run-atom (atom true)
              :complete-promise (promise)
              :queue (ArrayBlockingQueue. (:queue-size storage-def))}))]
    (.start (Thread. #(write-loop ds)))
    ds))
