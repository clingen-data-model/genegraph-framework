(ns genegraph.framework.storage.atom
  "Simple atom-based storage mechanism. Useful for local "
  (:require [genegraph.framework.storage :as storage]
            [genegraph.framework.protocol :as p]))

(defrecord AtomStore [name
                      type
                      state
                      instance]
  storage/HasInstance
  (instance [this] instance))



(defmethod p/init :atom-store [atom-def]
  (map->AtomStore
   (assoc atom-def
          :state (atom {:status :running})
          :instance (atom {}))))


(extend clojure.lang.Atom
  
  storage/IndexedWrite
  {:write
   (fn
     ([this k v] (swap! this assoc k v))
     ([this k v commit-promise]
      (storage/write this k v)
      (deliver commit-promise true)))}

  storage/IndexedRead
  {:read (fn [this k] (get @this k))}

  storage/IndexedDelete
  {:delete (fn
             ([this k]
              (swap! this dissoc k))
             ([this k commit-promise]
              (storage/delete this k)
              (deliver commit-promise true)))}
  )

(comment
  (def a (p/init {:name :test-atom-store :type :atom-store}))
  a
  (storage/write (storage/instance a) :k :v)
  (storage/read  (storage/instance a) :k)
  (storage/delete (storage/instance a) :k)
  (storage/read  (storage/instance a) :k) )


