(ns genegraph.base.refresh
  (:require [genegraph.framework.storage.rdf :as rdf]
            [genegraph.framework.storage.gcs :as gcs]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [hato.client :as hc])
  (:import [java.io File InputStream OutputStream]
           [java.nio.channels Channels]))


(defn init [options]
  (io/make-parents (:data-path options))
  (.mkdir (File. (:data-path options))))

;; TODO -- consider moving this to other file; may be a separate
;; operation from loading base files

(defn fetch-file [file-description options]
  (let [response (hc/get (:source file-description)
                         {:http-client (hc/build-http-client {:redirect-policy :always})
                          :as :stream})]
    (when (instance? InputStream (:body response))
      (with-open [wc (gcs/open-write-channel (:bucket options) (:target file-description))
                  os (Channels/newOutputStream wc)]
        (.transferTo (:body response) os)))))
