(ns transaction-publish-example
  (:require [genegraph.framework.app :as app]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.storage :as storage]
            [genegraph.framework.kafka.admin :as kafka-admin]
            [io.pedestal.log :as log]
            [io.pedestal.interceptor :as interceptor]
            [clojure.data.json :as json]))
