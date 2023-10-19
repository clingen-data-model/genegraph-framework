(ns genegraph.framework.graphql
  (:require
   [clojure.edn :as edn]
   [clojure.java.io :as io]
   [com.walmartlabs.lacinia.pedestal2 :as p2]
   [com.walmartlabs.lacinia.schema :as schema]
   [com.walmartlabs.lacinia.util :as util]
   [io.pedestal.http :as http]))

(def schema
  {:queries
   {:hello
    {:type 'String}}})

(defn ^:private resolve-hello
  [context args value]
  (clojure.pprint/pprint context)
  "Hello, Clojurians!")

(defn ^:private hello-schema
  []
  (-> schema
      (util/inject-resolvers {:queries/hello resolve-hello})
      schema/compile))

(comment

  (def test-interceptors
    [p2/initialize-tracing-interceptor
     p2/json-response-interceptor
     p2/error-response-interceptor
     p2/body-data-interceptor
     p2/graphql-data-interceptor
     p2/status-conversion-interceptor
     p2/missing-query-interceptor
     (p2/query-parser-interceptor (hello-schema))
     p2/disallow-subscriptions-interceptor
     p2/prepare-query-interceptor
     (p2/inject-app-context-interceptor {})
     p2/enable-tracing-interceptor
     p2/query-executor-handler]
    )

  (map type test-interceptors)
  (mapv :name (p2/default-interceptors (hello-schema) nil))
  
  (hello-schema)

  (def service (-> (hello-schema)
                   (p2/default-service nil)))
  (http/stop service)

  )
