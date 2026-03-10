(ns genegraph.framework.http-server-test
  (:require [genegraph.framework.http-server :as http-server]
            [genegraph.framework.protocol :as p]
            [genegraph.framework.event :as event]
            [genegraph.framework.topic]      ; loads :simple-queue-topic defmethod
            [genegraph.framework.processor]  ; loads :processor defmethod
            [io.pedestal.interceptor :as interceptor]
            [clojure.test :refer [deftest testing is]])
  (:import [java.net ServerSocket URI]
           [java.net.http HttpClient HttpRequest HttpResponse$BodyHandlers]))

;; ---------------------------------------------------------------------------
;; Helpers
;; ---------------------------------------------------------------------------

(defn free-port
  "Returns a TCP port number that is free at the time of the call."
  []
  (with-open [s (ServerSocket. 0)]
    (.getLocalPort s)))

(defn http-get
  "Performs a synchronous GET request to localhost:port/path.
   Returns {:status <int> :body <string>}."
  [port path]
  (let [client  (HttpClient/newHttpClient)
        request (-> (HttpRequest/newBuilder)
                    (.uri (URI/create (str "http://localhost:" port path)))
                    (.GET)
                    (.build))
        resp    (.send client request (HttpResponse$BodyHandlers/ofString))]
    {:status (.statusCode resp)
     :body   (.body resp)}))

(defn make-http-processor
  "Initialize a Processor suitable for use as an HTTP endpoint handler."
  [proc-name interceptors]
  (p/init {:name         proc-name
           :type         :processor
           :interceptors interceptors
           :topics       {}
           :storage      {}}))

(defn minimal-server-def
  "A server-def with no endpoints or custom routes."
  [port]
  {:type      :http-server
   :name      :test-server
   :port      port
   :endpoints []
   :routes    []})

;; ---------------------------------------------------------------------------
;; endpoint->route — unit tests (no running server required)
;; ---------------------------------------------------------------------------

(deftest endpoint->route-test
  (testing "returns a 5-element vector [path method interceptors :route-name name]"
    (let [proc  (make-http-processor :my-proc [])
          route (http-server/endpoint->route {:path "/api" :processor proc})]
      (is (= 5 (count route)))))

  (testing "first element is the path string"
    (let [proc  (make-http-processor :my-proc [])
          route (http-server/endpoint->route {:path "/api/resource" :processor proc})]
      (is (= "/api/resource" (nth route 0)))))

  (testing "second element defaults to :get when :method is not specified"
    (let [proc  (make-http-processor :my-proc [])
          route (http-server/endpoint->route {:path "/api" :processor proc})]
      (is (= :get (nth route 1)))))

  (testing "second element uses the specified :method"
    (let [proc  (make-http-processor :my-proc [])
          route (http-server/endpoint->route {:path "/api" :processor proc :method :post})]
      (is (= :post (nth route 1)))))

  (testing "third element is the interceptor vector from p/as-interceptors"
    ;; p/as-interceptors creates new objects on each call, so compare by name.
    (let [proc  (make-http-processor :my-proc [])
          route (http-server/endpoint->route {:path "/api" :processor proc})]
      (is (= (mapv :name (p/as-interceptors proc))
             (mapv :name (nth route 2))))))

  (testing "fourth element is the :route-name keyword"
    (let [proc  (make-http-processor :my-proc [])
          route (http-server/endpoint->route {:path "/api" :processor proc})]
      (is (= :route-name (nth route 3)))))

  (testing "fifth element is the processor's :name"
    (let [proc  (make-http-processor :my-proc [])
          route (http-server/endpoint->route {:path "/api" :processor proc})]
      (is (= :my-proc (nth route 4)))))

  (testing "user interceptors on the processor appear in the route interceptor vector"
    (let [ic    (interceptor/interceptor {:name ::my-ic :enter identity})
          proc  (make-http-processor :my-proc [ic])
          route (http-server/endpoint->route {:path "/api" :processor proc})
          ics   (nth route 2)]
      ;; p/as-interceptors prepends 4 framework interceptors; user ic comes after
      (is (some #(= ::my-ic (:name %)) ics)))))

;; ---------------------------------------------------------------------------
;; Server lifecycle — status transitions
;; ---------------------------------------------------------------------------

(deftest server-init-test
  (testing "p/init :http-server returns a Server record"
    (let [server (p/init (minimal-server-def (free-port)))]
      (is (instance? genegraph.framework.http_server.Server server))))

  (testing "status is :stopped before start"
    (let [server (p/init (minimal-server-def (free-port)))]
      (is (= :stopped (:status (p/status server))))))

  (testing "p/status includes the server :name"
    (let [server (p/init (minimal-server-def (free-port)))]
      (is (= :test-server (:name (p/status server))))))

  (testing "p/status includes the configured :port"
    (let [port   (free-port)
          server (p/init (minimal-server-def port))]
      (is (= port (:port (p/status server)))))))

(deftest server-lifecycle-test
  (testing "status is :running after p/start"
    (let [server (p/init (minimal-server-def (free-port)))]
      (p/start server)
      (try
        (is (= :running (:status (p/status server))))
        (finally
          (p/stop server)))))

  (testing "status is :stopped after p/stop"
    (let [server (p/init (minimal-server-def (free-port)))]
      (p/start server)
      (p/stop server)
      (is (= :stopped (:status (p/status server)))))))

;; ---------------------------------------------------------------------------
;; Request handling — real HTTP requests to a running server
;; ---------------------------------------------------------------------------

(deftest plain-route-handler-test
  (testing "a plain route function receives requests and returns a response"
    (let [port   (free-port)
          server (p/init {:type      :http-server
                          :name      :test-server
                          :port      port
                          :endpoints []
                          :routes    [["/hello"
                                       :get
                                       (fn [_] {:status 200 :body "hello"})
                                       :route-name ::hello]]})]
      (p/start server)
      (try
        (let [resp (http-get port "/hello")]
          (is (= 200 (:status resp)))
          (is (= "hello" (:body resp))))
        (finally
          (p/stop server)))))

  (testing "a request to an undefined route returns 404"
    (let [port   (free-port)
          server (p/init (minimal-server-def port))]
      (p/start server)
      (try
        (let [resp (http-get port "/not-defined")]
          (is (= 404 (:status resp))))
        (finally
          (p/stop server))))))

(deftest processor-endpoint-handler-test
  (testing "an endpoint backed by a processor interceptor returns the response"
    (let [port     (free-port)
          hello-ic (interceptor/interceptor
                    {:name  ::hello-ic
                     :enter (fn [e]
                              (assoc e :response {:status 200 :body "hello from processor"}))})
          proc     (make-http-processor :hello-proc [hello-ic])
          server   (p/init {:type      :http-server
                             :name      :test-server
                             :port      port
                             :endpoints [{:path "/greet" :processor proc :method :get}]
                             :routes    []})]
      (p/start server)
      (try
        (let [resp (http-get port "/greet")]
          (is (= 200 (:status resp)))
          (is (= "hello from processor" (:body resp))))
        (finally
          (p/stop server)))))

  (testing "processor interceptor has access to the request map via the event context"
    (let [port       (free-port)
          method-ref (atom nil)
          capture-ic (interceptor/interceptor
                      {:name  ::capture-ic
                       :enter (fn [e]
                                (reset! method-ref (get-in e [:request :request-method]))
                                (assoc e :response {:status 200 :body "ok"}))})
          proc       (make-http-processor :capture-proc [capture-ic])
          server     (p/init {:type      :http-server
                               :name      :test-server
                               :port      port
                               :endpoints [{:path "/check" :processor proc :method :get}]
                               :routes    []})]
      (p/start server)
      (try
        (http-get port "/check")
        (is (= :get @method-ref))
        (finally
          (p/stop server)))))

  (testing "multiple endpoints on the same server are each handled by their processor"
    (let [port  (free-port)
          mk-ic (fn [body]
                  (interceptor/interceptor
                   {:name  (keyword body)
                    :enter (fn [e] (assoc e :response {:status 200 :body body}))}))
          proc-a (make-http-processor :proc-a [(mk-ic "response-a")])
          proc-b (make-http-processor :proc-b [(mk-ic "response-b")])
          server (p/init {:type      :http-server
                           :name      :test-server
                           :port      port
                           :endpoints [{:path "/a" :processor proc-a}
                                       {:path "/b" :processor proc-b}]
                           :routes    []})]
      (p/start server)
      (try
        (is (= "response-a" (:body (http-get port "/a"))))
        (is (= "response-b" (:body (http-get port "/b"))))
        (finally
          (p/stop server))))))
