(ns langgraph.kotoba-checkpoint-test
  "Integration-style tests for kotoba-backed checkpointer using a scripted
  mock http-fn (no running kotoba-server required)."
  (:require [clojure.test :refer [deftest is testing]]
            #?(:clj  [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [clojure.string :as str]
            [langchain.kotoba-db         :as kdb]
            [langgraph.checkpoint        :as cp]
            [langgraph.kotoba-checkpoint :as kcp]))

;; ─── mock infrastructure ─────────────────────────────────────────────────────

(defn- nsid-from-url [url]
  (last (str/split url #"/xrpc/")))

(defn- stateful-mock-caps
  "Mock host-caps backed by an in-memory store that simulates kotoba's
  datomic.transact / datomic.q / datomic.pull endpoints.

  Checkpoints are stored as Clojure maps in `store` atom keyed by
  checkpoint/key. This is enough to exercise the full checkpointer
  round-trip without a running server."
  []
  (let [store   (atom {})    ; {:checkpoint-key entity-map}
        threads (atom {})]   ; {:thread-id #{entity-key ...}}
    {:http-fn
     (fn [{:keys [url body]}]
       (let [nsid   (nsid-from-url url)
             parsed (edn/read-string body)]
         {:status 200
          :body
          (pr-str
           (case nsid
             "ai.gftd.apps.kotobase.datomic.transact"
             (let [tx-data (edn/read-string (:tx_edn parsed))]
               (doseq [op tx-data]
                 (when (map? op)
                   (let [k (or (:checkpoint/key op)
                               (:db/id op))
                         tid (:checkpoint/thread op)]
                     (when k
                       (swap! store assoc k op)
                       (when tid
                         (swap! threads update tid (fnil conj #{}) k))))))
               {:status "ok" :graph "g" :tx_cid "cid" :commit_cid "cid2"
                :ipns_name "k51" :ipns_sequence 1 :ipns_valid_until "2099"
                :index_roots {} :datom_count 1 :journal_cids []
                :tempids {} :datoms []})

             "ai.gftd.apps.kotobase.datomic.q"
             ;; Supports two query shapes used by datomic-checkpointer:
             ;; (max ?step) . → scalar → max step for thread
             ;; [?e ...] → collection → all checkpoint entity keys for thread
             (let [qedn  (:query_edn parsed)
                   ins   (:inputs_edn parsed)
                   tid   (when (seq ins) (edn/read-string (first ins)))
                   ekeys (when tid (vec (@threads tid)))
                   steps (mapv (fn [k]
                                 (get-in @store [k :checkpoint/step] 0))
                               (or ekeys []))]
               (cond
                 ;; scalar: (max ?step) .
                 (str/includes? qedn "(max ")
                 {:graph "g" :basis_t nil
                  :rows_edn (if (seq steps)
                              [[(pr-str (apply max steps))]]
                              [])}
                 ;; collection: [?e ...]
                 :else
                 {:graph "g" :basis_t nil
                  :rows_edn (mapv (fn [k] [(pr-str k)]) (or ekeys []))}))

             "ai.gftd.apps.kotobase.datomic.pull"
             (let [eid-edn (:entity parsed)
                   eid     (edn/read-string eid-edn)
                   ;; lookup ref [:checkpoint/key "t/5"] or bare key string
                   ck-key  (if (vector? eid) (second eid) eid)
                   entity  (get @store ck-key)]
               {:graph "g" :basis_t nil
                :entity (pr-str ck-key) :datom_count 5 :datoms []
                :entity_edn (pr-str (or entity {}))})

             ;; unknown NSID
             (throw (ex-info "unknown nsid in mock" {:nsid nsid}))))}))
     :json-write pr-str
     :json-read  edn/read-string}))

;; ─── tests ───────────────────────────────────────────────────────────────────

(def ^:private test-conn
  (kdb/kotoba-conn "http://kotoba.test:8080" "k51testgraph"))

(deftest put-and-get-latest
  (let [caps (stateful-mock-caps)
        cp   (kcp/checkpointer test-conn caps)
        ckpt {:step 0 :state {:x 1} :frontier [:node-a] :status :running}]
    (cp/put! cp "thread-1" ckpt)
    (let [got (cp/get-latest cp "thread-1")]
      (testing "get-latest returns the stored checkpoint"
        (is (= 0 (:step got)))
        (is (= {:x 1} (:state got)))
        (is (= [:node-a] (:frontier got)))
        (is (= :running (:status got)))))))

(deftest multiple-steps-get-latest
  (let [caps (stateful-mock-caps)
        cp   (kcp/checkpointer test-conn caps)]
    (cp/put! cp "thread-2" {:step 0 :state {:n 0} :frontier [:a] :status :running})
    (cp/put! cp "thread-2" {:step 1 :state {:n 1} :frontier [:b] :status :running})
    (cp/put! cp "thread-2" {:step 2 :state {:n 2} :frontier [:c] :status :done})
    (let [got (cp/get-latest cp "thread-2")]
      (testing "get-latest returns highest step"
        (is (= 2 (:step got)))
        (is (= {:n 2} (:state got)))
        (is (= :done (:status got)))))))

(deftest list-checkpoints-all-steps
  (let [caps (stateful-mock-caps)
        cp   (kcp/checkpointer test-conn caps)]
    (cp/put! cp "thread-3" {:step 0 :state {:a 1} :frontier [] :status :running})
    (cp/put! cp "thread-3" {:step 1 :state {:a 2} :frontier [] :status :running})
    (let [all (cp/list-checkpoints cp "thread-3")]
      (testing "list-checkpoints returns all steps in order"
        (is (= 2 (count all)))
        (is (= [0 1] (mapv :step all)))))))

(deftest get-latest-empty-thread
  (let [caps (stateful-mock-caps)
        cp   (kcp/checkpointer test-conn caps)]
    (is (nil? (cp/get-latest cp "no-such-thread")))))

(deftest get-state-at-step
  (let [caps (stateful-mock-caps)
        cp   (kcp/checkpointer test-conn caps)]
    (cp/put! cp "thread-4" {:step 0 :state {:v 10} :frontier [] :status :running})
    (cp/put! cp "thread-4" {:step 1 :state {:v 20} :frontier [] :status :done})
    (let [at0 (cp/get-state-at cp "thread-4" 0)]
      (testing "get-state-at returns checkpoint at requested step"
        (is (= 0 (:step at0)))
        (is (= {:v 10} (:state at0)))))))

(deftest ensure-schema-calls-transact
  (let [captured (atom [])
        caps     {:http-fn   (fn [{:keys [url body]}]
                               (swap! captured conj
                                      {:nsid (nsid-from-url url)
                                       :body (edn/read-string body)})
                               {:status 200
                                :body   (pr-str {:status "ok" :tx_cid "x"
                                                 :commit_cid "y" :graph "g"
                                                 :ipns_name "k" :ipns_sequence 0
                                                 :ipns_valid_until "" :index_roots {}
                                                 :datom_count 0 :journal_cids []
                                                 :tempids {} :datoms []})})
                  :json-write pr-str
                  :json-read  edn/read-string}]
    (kcp/ensure-schema! test-conn caps)
    (testing "ensure-schema! calls transact"
      (is (= 1 (count @captured)))
      (is (= "ai.gftd.apps.kotobase.datomic.transact"
             (:nsid (first @captured)))))))
