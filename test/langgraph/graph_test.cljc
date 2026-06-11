(ns langgraph.graph-test
  (:require [clojure.test :refer [deftest is testing]]
            [langgraph.graph :as g]
            [langgraph.checkpoint :as cp]
            [langgraph.db :as db]))

(defn- linear-graph []
  (-> (g/state-graph {:channels {:log {:reducer (fnil into []) :default []}}})
      (g/add-node :a (fn [_] {:log [:a]}))
      (g/add-node :b (fn [_] {:log [:b]}))
      (g/set-entry-point :a)
      (g/add-edge :a :b)
      (g/set-finish-point :b)))

(deftest linear-execution
  (let [cg (g/compile-graph (linear-graph))]
    (is (= [:a :b] (:log (g/invoke cg {}))))
    (testing "stream yields one event per node run"
      (is (= [:a :b] (mapv :node (g/stream cg {})))))))

(deftest channels-and-reducers
  (let [cg (-> (g/state-graph {:channels {:n {:reducer + :default 0}
                                          :last {}}})
               (g/add-node :inc1 (fn [_] {:n 1 :last :inc1}))
               (g/add-node :inc2 (fn [_] {:n 10 :last :inc2}))
               (g/set-entry-point :inc1)
               (g/add-edge :inc1 :inc2)
               (g/compile-graph))]
    (let [out (g/invoke cg {:n 100})]
      (is (= 111 (:n out)) "reducer accumulates input + both nodes")
      (is (= :inc2 (:last out)) "default channel is last-write-wins"))))

(deftest conditional-routing
  (let [cg (-> (g/state-graph)
               (g/add-node :start (fn [s] s))
               (g/add-node :small (fn [s] (assoc s :result :small)))
               (g/add-node :big (fn [s] (assoc s :result :big)))
               (g/set-entry-point :start)
               (g/add-conditional-edges :start
                                        (fn [s] (if (> (:x s) 10) :big :small)))
               (g/compile-graph))]
    (is (= :big (:result (g/invoke cg {:x 11}))))
    (is (= :small (:result (g/invoke cg {:x 1}))))))

(deftest recursion-limit
  (let [cg (-> (g/state-graph)
               (g/add-node :loop (fn [s] s))
               (g/set-entry-point :loop)
               (g/add-edge :loop :loop)
               (g/compile-graph {:recursion-limit 5}))]
    (is (thrown? #?(:clj Exception :cljs js/Error) (g/invoke cg {})))))

(defn- interrupting-graph [checkpointer]
  (-> (g/state-graph {:channels {:log {:reducer (fnil into []) :default []}}})
      (g/add-node :draft (fn [_] {:log [:draft]}))
      (g/add-node :send (fn [_] {:log [:send]}))
      (g/set-entry-point :draft)
      (g/add-edge :draft :send)
      (g/compile-graph {:checkpointer checkpointer
                        :interrupt-before #{:send}})))

(deftest interrupt-and-resume-mem
  (let [cpr (cp/mem-checkpointer)
        cg (interrupting-graph cpr)
        r1 (g/run* cg {} {:thread-id "t1"})]
    (is (= :interrupted (:status r1)))
    (is (= [:draft] (:log (:state r1))) "stopped before :send")
    (testing "human-in-the-loop state edit, then resume with nil input"
      (g/update-state! cg "t1" {:log [:human-approved]})
      (let [r2 (g/run* cg nil {:thread-id "t1"})]
        (is (= :done (:status r2)))
        (is (= [:draft :human-approved :send] (:log (:state r2))))))))

(deftest interrupt-and-resume-datomic
  (let [conn (db/create-conn cp/checkpoint-schema)
        cpr (cp/datomic-checkpointer conn)
        cg (interrupting-graph cpr)]
    (g/run* cg {} {:thread-id "t1"})
    (testing "checkpoints are queryable datoms"
      (is (pos? (count (cp/list-checkpoints cpr "t1"))))
      (is (= :interrupted (:status (cp/get-latest cpr "t1")))))
    (testing "resume from datomic-backed checkpoint"
      (let [r2 (g/run* cg nil {:thread-id "t1"})]
        (is (= :done (:status r2)))
        (is (= [:draft :send] (:log (:state r2))))))
    (testing "time travel via get-state-at"
      (let [steps (mapv :step (cp/list-checkpoints cpr "t1"))]
        (is (= steps (vec (sort steps))))
        (is (some? (cp/get-state-at cpr "t1" (first steps))))))))
