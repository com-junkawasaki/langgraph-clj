(ns langgraph.db-test
  (:require [clojure.test :refer [deftest is testing]]
            [langgraph.db :as db]))

(def schema
  {:person/id     {:db/unique :db.unique/identity}
   :person/emails {:db/cardinality :db.cardinality/many}
   :person/friend {:db/valueType :db.type/ref}})

(defn- conn-with-people []
  (let [conn (db/create-conn schema)]
    (db/transact! conn
                  [{:db/id -1 :person/id "alice" :person/name "Alice" :person/age 30
                    :person/emails ["a@x.com" "alice@y.com"]}
                   {:db/id -2 :person/id "bob" :person/name "Bob" :person/age 25
                    :person/friend -1}])
    conn))

(deftest transact-and-query
  (let [conn (conn-with-people)
        dbv (db/db conn)]
    (testing "basic join"
      (is (= #{["Bob" "Alice"]}
             (db/q '[:find ?n ?fn
                     :where [?e :person/friend ?f]
                            [?e :person/name ?n]
                            [?f :person/name ?fn]]
                   dbv))))
    (testing "scalar find + predicate"
      (is (= "Alice"
             (db/q '[:find ?n .
                     :where [?e :person/age ?a]
                            [(> ?a 28)]
                            [?e :person/name ?n]]
                   dbv))))
    (testing "collection find"
      (is (= #{"a@x.com" "alice@y.com"}
             (set (db/q '[:find [?em ...]
                          :where [_ :person/emails ?em]]
                        dbv)))))
    (testing "function binding"
      (is (= #{[30 31]}
             (db/q '[:find ?a ?b
                     :in $ ?name
                     :where [?e :person/name ?name]
                            [?e :person/age ?a]
                            [(inc ?a) ?b]]
                   dbv "Alice"))))
    (testing "in collection binding"
      (is (= #{["Alice"] ["Bob"]}
             (db/q '[:find ?n
                     :in $ [?id ...]
                     :where [?e :person/id ?id]
                            [?e :person/name ?n]]
                   dbv ["alice" "bob"]))))
    (testing "aggregates"
      (is (= 2 (db/q '[:find (count ?e) .
                       :where [?e :person/name _]] dbv)))
      (is (= 30 (db/q '[:find (max ?a) .
                        :where [_ :person/age ?a]] dbv))))))

(deftest upsert-and-cardinality
  (let [conn (conn-with-people)]
    (testing "unique identity upsert merges instead of duplicating"
      (db/transact! conn [{:person/id "alice" :person/age 31}])
      (let [dbv (db/db conn)]
        (is (= 1 (db/q '[:find (count ?e) .
                         :where [?e :person/id "alice"]] dbv)))
        (is (= 31 (db/q '[:find ?a .
                          :where [?e :person/id "alice"]
                                 [?e :person/age ?a]] dbv)))))
    (testing "cardinality-one replaces, cardinality-many accumulates"
      (db/transact! conn [{:person/id "alice" :person/emails "third@z.com"}])
      (is (= 3 (count (db/q '[:find [?em ...]
                              :where [?e :person/id "alice"]
                                     [?e :person/emails ?em]]
                            (db/db conn))))))))

(deftest lookup-refs-and-retract
  (let [conn (conn-with-people)]
    (testing "lookup ref entid + entity"
      (let [e (db/entid (db/db conn) [:person/id "bob"])]
        (is (number? e))
        (is (= "Bob" (:person/name (db/entity (db/db conn) e))))))
    (testing "retractEntity removes incoming refs too"
      (db/transact! conn [[:db/retractEntity [:person/id "alice"]]])
      (let [dbv (db/db conn)]
        (is (nil? (db/entid dbv [:person/id "alice"])))
        (is (empty? (db/q '[:find ?f
                            :where [?e :person/friend ?f]] dbv)))))))

(deftest pull-test
  (let [conn (conn-with-people)
        dbv (db/db conn)]
    (testing "attrs, refs, nested, reverse"
      (let [bob (db/pull dbv '[:person/name {:person/friend [:person/name]}]
                         [:person/id "bob"])]
        (is (= "Bob" (:person/name bob)))
        (is (= "Alice" (get-in bob [:person/friend :person/name]))))
      (let [alice (db/pull dbv '[:person/name :person/_friend]
                           [:person/id "alice"])]
        (is (= 1 (count (:person/_friend alice))))))
    (testing "wildcard includes db/id and many-attrs as vectors"
      (let [alice (db/pull dbv '[*] [:person/id "alice"])]
        (is (number? (:db/id alice)))
        (is (vector? (:person/emails alice)))))))

(deftest not-or-clauses
  (let [conn (conn-with-people)
        dbv (db/db conn)]
    (is (= #{["Bob"]}
           (db/q '[:find ?n
                   :where [?e :person/name ?n]
                          (not [?e :person/id "alice"])]
                 dbv)))
    (is (= #{["Alice"] ["Bob"]}
           (db/q '[:find ?n
                   :where [?e :person/name ?n]
                          (or [?e :person/id "alice"]
                              [?e :person/id "bob"])]
                 dbv)))))

(deftest as-of-test
  (let [conn (db/create-conn schema)
        r1 (db/transact! conn [{:person/id "alice" :person/age 30}])
        _r2 (db/transact! conn [{:person/id "alice" :person/age 31}])
        old (db/as-of conn (:tx r1))]
    (is (= 30 (db/q '[:find ?a . :where [_ :person/age ?a]] old)))
    (is (= 31 (db/q '[:find ?a . :where [_ :person/age ?a]] (db/db conn))))))
