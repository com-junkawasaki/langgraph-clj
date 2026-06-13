(ns langgraph.checkpoint
  "Graph checkpointers: persistence of graph state per thread, enabling
  resume, human-in-the-loop interrupts, and time travel.

  The Datomic checkpointer stores every superstep as datoms — graph
  execution history becomes a queryable fact log (ADR-0010 L1 layer:
  facts as EAV, views as named Datalog queries). Time travel falls out
  of the data model: each step is an entity, `get-state-at` is a query."
  (:require #?(:clj [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [langchain.db :as db]))

(defprotocol Checkpointer
  (-put! [cp thread-id checkpoint]
    "checkpoint: {:step n :state map :frontier [nodes] :status kw}")
  (-get-latest [cp thread-id])
  (-list-checkpoints [cp thread-id]
    "All checkpoints for the thread, ascending by :step."))

(defn put! [cp thread-id ckpt] (-put! cp thread-id ckpt))
(defn get-latest [cp thread-id] (-get-latest cp thread-id))
(defn list-checkpoints [cp thread-id] (-list-checkpoints cp thread-id))

(defn get-state-at
  "Time travel: the checkpoint at a given step (nil if absent)."
  [cp thread-id step]
  (some #(when (= step (:step %)) %) (list-checkpoints cp thread-id)))

;; ───────────────────────── in-memory ─────────────────────────

(defn mem-checkpointer []
  (let [store (atom {})]
    (reify Checkpointer
      (-put! [_ tid ckpt] (swap! store update tid (fnil conj []) ckpt) ckpt)
      (-get-latest [_ tid] (peek (get @store tid [])))
      (-list-checkpoints [_ tid] (vec (sort-by :step (get @store tid [])))))))

;; ───────────────────────── Datomic-backed ─────────────────────────

(def checkpoint-schema
  "Merge into your db schema."
  {:checkpoint/key      {:db/unique :db.unique/identity} ; "<thread>/<step>"
   :checkpoint/thread   {}
   :checkpoint/step     {}
   :checkpoint/state    {}   ; pr-str EDN
   :checkpoint/frontier {}   ; pr-str EDN
   :checkpoint/status   {}})

(defn datomic-checkpointer
  "Checkpointer over a Datomic-API connection. `db-api` defaults to the
  built-in langchain.db; pass a Datomic/DataScript-shaped map to swap
  the backend (see langchain.db/api docstring)."
  ([conn] (datomic-checkpointer conn {}))
  ([conn {:keys [db-api] :or {db-api db/api}}]
   (let [{:keys [q transact! db pull]} db-api
         ->ckpt (fn [m]
                  {:step (:checkpoint/step m)
                   :state (edn/read-string (:checkpoint/state m))
                   :frontier (edn/read-string (:checkpoint/frontier m))
                   :status (:checkpoint/status m)})]
     (reify Checkpointer
       (-put! [_ tid {:keys [step state frontier status] :as ckpt}]
         (transact! conn
                    [{:db/id (str tid "/" step)
                      :checkpoint/key (str tid "/" step)
                      :checkpoint/thread tid
                      :checkpoint/step step
                      :checkpoint/state (pr-str state)
                      :checkpoint/frontier (pr-str frontier)
                      :checkpoint/status status}])
         ckpt)
       (-get-latest [_ tid]
         (let [dbv (db conn)
               step (q '[:find (max ?s) .
                         :in $ ?tid
                         :where [?e :checkpoint/thread ?tid]
                                [?e :checkpoint/step ?s]]
                       dbv tid)]
           (when step
             (->ckpt (pull dbv '[*] [:checkpoint/key (str tid "/" step)])))))
       (-list-checkpoints [_ tid]
         (let [dbv (db conn)
               es (q '[:find [?e ...]
                       :in $ ?tid
                       :where [?e :checkpoint/thread ?tid]]
                     dbv tid)]
           (->> es
                (map #(->ckpt (pull dbv '[*] %)))
                (sort-by :step)
                vec)))))))
