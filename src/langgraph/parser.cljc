(ns langgraph.parser
  "Output parsers — Runnables that turn assistant messages into data."
  (:require #?(:clj [clojure.edn :as edn]
               :cljs [cljs.reader :as edn])
            [clojure.string :as str]
            [langgraph.message :as msg]))

(defn str-parser
  "Assistant message → plain text."
  []
  (fn [m] (msg/text m)))

(defn edn-parser
  "Assistant message → EDN. Strips ```edn fences if present."
  []
  (fn [m]
    (let [s (-> (msg/text m)
                (str/replace #"(?s)```(?:edn|clojure)?\s*(.*?)```" "$1")
                str/trim)]
      (edn/read-string s))))

(defn json-parser
  "Assistant message → data, using a host-injected json-read fn
  (no JSON parser is bundled — WASM premise)."
  [json-read]
  (fn [m]
    (let [s (-> (msg/text m)
                (str/replace #"(?s)```(?:json)?\s*(.*?)```" "$1")
                str/trim)]
      (json-read s))))
