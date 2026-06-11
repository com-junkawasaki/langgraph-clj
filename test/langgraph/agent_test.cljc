(ns langgraph.agent-test
  (:require [clojure.test :refer [deftest is testing]]
            [langgraph.prebuilt :as prebuilt]
            [langgraph.model :as model]
            [langgraph.message :as msg]
            [langgraph.memory :as memory]
            [langgraph.graph :as g]
            [langgraph.db :as db]
            [langgraph.viz :as viz]))

(def weather-tool
  {:name "get_weather"
   :description "Get current weather for a location"
   :schema {:type "object"
            :properties {:location {:type "string"}}
            :required ["location"]}
   :fn (fn [{:keys [location]}] (str "72F and sunny in " location))})

(deftest react-agent-loop
  (let [m (model/mock-model
           [(msg/ai "" {:tool-calls [{:id "t1" :name "get_weather"
                                      :input {:location "Paris"}}]})
            (msg/ai "It is 72F and sunny in Paris.")])
        agent (prebuilt/create-react-agent {:model m :tools [weather-tool]})
        out (g/invoke agent {:messages [(msg/user "Weather in Paris?")]})
        msgs (:messages out)]
    (is (= [:user :assistant :tool :assistant] (mapv :role msgs)))
    (testing "tool result fed back to the model"
      (is (= "72F and sunny in Paris" (:content (nth msgs 2)))))
    (is (= "It is 72F and sunny in Paris." (:content (msg/last-message msgs))))))

(deftest react-agent-tool-error
  (let [m (model/mock-model
           [(msg/ai "" {:tool-calls [{:id "t1" :name "no_such_tool" :input {}}]})
            (msg/ai "Sorry, I cannot do that.")])
        agent (prebuilt/create-react-agent {:model m :tools [weather-tool]})
        out (g/invoke agent {:messages [(msg/user "hi")]})]
    (is (true? (:error? (nth (:messages out) 2))))))

(deftest datomic-chat-memory
  (let [conn (db/create-conn memory/memory-schema)
        hist (memory/datomic-chat-history conn)]
    ((:append! hist) "th-1" (msg/user "hello"))
    ((:append! hist) "th-1" (msg/ai "hi there"))
    ((:append! hist) "th-2" (msg/user "other thread"))
    (testing "messages come back in order, per thread"
      (is (= ["hello" "hi there"]
             (mapv :content ((:messages hist) "th-1"))))
      (is (= 1 (count ((:messages hist) "th-2")))))
    (testing "history is plain datoms — queryable directly"
      (is (= 2 (db/q '[:find (count ?m) .
                       :in $ ?tid
                       :where [?t :thread/id ?tid]
                              [?m :msg/thread ?t]]
                     (db/db conn) "th-1"))))
    (testing "clear!"
      ((:clear! hist) "th-1")
      (is (empty? ((:messages hist) "th-1")))
      (is (= 1 (count ((:messages hist) "th-2")))))))

(deftest anthropic-wire-format
  (testing "request body shape matches the Messages API"
    (let [body (model/request-body
                [(msg/system "be terse")
                 (msg/user "Weather in Paris?")
                 (msg/ai "" {:tool-calls [{:id "t1" :name "get_weather"
                                           :input {:location "Paris"}}]})
                 (msg/tool-result "t1" "72F")]
                {:tools [weather-tool]})]
      (is (= "claude-opus-4-8" (:model body)))
      (is (= "be terse" (:system body)))
      (is (= [{:name "get_weather"
               :description "Get current weather for a location"
               :input_schema (:schema weather-tool)}]
             (:tools body)))
      (is (= ["user" "assistant" "user"] (mapv :role (:messages body))))
      (is (= "tool_use" (get-in body [:messages 1 :content 0 :type])))
      (is (= "tool_result" (get-in body [:messages 2 :content 0 :type])))))
  (testing "response parsing extracts text + tool calls"
    (let [m (model/parse-response
             {:content [{:type "text" :text "checking"}
                        {:type "tool_use" :id "t9" :name "get_weather"
                         :input {:location "Tokyo"}}]
              :stop_reason "tool_use"
              :usage {:input_tokens 10 :output_tokens 5}})]
      (is (= :assistant (:role m)))
      (is (= "checking" (:content m)))
      (is (= "get_weather" (-> m :tool-calls first :name))))))

(deftest viz-smoke
  (let [agent (prebuilt/create-react-agent
               {:model (model/mock-model [(msg/ai "x")]) :tools []})
        mm (viz/mermaid agent)]
    (is (re-find #"flowchart TD" mm))
    (is (re-find #"agent" mm))
    (is (re-find #"tools --> agent" mm))))
