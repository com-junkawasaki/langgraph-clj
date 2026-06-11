(ns langgraph.tool
  "Tool definitions for tool-calling models.

  A tool is a plain map:

    {:name \"get_weather\"
     :description \"Get current weather for a location\"
     :schema {:type \"object\"
              :properties {:location {:type \"string\"}}
              :required [\"location\"]}
     :fn (fn [{:keys [location]}] …)}")

(defn tool
  [{:keys [name description schema fn] :as t}]
  {:pre [(string? name) (ifn? fn)]}
  (merge {:description "" :schema {:type "object" :properties {}}} t))

(defn execute
  "Executes one tool call {:id .. :name .. :input ..} against the tool
  list. Returns a :role :tool message. Errors are captured as
  {:error? true} tool results so the model can react (matches the
  Anthropic tool_result is_error contract)."
  [tools {:keys [id name input]}]
  (if-let [t (some #(when (= name (:name %)) %) tools)]
    (try
      {:role :tool :tool-call-id id :content (str ((:fn t) input))}
      (catch #?(:clj Exception :cljs :default) e
        {:role :tool :tool-call-id id :error? true
         :content (str "Error: " (ex-message e))}))
    {:role :tool :tool-call-id id :error? true
     :content (str "Error: unknown tool " name)}))

(defn execute-all
  "Executes every tool call in an assistant message; returns the
  :role :tool messages in call order."
  [tools assistant-msg]
  (mapv #(execute tools %) (:tool-calls assistant-msg)))

(defn ->anthropic
  "Converts a tool to the Anthropic Messages API wire format."
  [t]
  {:name (:name t)
   :description (:description t)
   :input_schema (:schema t)})
