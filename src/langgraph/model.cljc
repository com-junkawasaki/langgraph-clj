(ns langgraph.model
  "ChatModel protocol + a mock model for tests + an Anthropic Messages
  API adapter.

  WASM premise: this library performs no I/O itself. The Anthropic
  adapter takes host capabilities as injected functions —

    :http-fn    (fn [{:keys [url method headers body]}]
                  → {:status int :body string})
    :json-write (fn [clj-map] → json string)
    :json-read  (fn [json-string] → clj map, keyword keys)

  On a JS/WASM host json defaults to js/JSON; on the JVM inject
  cheshire/data.json. :http-fn must always be injected (fetch on a
  WASM host, an HTTP client on the JVM)."
  (:require [langgraph.runnable :as r]
            [langgraph.tool :as tool]))

(defprotocol ChatModel
  (-generate [model messages opts]
    "messages → assistant message map (see langgraph.message)."))

;; ChatModels are Runnables over a message vector.
(defrecord ModelRunnable [model]
  r/IRunnable
  (-invoke [_ input opts] (-generate model input opts))
  (-stream [_ input opts] [(-generate model input opts)]))

(defn as-runnable [model] (->ModelRunnable model))

(defn bind-tools
  "Returns a model that always passes the given tools."
  [model tools]
  (reify ChatModel
    (-generate [_ messages opts]
      (-generate model messages (assoc opts :tools tools)))))

;; ───────────────────────── mock model ─────────────────────────

(defn mock-model
  "Deterministic model for tests/offline runs. `responses` is a vector
  of assistant messages (or a fn messages→message); consumed in order,
  the last one repeats."
  [responses]
  (let [i (atom -1)]
    (reify ChatModel
      (-generate [_ messages opts]
        (if (fn? responses)
          (responses messages opts)
          (let [n (swap! i inc)]
            (nth responses (min n (dec (count responses))))))))))

;; ───────────────────────── Anthropic adapter ─────────────────────────

(def ^:private anthropic-url "https://api.anthropic.com/v1/messages")
(def default-model "claude-opus-4-8")

(defn- msg->anthropic [{:keys [role content tool-calls tool-call-id error?]}]
  (case role
    :system nil ; hoisted to top-level :system
    :tool {:role "user"
           :content [(cond-> {:type "tool_result"
                              :tool_use_id tool-call-id
                              :content content}
                       error? (assoc :is_error true))]}
    :assistant {:role "assistant"
                :content (vec (concat
                               (when (and content (not= content ""))
                                 [{:type "text" :text content}])
                               (for [{:keys [id name input]} tool-calls]
                                 {:type "tool_use" :id id :name name :input input})))}
    :user {:role "user" :content content}))

(defn- merge-consecutive
  "The API rejects consecutive same-role messages only loosely (it
  combines them), but tool_result blocks for one assistant turn must
  land in a single user message — merge adjacent same-role entries."
  [msgs]
  (reduce (fn [acc m]
            (let [prev (peek acc)]
              (if (and prev (= (:role prev) (:role m))
                       (vector? (:content prev)) (vector? (:content m)))
                (conj (pop acc) (update prev :content into (:content m)))
                (conj acc m))))
          [] msgs))

(defn request-body
  "Builds the Anthropic Messages API request body (a Clojure map) from
  langgraph messages + opts. Exposed for testing."
  [messages {:keys [model max-tokens tools system] :as _opts}]
  (let [sys (or system
                (some #(when (= :system (:role %)) (:content %)) messages))
        body {:model (or model default-model)
              :max_tokens (or max-tokens 16000)
              :messages (->> messages
                             (keep msg->anthropic)
                             merge-consecutive
                             vec)}]
    (cond-> body
      sys (assoc :system sys)
      (seq tools) (assoc :tools (mapv tool/->anthropic tools)))))

(defn parse-response
  "Anthropic response map → assistant message. Exposed for testing."
  [{:keys [content stop_reason usage] :as resp}]
  (when (= "refusal" stop_reason)
    (throw (ex-info "Model refused request" {:type :refusal :response resp})))
  (let [text (apply str (keep #(when (= "text" (:type %)) (:text %)) content))
        calls (vec (keep #(when (= "tool_use" (:type %))
                            {:id (:id %) :name (:name %) :input (:input %)})
                         content))]
    (cond-> {:role :assistant :content text :stop-reason stop_reason}
      (seq calls) (assoc :tool-calls calls)
      usage (assoc :usage usage))))

(defn anthropic-model
  "Anthropic Messages API chat model.

    (anthropic-model {:api-key …
                      :model \"claude-opus-4-8\"
                      :http-fn host-fetch
                      :json-write … :json-read …})"
  [{:keys [api-key model max-tokens http-fn json-write json-read url]
    :or {model default-model
         url anthropic-url
         #?@(:cljs [json-write (fn [m] (js/JSON.stringify (clj->js m)))
                    json-read (fn [s] (js->clj (js/JSON.parse s) :keywordize-keys true))])}}]
  (when-not http-fn
    (throw (ex-info ":http-fn must be injected (host capability)" {})))
  (when-not (and json-write json-read)
    (throw (ex-info ":json-write/:json-read must be injected on this host" {})))
  (reify ChatModel
    (-generate [_ messages opts]
      (let [body (request-body messages (merge {:model model :max-tokens max-tokens} opts))
            {:keys [status] resp-body :body}
            (http-fn {:url url
                      :method :post
                      :headers {"content-type" "application/json"
                                "x-api-key" api-key
                                "anthropic-version" "2023-06-01"}
                      :body (json-write body)})]
        (when-not (and status (<= 200 status 299))
          (throw (ex-info "Anthropic API error" {:status status :body resp-body})))
        (parse-response (json-read resp-body))))))
