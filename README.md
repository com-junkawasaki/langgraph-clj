# langgraph-clj

LangGraph / LangChain-style LLM orchestration in **portable Clojure** —
zero dependencies, every namespace is `.cljc`, designed to run on
**Clojure-on-WASM hosts** (SCI, ClojureScript, GraalVM) as well as the
JVM, with all state persisted through a **Datomic API**.

```
src/langgraph/
  db.cljc          Datomic-API-compatible EAV store + Datalog + pull (swappable)
  runnable.cljc    LCEL Runnables: pipe / parallel / branch / retry / fallbacks
  message.cljc     chat message data model (Anthropic-shaped maps)
  prompt.cljc      {var} templates, chat templates, placeholders
  model.cljc       ChatModel protocol, mock model, Anthropic adapter (I/O injected)
  tool.cljc        tool definitions + execution + wire-format conversion
  parser.cljc      str / edn / json output parsers
  memory.cljc      chat history as datoms
  graph.cljc       StateGraph + Pregel superstep loop + interrupts
  checkpoint.cljc  checkpointers (in-memory / Datomic) — resume & time travel
  prebuilt.cljc    create-react-agent
  viz.cljc         graph → Mermaid
```

## Design

- **WASM premise** — no JVM interop, no threads, no wall clock. The
  library does no I/O: HTTP and JSON are *injected host capabilities*
  (fetch on a WASM host, any client on the JVM).
- **Datomic API premise** — checkpoints and chat history are datoms.
  A minimal Datomic-compatible store (datalog `q`, `pull`, upsert,
  cardinality-many, lookup refs, `as-of`) is bundled; real Datomic
  Local or DataScript drops in via the `langgraph.db/api` function map.
  Graph execution history becomes a queryable fact log — time travel,
  audits, and cross-thread views are Datalog queries (ADR-0010 pattern).

## Quickstart

```clojure
(require '[langgraph.graph :as g]
         '[langgraph.prebuilt :as prebuilt]
         '[langgraph.model :as model]
         '[langgraph.message :as msg]
         '[langgraph.checkpoint :as cp]
         '[langgraph.db :as db])

;; --- a graph with reducer channels, conditional edges, interrupts ---
(def graph
  (-> (g/state-graph {:channels {:messages {:reducer into :default []}}})
      (g/add-node :draft (fn [s] {:messages [(msg/ai "draft…")]}))
      (g/add-node :send  (fn [s] {:messages [(msg/ai "sent")]}))
      (g/set-entry-point :draft)
      (g/add-edge :draft :send)
      (g/compile-graph
       {:checkpointer (cp/datomic-checkpointer (db/create-conn cp/checkpoint-schema))
        :interrupt-before #{:send}})))   ; human-in-the-loop

(g/run* graph {:messages [(msg/user "hello")]} {:thread-id "t1"})
;; => {:status :interrupted …}  — review, optionally edit:
(g/update-state! graph "t1" {:messages [(msg/user "approved")]})
(g/run* graph nil {:thread-id "t1"})     ; resume
;; => {:status :done …}

;; --- ReAct agent ---
(def agent
  (prebuilt/create-react-agent
   {:model (model/anthropic-model
            {:api-key API-KEY
             :model "claude-opus-4-8"
             :http-fn host-fetch          ; injected host capability
             :json-write … :json-read …}) ; defaults to js/JSON on cljs
    :tools [{:name "get_weather"
             :description "Get current weather for a location"
             :schema {:type "object"
                      :properties {:location {:type "string"}}
                      :required ["location"]}
             :fn (fn [{:keys [location]}] …)}]}))

(g/invoke agent {:messages [(msg/user "Weather in Paris?")]})
```

Everything is a Runnable — plain fns, keywords, and maps compose
directly:

```clojure
(require '[langgraph.runnable :as r] '[langgraph.prompt :as p])

(r/invoke (r/pipe (p/template "Translate to French: {text}")
                  (model/as-runnable claude)
                  msg/text)
          {:text "hello"})
```

Chat history and checkpoints are plain datoms, so views are queries:

```clojure
(db/q '[:find ?thread (count ?m)
        :where [?t :thread/id ?thread] [?m :msg/thread ?t]]
      (db/db conn))
```

## Mapping from upstream

See [docs/adr/0001-architecture.md](docs/adr/0001-architecture.md) for
the full LangGraph/LangChain → langgraph-clj correspondence table and
the rationale for the zero-dependency / injected-I/O design.

## Tests / example

```sh
clojure -M:test                                  # 21 tests, 78 assertions
clojure -Sdeps '{:paths ["src" "examples"]}' \
        -M -e "(require 'react-agent) (react-agent/-main)"
```
