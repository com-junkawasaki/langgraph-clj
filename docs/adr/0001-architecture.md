# ADR-0001: langgraph-clj — portable Clojure, Datomic-API-first LLM orchestration

- Status: Accepted (2026-06-11)
- 関連: kawasakijun ADR-0010 (Life Graph — EDN事実層 + Datalogビュー), ADR-0002 (Pregel DAG)

## 課題

LangGraph / LangChain 相当のエージェントオーケストレーションを、

1. **Clojure WASM で動く前提**(SCI / CLJS / GraalVM いずれのホストでも)で、
2. **Datomic API 前提**(状態・履歴・チェックポイントを EAV ファクトとして保持、ADR-0010 と同型)

で実装したい。Python の langgraph はランタイム・依存ともに重く、状態は pickle されたバイナリで、ワークスペースの「1事実1表現 / Datalog ビュー」原則に反する。

## 決定

### 1. 全コード .cljc・依存ゼロ・ホスト能力は注入

- ランタイム依存 0 個。`deps.edn` の `:deps {}`。
- JVM interop なし、スレッドなし、core.async なし、wall clock / 乱数なし。
- I/O(HTTP・JSON)は**ホスト能力として注入**する:
  `(anthropic-model {:http-fn … :json-write … :json-read …})`。
  WASM ホストでは fetch / JSON binding を、JVM では任意の HTTP client を渡す。
- 並行性は持たない。Pregel superstep 内のノード実行は逐次(意味論として並列)。

### 2. Datomic API のサブセットを内蔵し、差し替え可能にする

`langgraph.db` は Datomic 互換 API(`create-conn / transact! / q / pull /
entity / entid / as-of`)を持つ最小 EAV ストア + Datalog エンジン
(パターン・述語・関数束縛・not/or・集約・pull・reverse ref・
unique/identity upsert・cardinality many・lookup ref)。

上位層(`langgraph.checkpoint` / `langgraph.memory`)は
`{:q :transact! :db :pull :entid}` の関数マップ(`langgraph.db/api`)経由で
ストアに触る。本物の Datomic Local / DataScript は同シェイプのマップを
渡すだけで差し替えられる。

### 3. 状態・履歴・チェックポイントはすべて datom (ADR-0010 L1)

- **チェックポイント** = superstep ごとの entity
  (`:checkpoint/thread :checkpoint/step :checkpoint/state …`)。
  resume / human-in-the-loop / time travel は Datalog クエリで落ちてくる。
- **チャット履歴** = thread entity + message entity。
  「昨日の全 tool エラー」のような横断ビューが名前付きクエリになる。

### 4. LangGraph / LangChain との対応

| upstream | langgraph-clj |
|---|---|
| `StateGraph` / `add_node` / `add_edge` / `add_conditional_edges` | `langgraph.graph` 同名関数 |
| channel reducer (`Annotated[list, add]`) | `{:channels {:messages {:reducer into :default []}}}` |
| Pregel superstep / recursion_limit | `run-loop` (`:recursion-limit`, default 25) |
| checkpointer / thread_id / time travel | `langgraph.checkpoint` (mem / Datomic) |
| `interrupt_before/after` + `update_state` | `:interrupt-before/after` + `update-state!` |
| LCEL Runnable / pipe / RunnableParallel / fallbacks | `langgraph.runnable` (fn・map・keyword がそのまま Runnable) |
| PromptTemplate / ChatPromptTemplate / MessagesPlaceholder | `langgraph.prompt` |
| ChatModel / bind_tools | `langgraph.model` (protocol + mock + Anthropic adapter) |
| tools / ToolNode | `langgraph.tool` |
| output parsers | `langgraph.parser` |
| `create_react_agent` | `langgraph.prebuilt/create-react-agent` |
| `get_graph().draw_mermaid()` | `langgraph.viz/mermaid` |

## 非スコープ (v0.1)

- Datalog rules (%)・複数 db ソース・`d/history`(必要なら本物の Datomic を差す)
- 真の並列 superstep・ストリーミングトークン(チャンク列としての `stream` のみ)
- Send API / subgraph(次版候補)

## 帰結

- 同一コードが JVM / SCI / CLJS / WASM で動く。テストは JVM (`clojure -M:test`) で実行。
- エージェントの全実行履歴がワークスペースの事実層(ADR-0010)と同じ表現になり、
  m365-archive 等の既存 datom と join できる。
