# hcodex

把 [codex](https://github.com/openai/codex) 包装成高可用、多人聊天的 agent 服务。名字既是 hi-codex，也是 ha-codex。

## 分层

```
多人聊天层（上，计划中）  参与者、谁能发言、发起人 turn/start / 其他人排队、按输入记录作者
      │ 只依赖下层和上游已有的 API
高可用层（下，ha/）        PG thread store、跨 pod 交接、thread 绑定远程沙箱
      │ 通过 codex fork 的注册口接入
codex fork                 jingyugao/codex：per-turn env（fork.env.*）+ 注册口，尽量薄
```

- 下层不知道上层；上层不需要再改 codex。
- 鉴权、飞书、记忆等业务逻辑留在使用方（例如 Colony）。

| 目录 | crate | 内容 |
|---|---|---|
| `ha/pg` | `hcodex-pg` | PG thread store（实现上游 `ThreadStore`）、交接记录、沙箱绑定记录 |
| `ha/ext` | `hcodex-ha` | 跨 pod 交接、沙箱绑定（codex 的 thread extension） |
| `server` | `hcodex-server` | 二进制 `codex-app-server`：上游入口原样复制，启动前注册高可用层（`server/src/ha.rs`） |

## 依赖 codex

- `codex.rev` 固定依赖的 `jingyugao/codex` 提交；源码放在 `./codex`（已在 `.gitignore` 里）。CI 按 `codex.rev` 检出；本地可以 clone，或者软链接到已有的 checkout：

  ```sh
  git clone https://github.com/jingyugao/codex codex && git -C codex checkout "$(cat codex.rev)"
  # 或：ln -s ~/Code/codex-fork codex
  ```

- codex 的 crate 用 path 依赖引用（`Cargo.toml` 的 `[workspace.dependencies]`）。
- Cargo 只认根 workspace 的 `[patch]`，所以 hcodex 要重复 codex 的 `[patch]`；`Cargo.lock` 也以 codex 的为基础，保证依赖版本一致。升级 codex：改 `codex.rev`，把 `./codex` 切到该提交，运行 `scripts/sync-codex.sh`，它会同步 `[patch]`、`[workspace.lints]`、`rust-toolchain.toml`、`clippy.toml`、`rustfmt.toml` 和 `Cargo.lock`。
- `server/src/main.rs` 是上游 `app-server/src/main.rs` 的副本（只多了注册那两行），升级时对照上游更新。
- `codex-exec-server`（沙箱）和 `codex-code-mode-host`（Code Mode，必须和 app-server 放在同一目录）是上游原版，用 codex 自己的 workspace 编译：

  ```sh
  cargo build --manifest-path codex/codex-rs/Cargo.toml -p codex-exec-server-cli --bin codex-exec-server
  cargo build --manifest-path codex/codex-rs/Cargo.toml -p codex-code-mode-host --bin codex-code-mode-host
  ```

  code-mode-host 需要预编译的 V8，见 `.github/workflows/ci.yml` 中的 "Fetch prebuilt V8"。

## 功能

per-turn env（`fork.env.*`）由 codex fork 提供，见 fork 仓库的 `docs/fork/handoff.md`。

数据库对象沿用旧名字（表 `codex_fork_*`、NOTIFY 通道 `codex_fork_handoff`），与已有数据兼容。

### PostgreSQL thread store

环境变量（`config.toml` 里的 `experimental_thread_store` 保持默认）：

```sh
HCODEX_THREAD_STORE=postgres://user:pass@host:5432/db
```

- 实现上游的 `ThreadStore` trait（16 个必需方法），表结构见 `ha/pg/migrations/`，首次使用时自动迁移。
- 所有权防护：每个 thread 有 `owner_id` + `epoch`。resume 时 `epoch+1`，旧 owner 的写入返回 `Conflict`，防止两个 pod 同时写。
- URL 无效时启动直接 panic，**不会退回本地存储**。
- 未实现（返回 Unsupported 或空）：sections、attachments、projects、全文搜索、按路径读、paginated 历史。
- 排队（`thread/queue/*`）：使用本机 sqlite 队列库（上游 `Local` 配置下的 `LocalQueueStore`），不在 PG 中。队列只对本 pod 可见；`CODEX_SQLITE_HOME` 在 emptyDir 上时，pod 重建后队列丢失，在 PVC 上则保留。删除 PG thread 时不会清理它的队列项。
- 本地仍有的文件：app-server 自身的 sqlite（日志等）。部署时建议：`history.persistence = "none"`，`CODEX_SQLITE_HOME` 指向 emptyDir，控制日志级别。

### 跨 pod 交接（v1）

流程：

1. A 收到 SIGTERM（或 NOTIFY）后，先把交接记录写进存储（turn_id、environment 选择、per-turn env），再调用上游的 `suspend_turn_and_shutdown`：flush 历史，停止 turn，但不写终止事件。
2. 上游的 graceful drain 看到没有运行中的 turn，A 正常退出。
3. 客户端连到 B 并调用 `thread/resume`。B 在 idle 回调里认领记录，调用上游的 `recover_turn_if_idle`，以**同一个 turn_id** 继续执行。

v1 是"立即停"：进行中的工具调用会被中断，B 上模型看到中断后自行决定是否重试。所以只适合滚动发布、挑空闲时段的场景。"等到 step 边界再停"（v2）未实现。

环境变量：

| 变量 | 含义 |
|---|---|
| `HCODEX_HANDOFF_STORE` | `postgres://…` 或 `file:<dir>`；不设则交接功能关闭 |
| `HCODEX_HANDOFF_ON_SIGTERM` | `1`：SIGTERM 后经过宽限期，挂起仍在运行的 turn |
| `HCODEX_HANDOFF_GRACE_SECS` | 宽限期，默认 30，让 turn 有机会自然结束 |
| `HCODEX_OWNER_ID` | 本进程身份，默认取 `HOSTNAME` |

单个 thread 迁移（仅 Postgres store）：`NOTIFY codex_fork_handoff, '<thread_id>'`，加载着该 thread 的 pod 会挂起它。断线期间的通知会丢失，控制器需要重试，直到 owner 变化。

注意：

- 只在 WebSocket 模式下生效。stdio 模式下上游不做 graceful drain。
- 只有根 thread 的普通 turn 可以迁移。子 agent 会被中断，不做恢复。
- remote environment 的配置不随交接迁移：B 必须能识别同一个 environment_id。开启下面的沙箱绑定后自动处理；否则需要 `environments.toml` 或 `environment/add`。
- k8s 的 `terminationGracePeriodSeconds` 要大于 `HCODEX_HANDOFF_GRACE_SECS` 加挂起所需时间。

### thread 绑定远程沙箱

一个 thread 终身绑定一个远程沙箱（`codex-exec-server`）。绑定后不可修改；`thread/fork` 出来的 thread 继承源 thread 的沙箱。实现：`ha/ext/src/sandbox.rs`。

| 变量 | 含义 |
|---|---|
| `HCODEX_SANDBOX_URL_TEMPLATE` | 开启功能。沙箱地址模板，`{id}` 替换为 environment id，例如 `ws://sandbox-proxy/sandboxes/{id}`。由代理把 id 映射到实际沙箱 |
| `HCODEX_SANDBOX_STORE` | 绑定记录存储，`postgres://…` 或 `file:<dir>`（文件在 `<dir>/sandboxes/`）；不设时用 `HCODEX_HANDOFF_STORE` |
| 关闭本地环境 | **必须**：有 `environments.toml` 时写 `include_local = false`；没有时设 `CODEX_EXEC_SERVER_URL=none`。设置了模板而进程里仍有本地环境时，app-server 启动后立即以退出码 78 退出，避免命令落到 app-server 容器里执行 |

两种用法：

**A. 虚拟槽位（推荐）**：`environments.toml` 静态列出固定数量的槽位，地址都指向同一个网关，网关按 query 参数 `slot` 把槽位一致性 hash 到实际沙箱（Envoy：`hash_policy: [{query_parameter: {name: slot}}]`）。exec-server 只路由 `/` 且不看 query，所以网关不需要改写路径。客户端不调用 `environment/add`，只在创建 thread 时选一个槽位：

```toml
# $CODEX_HOME/environments.toml，由脚本生成 slot-0 … slot-1023
include_local = false
[[environments]]
id = "slot-0"
url = "ws://sandbox-gw:4910/?slot=slot-0"
```

```
thread/start {…, environments:[{environmentId:"slot-<hash(workspaceId) % 1024>", cwd:"/workspaces/<id>"}]}
```

- 槽位数固定，所以每个 app-server 的连接数有上限，不随 thread 增长；远程环境的连接是用到时才建立的，启动时不会连 1024 个槽位。加减沙箱只改网关后面的副本数，不改这个文件。
- 模板也按同样的格式配（`ws://sandbox-gw:4910/?slot={id}`），与静态地址保持一致。槽位已经在配置里登记，加载 thread 时不会重新登记。

**B. 每个 thread 一个环境**：创建时登记一次。

```
environment/add {environmentId:"sbx-<id>", execServerUrl:<按模板拼出的地址>}
thread/start    {…, environments:[{environmentId:"sbx-<id>", cwd:"/workspace"}]}
```

- 注意：上游的 `environment/add` 是**替换**语义，同一个 id 再登记一次会新建连接，可能中断正在执行的命令，所以 id 不要在 thread 之间复用。
- 登记的环境不会删除，app-server 的连接数随 thread 增长，需要定期重启或另行回收。

行为：

- thread 就绪时（`on_thread_ready`），如果它选中了远程环境，就写入绑定记录（PG 表 `codex_fork_thread_sandboxes`，只写一次，先写者生效）。没有选中远程环境、但由 `thread/fork` 产生的 thread，复制源 thread 的绑定。
- 每次加载 thread（新建、fork、resume，包括 pod 重启和交接后的 cold resume）：本进程没有注册过该 environment id 时，按模板注册；然后把它恢复成 thread 的 sticky 环境选择。这些都在 resume 响应返回之前完成，所以之后的 turn 和排队出队的 turn 都会用这个沙箱。
- 沙箱重启（地址不变）时，exec-server 客户端在下一次操作时自动重连。挂掉那一刻正在执行的命令会丢，文件不受影响。
- 只绑定远程环境；本地环境（`local`）不会被记录。
- 不做鉴权（由 k8s 网关负责），记录里没有密钥。
- 不校验客户端传入的 `turn/start.environments`；客户端需要保证不传别的环境。

## 测试

```sh
export CARGO_TARGET_DIR=$PWD/target
cargo build -p hcodex-server --bin codex-app-server
cargo build --manifest-path codex/codex-rs/Cargo.toml -p codex-exec-server-cli --bin codex-exec-server
cargo build --manifest-path codex/codex-rs/Cargo.toml -p codex-code-mode-host --bin codex-code-mode-host  # 可选，code mode 用例需要
HCODEX_PG_TEST_URL=postgres://user:pass@host:5432/db cargo test -p hcodex-pg -p hcodex-ha  # 不设 URL 时 PG 用例跳过
cargo clippy --workspace --all-targets -- -D warnings
```

三个二进制要在同一个 target 目录里，e2e 测试从那里找它们。

| 测试 | 覆盖 |
|---|---|
| `ha/pg/tests/pg_store.rs` | 与 InMemory 行为对齐的往返、列表/归档/删除、旧 owner 被拒、交接只被认领一次 |
| `ha/ext/tests/handoff_e2e.rs` | file store：SIGTERM → 同一 turn_id 在另一进程恢复，`echo $TRACE_ID` 带 per-turn env；两个独立 `CODEX_HOME` 只共享 PG：恢复成功，且两边都没有对话文件 |
| `ha/ext/tests/sandbox_e2e.rs` | 真实 `codex-exec-server` 作沙箱：绑定 → 杀掉进程 A → B 直接 resume，命令仍在沙箱执行 → fork 出的 thread 继承沙箱；静态槽位不需要 `environment/add`；本地环境未关闭时以退出码 78 退出；code mode：一个 JS cell 在沙箱里执行两条命令并带上 `fork.env.*` |
| `ha/ext/tests/queue_env.rs` | 上游队列项 metadata：`queue/start` 和空闲派发都把 `fork.env.*` 带到 turn env；update 保留 |

e2e 测试同时检查 codex fork 的 per-turn env 是否真的到达命令和 hook；升级 codex 后必须跑。

## 后续

- [ ] release：musl 静态包（`codex-app-server`、`codex-exec-server`、`codex-code-mode-host`），release 构建开 LTO。
- [ ] PG 持久化的排队（目前排队在 pod 本地 sqlite，pod 挂了或 thread 迁走会丢）。
- [ ] 交接 v2：在 step 边界挂起。
- [ ] 多人聊天层。
