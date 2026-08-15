# レプリケーション TCP / RDMA 両経路のテストカバレッジ棚卸し

* 作成: 2026-08-15
* 出自: [20260713-rdma-tcpless-replication.md](20260713-rdma-tcpless-replication.md) §10 の TODO
  「TCP と RDMA で同等機能を持つものについて、両方の経路がテストされていることを確認し、
  不足分のテストを追加する」の棚卸し成果物。
* 方法: src (datastore / datastore_impl / log_channel / replica_server / rdma 配下) の
  経路分岐点をコードから列挙し、test/limestone/replication/ の全テストファイル
  (棚卸し時点 44 本、ハイブリッドテスト削除後 40 本) + replication 外の関連テスト
  (datastore_impl/ 等) を突き合わせた。
* 改訂 (2026-08-15): 棚卸し初版のレビュー中に「ハイブリッド構成は製品として提供しない」
  という上位計画の決定が判明し、専用コード・テストの削除と起動時チェックの追加を実施した
  (計画書 §2.1 の追記参照)。本文書はその決定を反映した改訂版である。ギャップ判断は
  ユーザー確認済み: G4 は要追加 (代替)、G5 は追加不要、G6/G7 は要追加、G1/G2 は対象消滅。

## 1. 前提: 「経路」の定義

コード上の分岐軸は 3 つある。

| 軸 | 実体 | 決まり方 |
|---|---|---|
| 設定モード | `replication_config::mode()` = none / tcp / rdma | `TSURUGI_REPLICATION_HANDSHAKE_SOCKET` の有無 (`replication_config_loader.cpp`) |
| RDMA 有効フラグ | `datastore_impl::is_rdma_enabled()` | `REPLICATION_RDMA_SLOTS` の有無。モード判定には使わない |
| チャネル実効経路 | `log_channel_impl::get_replica_mode_locked()` | `rdma_send_stream_` があれば rdma、なければ `replica_connector_` で tcp |

サポートする構成は 2 つ: **純 TCP** (ENDPOINT のみ) と **純 RDMA** (HANDSHAKE_SOCKET +
SERVICE_ID + RDMA_SLOTS = TCP レス)。かつて存在した**ハイブリッド** (ENDPOINT +
RDMA_SLOTS = 制御チャネル TCP + データチャネル RDMA) は製品として提供しないことが
上位計画で決定しており、専用コードは削除済み・この組み合わせの設定は起動時エラーになる。

## 2. テスト資産の全体像

test/limestone/replication/ の 40 ファイル (ハイブリッドテスト削除後) と、
replication 外の関連 3 ファイル。

| 分類 | 本数 | 代表 |
|---|---|---|
| 経路非依存の単体 (モード設定なし) | 19 | message_* 系ワイヤコーデック、rdma_log_entries_parser/receiver、rdma_log_channel_receiver、rdma_send_stream_base、rdma_handshake_payload (RDMA 名でも実スタック不要、OFF ビルドでも走る) |
| TCP モード | 15 | log_channel_replication_test (E2E 15 本)、scenario_test (thread/process)、datastore_replication_test、replica_server_connector_test、channel_handler 系 |
| RDMA モード (ファイル全体 `#ifdef LIMESTONE_ENABLE_RDMA`) | 6 | 実 daemon 4 本 + scenario_tcpless_rdma_test + mock/factory フック 1 本 (rdma_establish_session_mock_test) |
| replication 外 | 3 | datastore_impl/replication_flag_test (async フラグ + RDMA_SLOTS 検証全ケース)、datastore_impl/rdma_sender_test、datastore_impl/datastore_impl_test (group commit フック) |

(ハイブリッド構成の廃止に伴い、scenario_test の rdma 2 パラメタ、
datastore_replication_test の rdma_128 パラメタ、ハイブリッド専用メッセージの単体テスト
4 本 — message_rdma_init / init_ack / finalize / finalize_ack — は削除した。)

E2E の柱: 純 TCP は scenario_test、純 RDMA (TCP レス) は
scenario_tcpless_rdma_test (process / thread 両形態、TCP ソケット 0 本の検証つき)。

## 3. 機能 × 経路マトリクス

凡例 — ✅: 担保あり / △: 部分的 (備考参照) / ❌: なし / N/A: その経路に機能が存在しない。
「レベル」は 単体 (ワイヤ/値/ロジック) / C (コンポーネント) / E2E。

| # | 機能 | TCP 経路 | RDMA 経路 | 備考 |
|---|---|---|---|---|
| 1 | 設定ロード・モード判定 | ✅ 単体: replication_config_test / loader_test / endpoint_test。TCP モード + RDMA_SLOTS 併用 (旧ハイブリッド) の起動時拒否も loader_test で担保 | ✅ 単体: 同 2 本の `#ifdef` 両分岐 + replication_flag_test (SLOTS 不正 9 ケース) | OFF ビルドで RDMA 設定が拒否されることも担保 |
| 2 | セッション確立 (master 側) | ✅ C+E2E: datastore_replication_test (open 成功/失敗/ready 経由/EXPECT_DEATH) | ✅ C+E2E: rdma_establish_session_test (実 daemon 4 本) + rdma_establish_session_mock_test (失敗 15 + 成功 1、ロールバック検証つき) | |
| 3 | セッション確立 (replica 側) | ✅ C: control_channel_handler_test (7)、replica_server_connector_test (8)、replica_server_test | ✅ E2E: rdma_establish_replica_session_test (拒否 5 種 + 成功 + master 死亡時解放) | |
| 4 | handshake 層 (daemon 経由) | N/A | ✅ 単体+E2E: rdma_handshake_payload_test (コーデック 13)、rdma_comm_handshake_test (実 daemon 完全往復) | RDMA 固有機能 |
| 5 | (削除済み) TCP 制御チャネル上の RDMA_INIT / RDMA_FINALIZE | — | — | ハイブリッド専用機能だったため、コード・テストとも削除 (replica 側の initialize_rdma / finalize_rdma 本体は純 RDMA の establish が使う共用部として存続し、replica_server_test が担保) |
| 6 | log_channel 登録台帳 | ✅ C: replica_server_connector_test (slot 登録 / 重複 FATAL / id オーバーフロー FATAL) | ✅ E2E: rdma_establish_replica_session_test 経由 | TCP 台帳と RDMA 台帳の相互排他 FATAL は未テストだが、構成が排他 (純 TCP か純 RDMA) になったため実運用で到達不能な防御コード。追加しない |
| 7 | WAL エントリ送信 (add/remove_entry、add/remove/truncate_storage) | ✅ E2E: log_channel_replication_test が全 entry 種別を replica pwal 内容一致まで検証 | ✅ 単体+C+E2E: 送信 = rdma_send_stream_base_test + log_channel_replication_test の fake stream (serializer 再利用 / 閾値 flush / 非同期 flush)。受信 = rdma_log_entries_parser_test (16) / receiver_test (7) / rdma_log_channel_receiver_test (16) が全 entry 種別。E2E = scenario_tcpless (normal entry) | **[G3]** RDMA の E2E は normal entry (+G4 対応後は blob) のみ。全 entry 種別の E2E は TCP のみ (RDMA 側は単体・C で担保) |
| 8 | BLOB 送信 | ✅ 単体+E2E: tcp_replication_message_io_test (6) + scenario_test tcp (blob 実ファイル内容一致) | ✅ 単体+C+E2E: parser の blob ストリーミング (ゼロ長/複数/部分到着/サイズ不一致) + log_channel_replication_test (pending 先行 flush + 内容伝送) + scenario_tcpless (blob_flows_without_tcp、実ファイル内容一致) | **[G4]** 対応済み (単位 J2)。旧ハイブリッド blob E2E の代替 |
| 9 | セッション begin/end と完了同期 | ✅ E2E: log_channel_replication_test (`wait_for_replica_ack()` = COMMON_ACK) | ✅ C+E2E: 同 (flush / 非同期 flush) + scenario_tcpless (end_session 復帰 = replica 書き込み済みの durability) | async フラグ (`REPLICATION_ASYNC_*`) のパースは replication_flag_test で担保 |
| 10 | group commit 伝播と完了同期 | ✅ 単体+E2E: message_group_commit_test (post_receive で epoch 永続) + scenario_test (epoch 一致) + datastore_impl_test (master フラグ/フック) | ✅ E2E: scenario_tcpless (RDMA 制御チャネル経由、flush=ACK 写像、連続制御フレームのシーケンス進行、master/replica epoch 一致) | 失敗系は #13 参照 |
| 11 | RDMA 制御チャネルのフレーム検証 (version / payload_size / partial フラグ / シーケンス不一致 → FATAL、型違い / キャスト失敗 → drop) | N/A (TCP はストリームでフレームなし) | ✅ C: replica_server_test (FATAL 4 分岐の EXPECT_DEATH + 型違い drop 後の継続 + malformed payload FATAL + 正常ルーティングで epoch 永続) + E2E: scenario_tcpless の連続フレーム | **[G6]** 対応済み (単位 J3)。キャスト失敗分岐は factory 経由で到達不能な防御コードのため対象外 |
| 12 | RDMA データチャネルのフレーム検証 | N/A | ✅ C: rdma_log_channel_receiver_test (シーケンスギャップ drop / 重複 drop / version・size FATAL / 部分フレーム完結) | |
| 13 | 失敗時挙動の経路差 (group commit) | ❌ 送信失敗 = ERROR + 継続、ACK 待ち失敗 = `close_session()` + `replica_exists_=false` (replica 切り離しで master 続行) が未テスト | ❌ 送信/flush 失敗 = FATAL (death test なし) | **[G5]** テスト追加はしない (2026-08-15 ユーザー判断)。RDMA の FATAL は仕様 (送信リング全チャネル共有のため切り離し不能) |
| 14 | RDMA チャネル id ディスパッチ (制御 id 振り分け / 範囲外 drop / 未登録 drop) | N/A (1 接続 = 1 チャネル) | ✅ C: replica_server_test (data event → receiver 呼び出し、error event は呼ばない、範囲外 id drop、未登録 receiver drop、制御 id 振り分け) | **[G7]** 対応済み (単位 J3) |
| 15 | シャットダウン / リソース解放 | ✅ C+E2E: replica_server_test (listener/accept/受信ブロック中の shutdown 等) + 全 E2E の teardown | ✅ C+E2E: datastore_impl_test / rdma_sender_test (initialize/shutdown 対) + establish 失敗ロールバック (mock_test のデストラクタ計数、replica 側は master 死亡時解放) | replica の clean shutdown (RDMA) は機能未実装 — 切り離しプロトコル (別タスク) のスコープ |
| 16 | 接続断・受信異常 | ✅ C: replica_connector_test (即切断で null 等)、replica_server_test (client 切断)、datastore_replication_test (replica 死亡 EXPECT_DEATH) | △ establish 中の異常は #2/#3 で担保。確立後の受信スレッド例外 → FATAL 系は未テスト | FATAL 系 death test の費用対効果は #13 と合わせて判断 |

## 4. ギャップ一覧と分類

### (a) テスト追加 (実施済み)

| ID | 内容 | テスト形態 |
|---|---|---|
| G4 | 純 RDMA (TCP レス) 構成での blob E2E | scenario_tcpless_rdma_test の blob_flows_without_tcp (単位 J2)。旧ハイブリッド scenario_test rdma の blob E2E の代替 |
| G6 | RDMA 制御チャネルのフレーム検証異常系 (`replica_server::handle_rdma_control_event()`) | replica_server_test に相乗り (単位 J3)。public な `on_rdma_receive()` へ手組みイベントを与える (制御チャネル id はテストフックで設定)。FATAL 5 種は EXPECT_DEATH、型違い drop は継続 + シーケンス消費まで検証。RDMA スタック不要のため OFF ビルドでも走る |
| G7 | RDMA データイベントの id ディスパッチ異常系 (範囲外 id drop / 未登録 receiver drop / 制御 id 振り分け) | replica_server_test に相乗り (単位 J3、`on_rdma_receive()` 直叩き) |

### (b) 追加不要と判断したもの (2026-08-15 ユーザー確認済み)

| ID | 内容 | 理由 |
|---|---|---|
| G3 | RDMA 経路の E2E での全 entry 種別検証 | entry 種別ごとの直列化・適用は経路非依存 (`message_log_entries::apply_to()` 共通)。RDMA 固有なのはフレーム分割・再組立であり、それは parser/receiver 単体 16+7 本が全種別で担保。E2E は「経路が通ること」の代表で足りる |
| G5 | TCP group commit 失敗時の「replica 切り離しで master 継続」(送信失敗 = ERROR 継続、ACK 待ち失敗 = close_session + replica_exists_=false) | ユーザー判断でテスト不要 |
| G2 | replica 台帳の TCP/RDMA 相互排他 FATAL | 構成が排他 (純 TCP か純 RDMA) になり、両台帳が同時に埋まる構成が存在しない。到達不能な防御コード |
| — | RDMA の group commit 失敗 FATAL / 確立後受信例外 FATAL の death test | FATAL が仕様 (送信リング共有のため切り離し不能)。death test は fork コストと RDMA スタック模擬の手間に対し、検証できるのは「ログを吐いて死ぬ」ことのみで費用対効果が低い |

### (削除により対象消滅)

| ID | 内容 |
|---|---|
| G1 | ハイブリッド master 側 RDMA_INIT / RDMA_FINALIZE のエラー分岐 — 機能ごとコード削除 |

### (c) スコープ外 (別タスク・機能未実装)

| 内容 | 行き先 |
|---|---|
| replica 側 clean shutdown (RDMA モードは無限 sleep + シグナル即死) | 切り離しプロトコル (nt-tsurugi-internal: replica-detach-command-notes.md) |
| establish 失敗時ロールバックの完全化 (再確立可能化) | nt-tsurugi-internal: rdma-tcpless-carryover-notes.md E 節 (項目 13) |
| replica のエラーが master に伝わらない (group_commit_error 未実装) | 計画書 §2.2 のとおり本作業スコープ外 (TCP 版から引き継ぐ課題) |

## 5. 棚卸しで見つかったカバレッジ以外の観察事項

* **`GC_BOUNDARY_SWITCH` は宙に浮いている**: メッセージ型 (`message_gc_boundary_switch`) と
  そのテストは存在するが、src 内に送信側の呼び出し箇所が一切なく、TCP/RDMA どちらの
  経路にも組み込まれていない。
* `log_channel::abort_session()` は両経路とも `std::abort()` の未実装スタブ。
* データチャネルの `pending_frames_` はイベント受領直後に必ず全消費されるため、
  並べ替えバッファとしては本番経路で機能していない (シーケンスギャップは drop)。
* §10 に「判明している具体例」とあった group commit の RDMA E2E 欠如は、起票 (2026-08-10)
  より後のフェーズ 3〜5 の作業 (scenario_tcpless_rdma_test) で解消済み。

## 6. 決定と作業単位 (2026-08-15 確定)

* 単位 J1: ハイブリッド廃止の実装 — 専用コード削除・起動時チェック追加・ハイブリッド
  テスト削除・文書反映 (本文書の改訂を含む)
* 単位 J2: G4 — scenario_tcpless_rdma_test への blob E2E 追加
* 単位 J3: G6 + G7 — replica_server_test への異常系追加
* (b)(c) は追加しない。
