/*
 * Copyright 2022-2026 Project Tsurugi.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
#pragma once

#include <cstddef>
#include <cstdint>
#include <memory>

#include "replication_message.h"

namespace limestone::replication {

/**
 * @brief コンパクション境界メッセージ。
 *
 * master が、コンパクションの完了のたびに、そのコンパクションの境界 epoch と、BLOB ファイルの GC の境界にした
 * BLOB の ID を、コントロールチャネルでレプリカへ送る。レプリカは応答を返さない。
 */
class message_compaction_boundary : public replication_message {
public:
    /**
     * @brief シリアライズした大きさ (バイト)。
     *
     * replication_message::send() が書く型の番号 (uint8) と、send_body() が書く境界 epoch (uint64) と BLOB の ID の
     * 境界 (uint64) の合計である。シリアライズの形を変えるときは、この値も変える。
     */
    static constexpr std::size_t wire_size = sizeof(std::uint8_t) + sizeof(std::uint64_t) + sizeof(std::uint64_t);

    message_compaction_boundary() = default;

    /**
     * @param boundary_epoch コンパクションの境界 epoch
     * @param blob_id_boundary BLOB ファイルの GC の境界にした BLOB の ID
     */
    message_compaction_boundary(std::uint64_t boundary_epoch, std::uint64_t blob_id_boundary);

    [[nodiscard]] message_type_id get_message_type_id() const override;
    void send_body(replication_message_io& io) const override;
    void receive_body(replication_message_io& io) override;

    [[nodiscard]] static std::unique_ptr<replication_message> create();

    [[nodiscard]] std::uint64_t boundary_epoch() const;
    [[nodiscard]] std::uint64_t blob_id_boundary() const;

    /**
     * @brief 受け取った境界を、レプリカの datastore に保持させる。応答は送らない。
     * @param resources コントロールチャネルのハンドラの資源 (control_channel_handler_resources)
     */
    void post_receive(handler_resources& resources) override;

private:
    // COMPACTION_BOUNDARY を replication_message の生成の表に登録する。静的な初期化は意図したものである。
    // 例外が起きたときは、プログラムをただちに終了させるのが望ましいので、clang-tidy の警告 (cert-err58-cpp) を
    // 無視する。
    // NOLINTNEXTLINE(cert-err58-cpp)
    inline static const bool registered_ = []() {
        replication_message::register_message_type(message_type_id::COMPACTION_BOUNDARY,
                                                   &message_compaction_boundary::create);
        return true;
    }();

    std::uint64_t boundary_epoch_{0};
    std::uint64_t blob_id_boundary_{0};
};

} // namespace limestone::replication
