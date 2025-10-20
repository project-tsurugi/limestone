#include "limestone/grpc/service/wal_history_service_impl.h"

#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>

#include "grpc_test_helper.h"
#include "limestone/grpc/backend/grpc_service_backend.h"
#include "limestone/grpc/backend/standalone_backend.h"
#include "limestone/grpc/service/message_versions.h"
#include "limestone/log_entry.h"
#include "wal_history.grpc.pb.h"
#include "limestone/grpc/client/wal_history_client.h"
#include <boost/filesystem.hpp>

namespace limestone::grpc::service::testing {

using limestone::grpc::proto::WalHistoryRequest;
using limestone::grpc::proto::WalHistoryResponse;
using limestone::grpc::proto::WalHistoryService;

class wal_history_client_test : public ::testing::Test {
protected:

    static constexpr const char* log_dir = "/tmp/wal_history_client_test";
    limestone::grpc::testing::grpc_test_helper helper_;


    void SetUp() override {
        boost::filesystem::remove_all(log_dir);
        boost::filesystem::create_directories(log_dir);
        helper_.set_backend_factory([]() {
            return limestone::grpc::backend::grpc_service_backend::create_standalone(log_dir);
        });
        helper_.add_service_factory([](limestone::grpc::backend::grpc_service_backend& backend) {
            return std::make_unique<limestone::grpc::service::wal_history_service_impl>(backend);
        });
        helper_.setup();
    }


    void TearDown() override {
        helper_.tear_down();
        boost::filesystem::remove_all(log_dir);
    }
};


TEST_F(wal_history_client_test, get_wal_history_with_entries) {
    helper_.start_server();

    // prepare wal history on disk
    limestone::internal::wal_history wh(log_dir);
    wh.append(42);
    wh.append(84);
    auto expected = wh.list();
    // set last epoch file
    FILE* fp = std::fopen((std::string(log_dir) + "/epoch").c_str(), "wb");
    ASSERT_TRUE(fp != nullptr);
    limestone::api::log_entry::durable_epoch(fp, 100);
    std::fclose(fp);

    WalHistoryRequest req;
    req.set_version(list_wal_history_message_version);
    WalHistoryResponse resp;

    limestone::grpc::client::wal_history_client client(helper_.create_channel());
    auto status = client.get_wal_history(req, resp, 2000); // 2s timeout

    EXPECT_TRUE(status.ok());
    ASSERT_EQ(resp.records_size(), expected.size());
    for (int i = 0; i < resp.records_size(); ++i) {
        const auto& rec = resp.records(i);
        const auto& exp = expected[i];
        EXPECT_EQ(rec.epoch(), exp.epoch);
        EXPECT_EQ(rec.identity(), exp.identity);
        EXPECT_EQ(rec.timestamp(), static_cast<int64_t>(exp.timestamp));
    }
    EXPECT_EQ(resp.last_epoch(), 100);
}


TEST_F(wal_history_client_test, get_wal_history_server_down) {
    // Do NOT start_server(); simulate server unreachable.
    WalHistoryRequest req;
    req.set_version(list_wal_history_message_version);
    WalHistoryResponse resp;

    limestone::grpc::client::wal_history_client client(helper_.server_address());
    // short timeout to fail quickly
    auto status = client.get_wal_history(req, resp, 100);

    EXPECT_FALSE(status.ok());
}



} // namespace limestone::grpc::service::testing
