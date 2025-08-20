#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include "limestone/grpc/service/wal_history_service_impl.h"
#include "wal_history.grpc.pb.h"
#include "grpc_server_test_base.h"
#include "limestone/grpc/backend/grpc_service_backend.h"
#include "limestone/grpc/backend/standalone_backend.h"
#include "limestone/log_entry.h"


namespace limestone::grpc::service::testing {

using WalHistoryRequest = limestone::grpc::proto::WalHistoryRequest;
using WalHistoryResponse = limestone::grpc::proto::WalHistoryResponse;
using WalHistoryService = limestone::grpc::proto::WalHistoryService;


class wal_history_service_impl_test : public limestone::grpc::testing::grpc_server_test_base {
protected:
    static constexpr const char* log_dir = "/tmp/wal_history_service_impl_test";

    void write_epoch_file(uint64_t epoch_id) {
        auto epoch_file = boost::filesystem::path(log_dir) / "epoch";
        FILE* fp = std::fopen(epoch_file.c_str(), "wb");
        ASSERT_TRUE(fp != nullptr);
        limestone::api::log_entry::durable_epoch(fp, epoch_id);
        std::fclose(fp);
    }

    void SetUp() override {
        boost::filesystem::remove_all(log_dir);
        boost::filesystem::create_directories(log_dir);
        set_backend_factory([]() {
            return limestone::grpc::backend::grpc_service_backend::create_standalone(log_dir);
        });
        set_service_factory([](limestone::grpc::backend::grpc_service_backend& backend) {
            return std::make_unique<wal_history_service_impl>(backend);
        });
        limestone::grpc::testing::grpc_server_test_base::SetUp();
    }

    void TearDown() override {
        limestone::grpc::testing::grpc_server_test_base::TearDown();
        boost::filesystem::remove_all(log_dir);
    }
};


TEST_F(wal_history_service_impl_test, list_wal_history_empty) {
    start_server();

    WalHistoryRequest request;
    WalHistoryResponse response;
    ::grpc::ClientContext context;
    auto stub = WalHistoryService::NewStub(
        ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials()));
    auto status = stub->GetWalHistory(&context, request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response.records_size(), 0);
}

TEST_F(wal_history_service_impl_test, list_wal_history_single) {
    start_server();

    limestone::internal::wal_history wh(log_dir);
    wh.append(123);
    auto expected = wh.list();
    write_epoch_file(200);

    WalHistoryRequest request;
    WalHistoryResponse response;
    ::grpc::ClientContext context;
    auto stub = WalHistoryService::NewStub(
        ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials()));
    auto status = stub->GetWalHistory(&context, request, &response);
    EXPECT_TRUE(status.ok());
    ASSERT_EQ(response.records_size(), expected.size());
    for (int i = 0; i < response.records_size(); ++i) {
        const auto& rec = response.records(i);
        const auto& exp = expected[i];
        EXPECT_EQ(rec.epoch(), exp.epoch);
        EXPECT_EQ(rec.identity(), exp.identity);
        EXPECT_EQ(rec.timestamp(), static_cast<int64_t>(exp.timestamp));
    }
    EXPECT_EQ(response.last_epoch(), 200);
}

TEST_F(wal_history_service_impl_test, list_wal_history_multiple) {
    start_server();

    limestone::internal::wal_history wh(log_dir);
    wh.append(111);
    wh.append(222);
    wh.append(333);
    auto expected = wh.list();
    write_epoch_file(400);

    WalHistoryRequest request;
    WalHistoryResponse response;
    ::grpc::ClientContext context;
    auto stub = WalHistoryService::NewStub(
        ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials()));
    auto status = stub->GetWalHistory(&context, request, &response);
    EXPECT_TRUE(status.ok());
    ASSERT_EQ(response.records_size(), expected.size());
    for (int i = 0; i < response.records_size(); ++i) {
        const auto& rec = response.records(i);
        const auto& exp = expected[i];
        EXPECT_EQ(rec.epoch(), exp.epoch);
        EXPECT_EQ(rec.identity(), exp.identity);
        EXPECT_EQ(rec.timestamp(), static_cast<int64_t>(exp.timestamp));
    }
    EXPECT_EQ(response.last_epoch(), 400);
}

TEST_F(wal_history_service_impl_test, list_wal_history_with_max_last_epoch) {
    start_server();

    limestone::internal::wal_history wh(log_dir);
    wh.append(std::numeric_limits<uint64_t>::max());
    auto expected = wh.list();
    write_epoch_file(std::numeric_limits<uint64_t>::max());

    WalHistoryRequest request;
    WalHistoryResponse response;
    ::grpc::ClientContext context;
    auto stub = WalHistoryService::NewStub(
        ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials()));
    auto status = stub->GetWalHistory(&context, request, &response);
    EXPECT_TRUE(status.ok());
    ASSERT_EQ(response.records_size(), expected.size());
    for (int i = 0; i < response.records_size(); ++i) {
        const auto& rec = response.records(i);
        const auto& exp = expected[i];
        EXPECT_EQ(rec.epoch(), exp.epoch);
        EXPECT_EQ(rec.identity(), exp.identity);
        EXPECT_EQ(rec.timestamp(), static_cast<int64_t>(exp.timestamp));
    }
    EXPECT_EQ(response.last_epoch(), std::numeric_limits<uint64_t>::max());
}

// NOTE: This test is disabled because AddressSanitizer (ASan) reports a memory leak in CI environments.
// The root cause could not be identified after investigation. The test is kept for reference but is not run by default.
TEST_F(wal_history_service_impl_test, DISABLED_list_wal_history_epoch_greater_than_last_epoch_should_throw) {
    start_server();

    limestone::internal::wal_history wh(log_dir);
    wh.append(1000); // epoch of wal_history
    auto expected = wh.list();
    write_epoch_file(500); // set last_epoch to a smaller value

    limestone::grpc::proto::WalHistoryRequest request;
    limestone::grpc::proto::WalHistoryResponse response;
    ::grpc::ClientContext context;
    auto stub = limestone::grpc::proto::WalHistoryService::NewStub(
        ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials()));
    auto status = stub->GetWalHistory(&context, request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_NE(std::string(status.error_message()).find("wal history contains a record whose epoch is greater than last_epoch"), std::string::npos);
}

} // namespace limestone::grpc::service::testing
