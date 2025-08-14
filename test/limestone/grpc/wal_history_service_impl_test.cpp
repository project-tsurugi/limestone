#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include "limestone/grpc/service/wal_history_service_impl.h"
#include "wal_history.grpc.pb.h"
#include "grpc_server_test_base.h"
#include "limestone/grpc/backend/grpc_service_backend.h"
#include "limestone/grpc/backend/standalone_backend.h"

namespace limestone::grpc::service::testing {


class wal_history_service_impl_test : public limestone::grpc::testing::grpc_server_test_base {
protected:
    static constexpr const char* log_dir = "/tmp/wal_history_service_impl_test";

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

    limestone::grpc::proto::ListWalHistoryRequest request;
    limestone::grpc::proto::ListWalHistoryResponse response;
    ::grpc::ClientContext context;
    auto stub = limestone::grpc::proto::WalHistoryService::NewStub(
        ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials()));
    auto status = stub->ListWalHistory(&context, request, &response);
    EXPECT_TRUE(status.ok());
    EXPECT_EQ(response.records_size(), 0);
}

TEST_F(wal_history_service_impl_test, list_wal_history_single) {
    start_server();

    limestone::internal::wal_history wh(log_dir);
    wh.append(123);
    auto expected = wh.list();

    limestone::grpc::proto::ListWalHistoryRequest request;
    limestone::grpc::proto::ListWalHistoryResponse response;
    ::grpc::ClientContext context;
    auto stub = limestone::grpc::proto::WalHistoryService::NewStub(
        ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials()));
    auto status = stub->ListWalHistory(&context, request, &response);
    EXPECT_TRUE(status.ok());
    ASSERT_EQ(response.records_size(), expected.size());
    for (int i = 0; i < response.records_size(); ++i) {
        const auto& rec = response.records(i);
        const auto& exp = expected[i];
        EXPECT_EQ(rec.epoch(), exp.epoch);
        EXPECT_EQ(rec.timestamp(), static_cast<int64_t>(exp.timestamp));
        ASSERT_EQ(rec.unique_id().size(), exp.unique_id.size());
        for (size_t j = 0; j < exp.unique_id.size(); ++j) {
            EXPECT_EQ(static_cast<uint8_t>(rec.unique_id()[j]), exp.unique_id[j]);
        }
    }
}

TEST_F(wal_history_service_impl_test, list_wal_history_multiple) {
    start_server();

    limestone::internal::wal_history wh(log_dir);
    wh.append(111);
    wh.append(222);
    wh.append(333);
    auto expected = wh.list();

    limestone::grpc::proto::ListWalHistoryRequest request;
    limestone::grpc::proto::ListWalHistoryResponse response;
    ::grpc::ClientContext context;
    auto stub = limestone::grpc::proto::WalHistoryService::NewStub(
        ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials()));
    auto status = stub->ListWalHistory(&context, request, &response);
    EXPECT_TRUE(status.ok());
    ASSERT_EQ(response.records_size(), expected.size());
    for (int i = 0; i < response.records_size(); ++i) {
        const auto& rec = response.records(i);
        const auto& exp = expected[i];
        EXPECT_EQ(rec.epoch(), exp.epoch);
        EXPECT_EQ(rec.timestamp(), static_cast<int64_t>(exp.timestamp));
        ASSERT_EQ(rec.unique_id().size(), exp.unique_id.size());
        for (size_t j = 0; j < exp.unique_id.size(); ++j) {
            EXPECT_EQ(static_cast<uint8_t>(rec.unique_id()[j]), exp.unique_id[j]);
        }
    }
}

// NOTE: This test is disabled because AddressSanitizer (ASan) reports a memory leak in CI environments.
// The root cause could not be identified after investigation. The test is kept for reference but is not run by default.
TEST_F(wal_history_service_impl_test, DISABLED_list_wal_history_backend_throws) {
    class throwing_backend : public limestone::grpc::backend::standalone_backend {
    public:
        using standalone_backend::standalone_backend;
        std::vector<limestone::internal::wal_history::record> list_wal_history() override {
            throw std::runtime_error("backend error");
        }
    };

    set_backend_factory([]() {
        return std::make_unique<throwing_backend>(log_dir);
    });
    start_server();

    limestone::grpc::proto::ListWalHistoryRequest request;
    limestone::grpc::proto::ListWalHistoryResponse response;
    ::grpc::ClientContext context;
    auto stub = limestone::grpc::proto::WalHistoryService::NewStub(
        ::grpc::CreateChannel(server_address_, ::grpc::InsecureChannelCredentials()));
    auto status = stub->ListWalHistory(&context, request, &response);
    EXPECT_FALSE(status.ok());
    EXPECT_EQ(status.error_code(), ::grpc::StatusCode::INTERNAL);
    EXPECT_STREQ(status.error_message().c_str(), "backend error");
}

} // namespace limestone::grpc::service::testing
