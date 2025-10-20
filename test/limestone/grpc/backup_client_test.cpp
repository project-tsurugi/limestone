#include "limestone/grpc/client/backup_client.h"
#include <gtest/gtest.h>
#include <grpcpp/grpcpp.h>
#include "grpc_test_helper.h"
#include <boost/filesystem.hpp>
#include "limestone/grpc/service/backup_service_impl.h"
#include "backend_test_fixture.h"
#include <gtest/gtest-param-test.h>

namespace limestone::testing {

using limestone::grpc::proto::BackupService;
using limestone::grpc::proto::BeginBackupRequest;
using limestone::grpc::proto::BeginBackupResponse;
using limestone::grpc::service::backup_service_impl;
using limestone::grpc::service::session_timeout_seconds;

class backup_client_test : public backend_test_fixture, public ::testing::WithParamInterface<std::string> {
protected:

    static constexpr const char* log_dir = "/tmp/backup_client_test";
    limestone::grpc::testing::grpc_test_helper helper_;

    char const* get_location() const override {
        return log_dir;
    }

    void SetUp() override {
        // setenv("GRPC_VERBOSITY", "DEBUG", 1);
        // setenv("GRPC_TRACE", "client_channel,subchannel,call,error,connectivity_state", 1);

        boost::filesystem::remove_all(log_dir);
        boost::filesystem::create_directories(log_dir);

        mode = GetParam();
        if (mode == "standalone") {
            helper_.set_backend_factory([]() {
                return limestone::grpc::backend::grpc_service_backend::create_standalone(log_dir);
            });
        } else if (mode == "inproc") {
            helper_.set_backend_factory([this]() {
                return limestone::grpc::backend::grpc_service_backend::create_inproc(*datastore_, log_dir);
            });
        }

        helper_.add_service_factory([](limestone::grpc::backend::grpc_service_backend& backend) {
            return std::make_unique<limestone::grpc::service::backup_service_impl>(backend);
        });
        helper_.setup();
        backend_test_fixture::SetUp();
    }

    void TearDown() override {
        helper_.tear_down();
        backend_test_fixture::TearDown();
        boost::filesystem::remove_all(log_dir);
    }
private:
    std::string mode;
};



INSTANTIATE_TEST_SUITE_P(
    BackupClientModes,
    backup_client_test,
    ::testing::Values("standalone", "inproc")
);

TEST_P(backup_client_test, begin_backup_success) {
    std::string mode = GetParam();

    gen_datastore();
    prepare_backup_test_files();
    
    if (mode == "standalone") {
        // In standalone mode, we need to shutdown
        datastore_->shutdown();
        datastore_ = nullptr;
    } 
    assert_backup_file_conditions([](const backup_condition& c) { return c.pre_rotation_path; });

    helper_.start_server();

    BeginBackupRequest req;
    req.set_version(1);
    req.set_begin_epoch(0);
    req.set_end_epoch(0);

    BeginBackupResponse resp;

    auto before = static_cast<int64_t>(std::time(nullptr));
    auto status = [&]() {
        limestone::grpc::client::backup_client client(helper_.create_channel());
        if (mode == "standalone") {
            return client.begin_backup(req, resp, 2000);  // 2s timeout
        } else {
            return run_with_epoch_switch([&mode, &client, &req, &resp]() { return client.begin_backup(req, resp, 2000); }, 7);
        }
    }();

    auto after = static_cast<int64_t>(std::time(nullptr));

    EXPECT_TRUE(status.ok());


    auto session_id = resp.session_id();
    auto expire_at = resp.expire_at();
    auto start_epoch = resp.start_epoch();
    auto finish_epoch = resp.finish_epoch();
    auto objects = resp.objects();

    std::regex uuid_regex(R"([0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12})");
    EXPECT_TRUE(std::regex_match(session_id, uuid_regex)) << "session_id is not a valid UUID: " << session_id;
    EXPECT_GE(expire_at, before + session_timeout_seconds);
    EXPECT_LE(expire_at, after + session_timeout_seconds);
    EXPECT_EQ(start_epoch, 0);
    EXPECT_EQ(finish_epoch, 0);

    auto filtered_conditions = get_filtered_backup_conditions([&](const backup_condition& c) {
        if (mode == "standalone") {
            return c.is_offline_backup_target;
        }
        return c.is_online_backup_target;
    });
    std::unordered_set<std::string> filtered_object_ids;
    for (const auto& cond : filtered_conditions) {
        filtered_object_ids.insert(cond.object_id);
    }

    for(auto obj : objects) {
        auto matched = find_matching_backup_conditions(obj.object_id(), filtered_conditions);
        ASSERT_FALSE(matched.empty()) << "No matching backup condition for object: " << obj.object_id();
        ASSERT_LT(matched.size(), 2) << "Multiple matching backup conditions for object: " << obj.object_id();
        auto cond = matched.front();
        filtered_object_ids.erase(cond.object_id);
    }
    ASSERT_TRUE(filtered_object_ids.empty()) << "Some expected backup conditions were not matched. Remaining IDs: " << [&]() {
        std::ostringstream oss;
        for (const auto& id : filtered_object_ids) {
            oss << id << ", ";
        }
        return oss.str();
    }();
}

TEST_P(backup_client_test, begin_backup_server_down) {
    // Do NOT start_server(); simulate server unreachable.
    BeginBackupRequest req;
    req.set_version(1); // FIXME: Define appropriate version
    req.set_begin_epoch(0);
    req.set_end_epoch(100);

    BeginBackupResponse resp;

    limestone::grpc::client::backup_client client(helper_.server_address());
    auto status = client.begin_backup(req, resp, 100); // short timeout

    EXPECT_FALSE(status.ok());
}

} // namespace limestone::grpc::service::testing