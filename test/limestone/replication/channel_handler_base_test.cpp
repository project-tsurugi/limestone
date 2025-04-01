#include "replication/channel_handler_base.h"
#include "replication/validation_result.h"
#include "replication/message_ack.h"
#include "replication/message_error.h"
#include "replication/socket_io.h"
#include "gtest/gtest.h"
#include <pthread.h>

namespace limestone::testing {
using namespace limestone::replication;

class dummy_server {};

class test_handler : public channel_handler_base {
public:
    test_handler(dummy_server& server, bool valid)
        : channel_handler_base(reinterpret_cast<replica_server&>(server)), valid_(valid), dispatched_(false) {}
    using channel_handler_base::get_server;
protected:
    validation_result authorize() override {
        pthread_setname_np(pthread_self(), "test-handler");
        return validation_result::success();
    }

    validation_result validate_initial(std::unique_ptr<replication_message>) override {
        return valid_ ? validation_result::success()
                      : validation_result::error(42, "bad request");
    }

    void send_initial_ack(socket_io& io) const override {
        io.send_string("ACK_SENT");
    }

    void dispatch(replication_message&, handler_resources& resources) override {
        dispatched_ = true;
        throw std::runtime_error("stop loop");
    }

public:
    bool dispatched() const { return dispatched_; }

private:
    bool valid_;
    mutable bool dispatched_;
};

TEST(channel_handler_base_test, run_sends_initial_ack_and_dispatches) {
    dummy_server server;
    test_handler handler(server, true);

    message_ack dummy_msg;
    socket_io preparer("");
    replication_message::send(preparer, dummy_msg);
    socket_io io(preparer.get_out_string());

    EXPECT_THROW(handler.run(io, std::make_unique<message_ack>()), std::runtime_error);

    socket_io ack_io(io.get_out_string());
    EXPECT_EQ(ack_io.receive_string(), "ACK_SENT");
    EXPECT_TRUE(handler.dispatched());

    char name[16];
    pthread_getname_np(pthread_self(), name, sizeof(name));
    EXPECT_STREQ(name, "test-handler");
}

TEST(channel_handler_base_test, run_sends_error_on_validation_failure) {
    dummy_server server;
    test_handler handler(server, false);

    socket_io out("");
    handler.run(out, std::make_unique<message_ack>());

    socket_io in(out.get_out_string());
    auto resp = replication_message::receive(in);
    auto* err = dynamic_cast<message_error*>(resp.get());
    ASSERT_NE(err, nullptr);
    EXPECT_EQ(err->get_error_code(), 42);
    EXPECT_EQ(err->get_error_message(), "bad request");

    char name[16];
    pthread_getname_np(pthread_self(), name, sizeof(name));
    EXPECT_STREQ(name, "test-handler");
}

TEST(channel_handler_base_test, send_ack_in_loop) {
    dummy_server server;
    class ack_handler : public channel_handler_base {
    public:
        explicit ack_handler(dummy_server& s)
            : channel_handler_base(reinterpret_cast<replica_server&>(s)) {}

    protected:
        validation_result authorize() override {
            pthread_setname_np(pthread_self(), "ack-handler");
            return validation_result::success();
        }

        validation_result validate_initial(std::unique_ptr<replication_message>) override {
            return validation_result::success();
        }

        void send_initial_ack(socket_io&) const override {}
        void dispatch(replication_message&, handler_resources& resources) override {
            send_ack(resources.get_socket_io());
            throw std::runtime_error("stop loop");
        }
    } handler(server);

    message_ack dummy_msg;
    socket_io preparer("");
    replication_message::send(preparer, dummy_msg);
    socket_io io(preparer.get_out_string());
    std::cerr << preparer.get_out_string() << std::endl;
    std::cerr << "io = " << io.get_out_string() << std::endl;
    EXPECT_THROW(handler.run(io, std::make_unique<message_ack>()), std::runtime_error);
    std::cerr << "io = " << io.get_out_string() << std::endl;

    socket_io in(io.get_out_string());
    auto response = replication_message::receive(in);
    auto* ack = dynamic_cast<message_ack*>(response.get());
    ASSERT_NE(ack, nullptr);

    char name[16];
    pthread_getname_np(pthread_self(), name, sizeof(name));
    EXPECT_STREQ(name, "ack-handler");
}

TEST(channel_handler_base_test, run_sends_error_when_assign_fails) {
    dummy_server server;

    class failing_handler : public channel_handler_base {
    public:
        explicit failing_handler(dummy_server& s)
            : channel_handler_base(reinterpret_cast<replica_server&>(s)) {}

    protected:
        validation_result authorize() override {
            return validation_result::error(99, "assign failed");
        }

        validation_result validate_initial(std::unique_ptr<replication_message>) override {
            return validation_result::success();  // 呼ばれないはず
        }

        void send_initial_ack(socket_io&) const override {}
        void dispatch(replication_message&, handler_resources&) override {}
    };

    failing_handler handler(server);
    socket_io io("");

    handler.run(io, std::make_unique<message_ack>());

    socket_io reader(io.get_out_string());
    auto msg = replication_message::receive(reader);
    auto* err = dynamic_cast<message_error*>(msg.get());
    ASSERT_NE(err, nullptr);
    EXPECT_EQ(err->get_error_code(), 99);
    EXPECT_EQ(err->get_error_message(), "assign failed");
}


TEST(channel_handler_base_test, get_server) {
    dummy_server server;
    test_handler handler(server, true);
    replica_server& got_server = handler.get_server();

    EXPECT_EQ(reinterpret_cast<void*>(&got_server), reinterpret_cast<void*>(&server));
}

}  // namespace limestone::testing