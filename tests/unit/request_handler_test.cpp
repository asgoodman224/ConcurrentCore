#include "concurrentcore/networking/protocol.hpp"
#include "concurrentcore/server/request_handler.hpp"
#include "concurrentcore/server/server_config.hpp"

#include "support/test_support.hpp"

#include <gtest/gtest.h>

#include <mutex>
#include <string>
#include <vector>

using namespace concurrentcore;
using nlohmann::json;
using namespace std::chrono_literals;

namespace {

class RequestHandlerTest : public ::testing::Test {
protected:
    RequestHandlerTest()
        : jobs_(make_config(), std::make_shared<const Workspace>(dir_.path())),
          handler_(jobs_, [] { return json{{"connections", {{"active", 1}}}}; }) {}

    // Job callbacks capture sink_ (and thus this fixture); stop all workers
    // before any member they might touch is destroyed.
    ~RequestHandlerTest() override { jobs_.shutdown(0ms); }

    static JobManagerConfig make_config() {
        JobManagerConfig c;
        c.worker_count = 2;
        c.queue_capacity = 16;
        return c;
    }

    json call(const json& request) {
        return json::parse(handler_.handle(protocol::serialize(request), sink_));
    }
    json call_raw(std::string_view payload) { return json::parse(handler_.handle(payload, sink_)); }

    static std::string error_code(const json& response) {
        EXPECT_FALSE(response.value("ok", true)) << response.dump();
        return response["error"].value("code", "");
    }

    cctest::TempDir dir_;
    JobManager jobs_;
    RequestHandler handler_;
    std::mutex events_mutex_;
    std::vector<json> events_;
    cctest::Counter event_count_;
    RequestHandler::EventSink sink_ = [this](std::string payload) {
        {
            std::lock_guard lock(events_mutex_);
            events_.push_back(json::parse(payload));
        }
        event_count_.increment();
    };
};

}  // namespace

TEST(Protocol, OperationNamesRoundTrip) {
    for (const auto op : {protocol::Operation::SubmitJob, protocol::Operation::GetJobStatus,
                          protocol::Operation::GetJobResult, protocol::Operation::CancelJob,
                          protocol::Operation::GetServerStats, protocol::Operation::Ping}) {
        EXPECT_EQ(protocol::operation_from_string(protocol::to_string(op)), op);
    }
    EXPECT_FALSE(protocol::operation_from_string("ping").has_value());  // case-sensitive
}

TEST(Protocol, ErrorCodeNamesRoundTrip) {
    for (const auto code : {ErrorCode::MalformedJson, ErrorCode::QueueFull, ErrorCode::CorruptData,
                            ErrorCode::InternalError, ErrorCode::ServerBusy}) {
        EXPECT_EQ(error_code_from_string(to_string(code)), code);
    }
}

TEST(Protocol, ParseRejectsGarbageAndDeepNesting) {
    EXPECT_THROW((void)protocol::parse(""), CoreError);
    EXPECT_THROW((void)protocol::parse("{oops"), CoreError);
    EXPECT_THROW((void)protocol::parse(std::string(100, '[') + std::string(100, ']')), CoreError);
    EXPECT_NO_THROW((void)protocol::parse(std::string(10, '[') + std::string(10, ']')));
}

TEST(Protocol, SerializeReplacesInvalidUtf8InsteadOfThrowing) {
    const json j = {{"s", std::string("bad \xFF\xFE bytes")}};
    std::string out;
    EXPECT_NO_THROW(out = protocol::serialize(j));
    EXPECT_NE(out.find("bad"), std::string::npos);
}

TEST_F(RequestHandlerTest, PingEchoesRequestId) {
    auto r = call({{"type", "PING"}, {"request_id", "abc-1"}});
    EXPECT_TRUE(r["ok"]);
    EXPECT_EQ(r["kind"], "response");
    EXPECT_EQ(r["type"], "PING");
    EXPECT_EQ(r["request_id"], "abc-1");
    EXPECT_TRUE(r["result"]["pong"]);
    r = call({{"type", "PING"}, {"request_id", 77}});
    EXPECT_EQ(r["request_id"], 77);
}

TEST_F(RequestHandlerTest, MalformedAndInvalidRequests) {
    EXPECT_EQ(error_code(call_raw("{not json")), "MALFORMED_JSON");
    EXPECT_EQ(error_code(call_raw("[1,2,3]")), "INVALID_REQUEST");
    EXPECT_EQ(error_code(call({{"request_id", 1}})), "INVALID_REQUEST");
    EXPECT_EQ(error_code(call({{"type", 5}})), "INVALID_REQUEST");
    EXPECT_EQ(error_code(call({{"type", "PING"}, {"request_id", json::array()}})), "INVALID_REQUEST");
    EXPECT_EQ(error_code(call({{"type", "FORMAT_DISK"}})), "UNKNOWN_OPERATION");
    EXPECT_EQ(error_code(call({{"type", "GET_JOB_STATUS"}})), "INVALID_PARAMS");
    EXPECT_EQ(error_code(call({{"type", "GET_JOB_STATUS"}, {"job_id", -3}})), "INVALID_PARAMS");
    EXPECT_EQ(error_code(call({{"type", "GET_JOB_STATUS"}, {"job_id", 999}})), "JOB_NOT_FOUND");
    EXPECT_EQ(error_code(call({{"type", "CANCEL_JOB"}, {"job_id", 999}})), "JOB_NOT_FOUND");
}

TEST_F(RequestHandlerTest, SubmitValidation) {
    EXPECT_EQ(error_code(call({{"type", "SUBMIT_JOB"}})), "INVALID_PARAMS");
    EXPECT_EQ(error_code(call({{"type", "SUBMIT_JOB"}, {"task_type", "rot13"}})), "UNKNOWN_TASK_TYPE");
    EXPECT_EQ(error_code(call({{"type", "SUBMIT_JOB"}, {"task_type", "sha256"}, {"params", {{"file", "../x"}}}})),
              "PATH_NOT_ALLOWED");
    EXPECT_EQ(error_code(call({{"type", "SUBMIT_JOB"},
                               {"task_type", "compute"},
                               {"params", {{"limit", 10}}},
                               {"notify", "yes"}})),
              "INVALID_PARAMS");
}

TEST_F(RequestHandlerTest, SubmitStatusResultFlow) {
    const auto submitted = call({{"type", "SUBMIT_JOB"},
                                 {"request_id", 1},
                                 {"task_type", "compute"},
                                 {"params", {{"limit", 1000}}}});
    ASSERT_TRUE(submitted["ok"]) << submitted.dump();
    const auto id = submitted["result"]["job_id"].get<std::uint64_t>();
    EXPECT_EQ(submitted["result"]["status"], "QUEUED");

    ASSERT_TRUE(jobs_.wait_idle(10s));
    const auto status = call({{"type", "GET_JOB_STATUS"}, {"job_id", id}});
    EXPECT_EQ(status["result"]["status"], "COMPLETED");
    EXPECT_FALSE(status["result"].contains("result"));

    const auto result = call({{"type", "GET_JOB_RESULT"}, {"job_id", id}});
    EXPECT_EQ(result["result"]["result"]["prime_count"], 168);

    const auto cancel = call({{"type", "CANCEL_JOB"}, {"job_id", id}});
    EXPECT_EQ(cancel["result"]["outcome"], "ALREADY_FINISHED");
}

TEST_F(RequestHandlerTest, ResultOfUnfinishedJobIsJobNotFinished) {
    const auto submitted = call({{"type", "SUBMIT_JOB"},
                                 {"task_type", "compute"},
                                 {"params", {{"limit", 150'000'000}}}});
    const auto id = submitted["result"]["job_id"].get<std::uint64_t>();
    EXPECT_EQ(error_code(call({{"type", "GET_JOB_RESULT"}, {"job_id", id}})), "JOB_NOT_FINISHED");
    const auto cancel = call({{"type", "CANCEL_JOB"}, {"job_id", id}});
    EXPECT_TRUE(cancel["ok"]);
    ASSERT_TRUE(jobs_.wait_idle(10s));
    EXPECT_EQ(call({{"type", "GET_JOB_RESULT"}, {"job_id", id}})["result"]["status"], "CANCELLED");
}

TEST_F(RequestHandlerTest, NotifyDeliversCorrelatedEvent) {
    const auto submitted = call({{"type", "SUBMIT_JOB"},
                                 {"request_id", "job-42"},
                                 {"task_type", "text-analysis"},
                                 {"params", {{"text", "one two three"}}},
                                 {"notify", true}});
    ASSERT_TRUE(submitted["ok"]);
    ASSERT_TRUE(event_count_.wait_for_at_least(1));
    std::lock_guard lock(events_mutex_);
    ASSERT_EQ(events_.size(), 1U);
    const auto& e = events_[0];
    EXPECT_EQ(e["kind"], "event");
    EXPECT_EQ(e["type"], "JOB_FINISHED");
    EXPECT_EQ(e["request_id"], "job-42");
    EXPECT_EQ(e["data"]["status"], "COMPLETED");
    EXPECT_EQ(e["data"]["result"]["words"], 3);
}

TEST_F(RequestHandlerTest, StatsMergeServerLevelFields) {
    const auto r = call({{"type", "GET_SERVER_STATS"}});
    ASSERT_TRUE(r["ok"]);
    const auto& s = r["result"];
    for (const auto* key : {"uptime_seconds", "jobs", "queue", "workers", "latency_ms", "throughput"}) {
        EXPECT_TRUE(s.contains(key)) << key;
    }
    EXPECT_EQ(s["workers"]["total"], 2);
    EXPECT_EQ(s["connections"]["active"], 1);
}

TEST(ServerConfig, JsonApplicationAndValidation) {
    ServerConfig c;
    apply_json(c, {{"port", 9000}, {"worker_threads", 3}, {"idle_timeout_seconds", 5}});
    EXPECT_EQ(c.port, 9000);
    EXPECT_EQ(c.worker_threads, 3U);
    EXPECT_EQ(c.idle_timeout.count(), 5);
    EXPECT_NO_THROW(validate(c));
    EXPECT_THROW(apply_json(c, {{"no_such_key", 1}}), std::invalid_argument);
    EXPECT_THROW(apply_json(c, {{"port", 70000}}), std::invalid_argument);
    EXPECT_THROW(apply_json(c, {{"port", "80"}}), std::invalid_argument);
    c.worker_threads = 0;
    EXPECT_THROW(validate(c), std::invalid_argument);
}
