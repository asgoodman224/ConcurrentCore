// End-to-end tests: a real Server on an ephemeral localhost port, driven by
// real TCP clients (the Client library and raw sockets).

#include "concurrentcore/client/client.hpp"
#include "concurrentcore/networking/frame.hpp"
#include "concurrentcore/networking/protocol.hpp"
#include "concurrentcore/server/tcp_server.hpp"
#include "concurrentcore/tasks/tasks.hpp"

#include "support/test_support.hpp"

#include <gtest/gtest.h>
#include <spdlog/spdlog.h>

#include <atomic>
#include <functional>
#include <set>
#include <thread>
#include <vector>

using namespace concurrentcore;
using nlohmann::json;
using namespace std::chrono_literals;

namespace {

/// Blocking raw TCP connection for byte-level protocol tests.
class RawConnection {
public:
    explicit RawConnection(std::uint16_t port) {
        socket_.connect({asio::ip::make_address("127.0.0.1"), port});
        socket_.set_option(asio::ip::tcp::no_delay(true));
    }
    void write(std::string_view bytes) { asio::write(socket_, asio::buffer(bytes.data(), bytes.size())); }
    void write_frame(const json& message) { write(net::encode_frame(protocol::serialize(message))); }

    json read_message() {
        std::array<unsigned char, 4> h{};
        asio::read(socket_, asio::buffer(h));
        const std::uint32_t n = (std::uint32_t{h[0]} << 24) | (std::uint32_t{h[1]} << 16) |
                                (std::uint32_t{h[2]} << 8) | std::uint32_t{h[3]};
        std::string payload(n, '\0');
        asio::read(socket_, asio::buffer(payload));
        return json::parse(payload);
    }

    /// True if the server closed the connection (EOF / reset).
    bool closed_by_peer() {
        std::array<char, 1> b{};
        std::error_code ec;
        socket_.read_some(asio::buffer(b), ec);
        return static_cast<bool>(ec);
    }

    void close() {
        std::error_code ec;
        socket_.close(ec);
    }

private:
    asio::io_context io_;
    asio::ip::tcp::socket socket_{io_};
};

class ServerTest : public ::testing::Test {
protected:
    void SetUp() override { spdlog::set_level(spdlog::level::warn); }

    void TearDown() override {
        if (server_) {
            server_->shutdown(0ms);
        }
    }

    ServerConfig base_config() {
        ServerConfig c;
        c.host = "127.0.0.1";
        c.port = 0;
        c.io_threads = 2;
        c.worker_threads = 4;
        c.queue_capacity = 256;
        c.workspace = dir_.path();
        c.shutdown_drain_timeout = 5s;
        return c;
    }

    void start(const std::function<void(ServerConfig&)>& tweak = {}, JobManager::Executor exec = {}) {
        ServerConfig c = base_config();
        if (tweak) {
            tweak(c);
        }
        server_ = std::make_unique<Server>(c, std::move(exec));
        server_->start();
        ASSERT_NE(server_->port(), 0);
    }

    std::unique_ptr<Client> connect() {
        auto client = std::make_unique<Client>();
        client->connect("127.0.0.1", server_->port(), 5s);
        return client;
    }

    // Submits with notify=true and waits for the JOB_FINISHED event.
    json run_job(Client& c, const std::string& type, const json& params) {
        const auto r = c.submit(type, params, true);
        EXPECT_TRUE(r["ok"]) << r.dump();
        const auto event = c.wait_for_event(30s);
        EXPECT_TRUE(event.has_value());
        return event ? (*event)["data"] : json();
    }

    cctest::TempDir dir_;
    std::unique_ptr<Server> server_;
};

}  // namespace

TEST_F(ServerTest, PingAndStats) {
    start();
    auto c = connect();
    const auto pong = c->ping();
    EXPECT_TRUE(pong["ok"]);
    EXPECT_EQ(pong["result"]["protocol_version"], 1);
    const auto stats = c->stats();
    ASSERT_TRUE(stats["ok"]);
    EXPECT_EQ(stats["result"]["workers"]["total"], 4);
    EXPECT_EQ(stats["result"]["connections"]["active"], 1);
}

TEST_F(ServerTest, AllFiveTaskTypesProduceCorrectResults) {
    start();
    const std::string data = cctest::deterministic_bytes(200'000);
    cctest::write_file(dir_.path() / "data.bin", data);
    cctest::write_file(dir_.path() / "text.txt", "alpha beta\ngamma\n");
    auto c = connect();

    auto sha = run_job(*c, "sha256", {{"file", "data.bin"}});
    EXPECT_EQ(sha["status"], "COMPLETED");
    EXPECT_EQ(sha["result"]["digest"], sha256_hex(data));

    auto text = run_job(*c, "text-analysis", {{"file", "text.txt"}});
    EXPECT_EQ(text["result"]["words"], 3);
    EXPECT_EQ(text["result"]["lines"], 2);

    auto comp = run_job(*c, "compress", {{"input", "data.bin"}, {"format", "gzip"}, {"level", 9}});
    EXPECT_EQ(comp["status"], "COMPLETED");
    EXPECT_EQ(comp["result"]["original_bytes"], data.size());
    EXPECT_TRUE(std::filesystem::exists(dir_.path() / "data.bin.gz"));

    auto decomp = run_job(*c, "decompress", {{"input", "data.bin.gz"}, {"output", "copy.bin"}});
    EXPECT_EQ(decomp["status"], "COMPLETED");
    EXPECT_EQ(cctest::read_file(dir_.path() / "copy.bin"), data);

    auto compute = run_job(*c, "compute", {{"limit", 100'000}});
    EXPECT_EQ(compute["result"]["prime_count"], 9592);
    EXPECT_EQ(compute["task_type"], "compute");
    EXPECT_TRUE(compute["execution_ms"].is_number());
}

TEST_F(ServerTest, TaskFailuresAreReportedNotFatal) {
    start();
    auto c = connect();
    auto missing = run_job(*c, "sha256", {{"file", "nope.bin"}});
    EXPECT_EQ(missing["status"], "FAILED");
    EXPECT_EQ(missing["error"]["code"], "FILE_NOT_FOUND");
    cctest::write_file(dir_.path() / "bad.zz", "not compressed");
    auto corrupt = run_job(*c, "decompress", {{"input", "bad.zz"}, {"output", "x"}});
    EXPECT_EQ(corrupt["error"]["code"], "CORRUPT_DATA");
    EXPECT_TRUE(c->ping()["ok"]);  // connection and server unaffected
}

TEST_F(ServerTest, ManyClientsSubmitConcurrently) {
    start();
    constexpr int kClients = 12;
    constexpr int kJobsPerClient = 20;
    std::atomic<int> correct{0};
    std::vector<std::thread> threads;
    for (int t = 0; t < kClients; ++t) {
        threads.emplace_back([&, t] {
            Client c;
            c.connect("127.0.0.1", server_->port(), 5s);
            std::map<std::uint64_t, std::uint64_t> expected;  // job -> limit
            for (int j = 0; j < kJobsPerClient; ++j) {
                const std::uint64_t limit = 1000 + static_cast<std::uint64_t>(t * 100 + j);
                const auto r = c.submit("compute", {{"limit", limit}}, true);
                ASSERT_TRUE(r["ok"]) << r.dump();
                expected[r["result"]["job_id"].get<std::uint64_t>()] = limit;
            }
            for (int j = 0; j < kJobsPerClient; ++j) {
                const auto e = c.wait_for_event(30s);
                ASSERT_TRUE(e.has_value());
                const auto& job = (*e)["data"];
                const auto id = job["job_id"].get<std::uint64_t>();
                ASSERT_TRUE(expected.contains(id)) << "event for another client's job";
                if (job["result"]["prime_count"] == count_primes_below(expected[id]).count) {
                    ++correct;
                }
            }
        });
    }
    for (auto& t : threads) {
        t.join();
    }
    EXPECT_EQ(correct.load(), kClients * kJobsPerClient);
    EXPECT_EQ(server_->jobs().stats().completed, static_cast<std::uint64_t>(kClients * kJobsPerClient));
}

TEST_F(ServerTest, PipelinedRequestsAreCorrelatedByRequestId) {
    start();
    RawConnection raw(server_->port());
    std::string batch;
    for (int i = 0; i < 50; ++i) {
        const json req = (i % 2 == 0) ? json{{"type", "PING"}, {"request_id", "req-" + std::to_string(i)}}
                                      : json{{"type", "GET_JOB_STATUS"}, {"job_id", 999'999},
                                             {"request_id", "req-" + std::to_string(i)}};
        batch += net::encode_frame(protocol::serialize(req));
    }
    raw.write(batch);  // 50 frames in one write
    std::set<std::string> seen;
    for (int i = 0; i < 50; ++i) {
        const auto r = raw.read_message();
        const std::string id = r["request_id"];
        const int n = std::stoi(id.substr(4));
        EXPECT_EQ(r["type"], n % 2 == 0 ? "PING" : "GET_JOB_STATUS");
        EXPECT_EQ(r["ok"], n % 2 == 0);
        seen.insert(id);
    }
    EXPECT_EQ(seen.size(), 50U);
}

TEST_F(ServerTest, FrameDeliveredOneByteAtATime) {
    start();
    RawConnection raw(server_->port());
    const std::string wire = net::encode_frame(R"({"type":"PING","request_id":"slow"})");
    for (const char ch : wire) {
        raw.write(std::string_view(&ch, 1));
    }
    const auto r = raw.read_message();
    EXPECT_EQ(r["request_id"], "slow");
    EXPECT_TRUE(r["ok"]);
}

TEST_F(ServerTest, MalformedJsonGetsErrorAndConnectionSurvives) {
    start();
    RawConnection raw(server_->port());
    raw.write(net::encode_frame("{this is not json"));
    auto r = raw.read_message();
    EXPECT_FALSE(r["ok"]);
    EXPECT_EQ(r["error"]["code"], "MALFORMED_JSON");
    raw.write(net::encode_frame(""));  // empty frame
    r = raw.read_message();
    EXPECT_EQ(r["error"]["code"], "MALFORMED_JSON");
    raw.write_frame({{"type", "PING"}, {"request_id", 1}});
    r = raw.read_message();
    EXPECT_TRUE(r["ok"]);
}

TEST_F(ServerTest, OversizedMessageIsRejectedAndConnectionClosed) {
    start([](ServerConfig& c) {
        c.max_message_bytes = 1024;
        c.max_pending_write_bytes = 64 * 1024;
    });
    RawConnection raw(server_->port());
    // Announce 10 MB but send only the header: the server must reject it
    // from the length alone, without waiting for or buffering the payload.
    raw.write(std::string("\x00\xA0\x00\x00", 4));
    const auto r = raw.read_message();
    EXPECT_EQ(r["error"]["code"], "MESSAGE_TOO_LARGE");
    EXPECT_TRUE(raw.closed_by_peer());
    auto c = connect();
    EXPECT_TRUE(c->ping()["ok"]);  // server is fine
}

TEST_F(ServerTest, ClientDisconnectMidFrameDoesNotAffectServer) {
    start();
    {
        RawConnection raw(server_->port());
        const std::string wire = net::encode_frame(R"({"type":"PING"})");
        raw.write(wire.substr(0, 6));  // header + 2 payload bytes
        raw.close();
    }
    ASSERT_TRUE(cctest::wait_until([&] { return server_->active_connections() == 0; }));
    auto c = connect();
    EXPECT_TRUE(c->ping()["ok"]);
}

TEST_F(ServerTest, DisconnectBeforeNotifiedJobFinishesIsSafe) {
    start();
    {
        auto c = connect();
        for (int i = 0; i < 8; ++i) {
            ASSERT_TRUE(c->submit("compute", {{"limit", 300'000}}, true)["ok"]);
        }
        c->close();  // events will target a dead session (weak_ptr expires)
    }
    ASSERT_TRUE(server_->jobs().wait_idle(30s));
    EXPECT_EQ(server_->jobs().stats().completed, 8U);
    auto c = connect();
    EXPECT_TRUE(c->ping()["ok"]);
}

TEST_F(ServerTest, CancelRunningJobOverTheNetwork) {
    start();
    auto c = connect();
    const auto r = c->submit("compute", {{"limit", 150'000'000}});
    const auto id = r["result"]["job_id"].get<std::uint64_t>();
    ASSERT_TRUE(cctest::wait_until([&] { return c->status(id)["result"]["status"] == "RUNNING"; }));
    const auto cancel = c->cancel(id);
    EXPECT_EQ(cancel["result"]["outcome"], "CANCELLATION_REQUESTED");
    ASSERT_TRUE(cctest::wait_until([&] { return c->status(id)["result"]["status"] == "CANCELLED"; }));
    const auto result = c->result(id);
    EXPECT_EQ(result["result"]["error"]["code"], "CANCELLED");
}

TEST_F(ServerTest, BackPressureReturnsQueueFull) {
    cctest::Gate gate;
    start([](ServerConfig& c) {
        c.worker_threads = 1;
        c.queue_capacity = 2;
    }, [&gate](const TaskSpec&, const TaskContext& ctx) -> json {
        while (!gate.wait_for(1ms)) {
            ctx.throw_if_cancelled();
        }
        return {{"ok", true}};
    });
    auto c = connect();
    int accepted = 0;
    std::string last_error;
    for (int i = 0; i < 10 && last_error.empty(); ++i) {
        const auto r = c->submit("compute", {{"limit", 1}});
        if (r["ok"]) {
            ++accepted;
        } else {
            last_error = r["error"]["code"];
        }
    }
    EXPECT_EQ(last_error, "QUEUE_FULL");
    EXPECT_GE(accepted, 2);
    EXPECT_LE(accepted, 3);  // 1 running + 2 queued
    gate.open();
    server_->shutdown(10s);  // executor references `gate`: stop before it is destroyed
}

TEST_F(ServerTest, ConnectionLimitIsEnforced) {
    start([](ServerConfig& c) { c.max_connections = 2; });
    auto a = connect();
    auto b = connect();
    ASSERT_TRUE(a->ping()["ok"]);
    ASSERT_TRUE(b->ping()["ok"]);
    RawConnection third(server_->port());
    const auto r = third.read_message();
    EXPECT_EQ(r["error"]["code"], "SERVER_BUSY");
    EXPECT_TRUE(third.closed_by_peer());
    EXPECT_TRUE(a->ping()["ok"]);
}

TEST_F(ServerTest, IdleConnectionsAreClosed) {
    start([](ServerConfig& c) { c.idle_timeout = 1s; });
    RawConnection raw(server_->port());
    const auto t0 = std::chrono::steady_clock::now();
    EXPECT_TRUE(raw.closed_by_peer());  // blocks until the server closes it
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 10s);
}

TEST_F(ServerTest, GracefulShutdownFinishesJobsAndDeliversEvents) {
    start([](ServerConfig& c) { c.worker_threads = 2; });
    auto c = connect();
    constexpr int kJobs = 6;
    for (int i = 0; i < kJobs; ++i) {
        ASSERT_TRUE(c->submit("compute", {{"limit", 400'000}}, true)["ok"]);
    }
    std::thread stopper([&] { server_->shutdown(30s); });
    int completed = 0;
    for (int i = 0; i < kJobs; ++i) {
        const auto e = c->wait_for_event(30s);
        ASSERT_TRUE(e.has_value());
        if ((*e)["data"]["status"] == "COMPLETED") {
            ++completed;
        }
    }
    stopper.join();
    EXPECT_EQ(completed, kJobs);
    EXPECT_THROW((void)c->ping(2s), ClientError);  // server closed the connection
}

TEST_F(ServerTest, SubmissionsDuringShutdownAreRejected) {
    cctest::Gate gate;
    cctest::Counter started;
    start([](ServerConfig& c) { c.worker_threads = 1; },
          [&](const TaskSpec&, const TaskContext&) -> json {
              started.increment();
              gate.wait();
              return {{"ok", true}};
          });
    auto c = connect();
    ASSERT_TRUE(c->submit("compute", {{"limit", 1}})["ok"]);
    ASSERT_TRUE(started.wait_for_at_least(1));
    std::thread stopper([&] { server_->shutdown(30s); });
    ASSERT_TRUE(cctest::wait_until([&] { return !server_->jobs().stats().accepting; }));
    const auto r = c->submit("compute", {{"limit", 1}});
    EXPECT_EQ(r["error"]["code"], "SERVER_SHUTTING_DOWN");
    gate.open();
    stopper.join();
}

TEST_F(ServerTest, ImmediateShutdownCancelsRunningWork) {
    start([](ServerConfig& c) { c.worker_threads = 1; });
    auto c = connect();
    const auto r = c->submit("compute", {{"limit", 150'000'000}}, true);
    const auto id = r["result"]["job_id"].get<std::uint64_t>();
    ASSERT_TRUE(cctest::wait_until([&] { return c->status(id)["result"]["status"] == "RUNNING"; }));
    const auto t0 = std::chrono::steady_clock::now();
    std::thread stopper([&] { server_->shutdown(0ms); });
    const auto e = c->wait_for_event(10s);
    stopper.join();
    ASSERT_TRUE(e.has_value());
    EXPECT_EQ((*e)["data"]["status"], "CANCELLED");
    EXPECT_LT(std::chrono::steady_clock::now() - t0, 5s);
}
