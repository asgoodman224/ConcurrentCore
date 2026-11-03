#pragma once

#include "concurrentcore/jobs/job_manager.hpp"

#include <nlohmann/json.hpp>

#include <functional>
#include <string>
#include <string_view>

namespace concurrentcore {

/// Translates protocol requests into JobManager calls. Independent of the
/// network layer so it can be unit-tested directly.
///
/// handle() runs on a network thread. It only performs short, non-blocking
/// operations (registry lookups, try-push onto the bounded queue), so a
/// network thread is never stalled by job execution or back-pressure.
class RequestHandler {
public:
    /// Delivers a serialized event to the originating connection. Called
    /// from worker threads; must be thread-safe and non-blocking. If the
    /// connection is gone the implementation silently drops the event.
    using EventSink = std::function<void(std::string payload)>;

    /// Supplies server-level statistics (connections, ...) merged into the
    /// GET_SERVER_STATS response.
    using StatsProvider = std::function<nlohmann::json()>;

    explicit RequestHandler(JobManager& jobs, StatsProvider server_stats = {});

    /// Handles one framed payload and returns the serialized response.
    /// Never throws for client errors; they become error responses.
    [[nodiscard]] std::string handle(std::string_view payload, const EventSink& sink) const;

    /// Same, operating on an already-parsed message.
    [[nodiscard]] nlohmann::json handle_message(const nlohmann::json& request,
                                                const EventSink& sink) const;

    [[nodiscard]] static nlohmann::json stats_to_json(const JobManagerStats& stats);

private:
    nlohmann::json submit(std::string_view op, const nlohmann::json& request_id,
                          const nlohmann::json& request, const EventSink& sink) const;
    nlohmann::json job_query(std::string_view op, const nlohmann::json& request_id,
                             const nlohmann::json& request, bool include_result) const;
    nlohmann::json cancel(std::string_view op, const nlohmann::json& request_id,
                          const nlohmann::json& request) const;
    nlohmann::json server_stats(std::string_view op, const nlohmann::json& request_id) const;

    JobManager& jobs_;
    StatsProvider server_stats_;
};

}  // namespace concurrentcore
