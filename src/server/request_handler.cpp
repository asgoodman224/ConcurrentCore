#include "concurrentcore/server/request_handler.hpp"

#include "concurrentcore/common/json_util.hpp"
#include "concurrentcore/networking/protocol.hpp"

#include <spdlog/spdlog.h>

#include <chrono>
#include <utility>

namespace concurrentcore {

using nlohmann::json;
namespace proto = protocol;

namespace {

constexpr std::string_view kUnknownOp = "ERROR";

// Extracts and validates "job_id". Throws CoreError(InvalidParams).
JobId require_job_id(const json& request) {
    const auto it = request.find("job_id");
    const auto id = it == request.end() ? std::nullopt : as_non_negative_integer(*it);
    if (!id || *id == 0) {
        throw CoreError(ErrorCode::InvalidParams, "'job_id' (positive integer) is required");
    }
    return *id;
}

}  // namespace

RequestHandler::RequestHandler(JobManager& jobs, StatsProvider server_stats)
    : jobs_(jobs), server_stats_(std::move(server_stats)) {}

std::string RequestHandler::handle(std::string_view payload, const EventSink& sink) const {
    json request;
    try {
        request = proto::parse(payload);
    } catch (const CoreError& e) {
        return proto::serialize(proto::make_error(kUnknownOp, nullptr, e.code(), e.what()));
    }
    return proto::serialize(handle_message(request, sink));
}

json RequestHandler::handle_message(const json& request, const EventSink& sink) const {
    if (!request.is_object()) {
        return proto::make_error(kUnknownOp, nullptr, ErrorCode::InvalidRequest,
                                 "request must be a JSON object");
    }

    // request_id is echoed verbatim so clients can correlate pipelined
    // requests. Only strings and integers are accepted.
    json request_id = nullptr;
    if (const auto it = request.find("request_id"); it != request.end() && !it->is_null()) {
        if (!it->is_string() && !it->is_number_integer()) {
            return proto::make_error(kUnknownOp, nullptr, ErrorCode::InvalidRequest,
                                     "'request_id' must be a string or an integer");
        }
        request_id = *it;
    }

    const auto type_it = request.find("type");
    if (type_it == request.end() || !type_it->is_string()) {
        return proto::make_error(kUnknownOp, request_id, ErrorCode::InvalidRequest,
                                 "'type' (string) is required");
    }
    const auto& type_text = type_it->get_ref<const std::string&>();
    const auto op = proto::operation_from_string(type_text);
    if (!op) {
        return proto::make_error(kUnknownOp, request_id, ErrorCode::UnknownOperation,
                                 "unknown operation '" + type_text + "'");
    }
    const std::string_view op_name = proto::to_string(*op);

    try {
        switch (*op) {
            case proto::Operation::SubmitJob:
                return submit(op_name, request_id, request, sink);
            case proto::Operation::GetJobStatus:
                return job_query(op_name, request_id, request, /*include_result=*/false);
            case proto::Operation::GetJobResult:
                return job_query(op_name, request_id, request, /*include_result=*/true);
            case proto::Operation::CancelJob:
                return cancel(op_name, request_id, request);
            case proto::Operation::GetServerStats:
                return server_stats(op_name, request_id);
            case proto::Operation::Ping: {
                const auto now = std::chrono::duration_cast<std::chrono::milliseconds>(
                    std::chrono::system_clock::now().time_since_epoch());
                return proto::make_ok(op_name, request_id,
                                      {{"pong", true},
                                       {"protocol_version", proto::kProtocolVersion},
                                       {"server_time_ms", now.count()}});
            }
        }
    } catch (const CoreError& e) {
        return proto::make_error(op_name, request_id, e.code(), e.what());
    } catch (const std::exception& e) {
        // Unexpected: report as INTERNAL_ERROR, keep the connection alive.
        spdlog::error("unexpected error handling {}: {}", op_name, e.what());
        return proto::make_error(op_name, request_id, ErrorCode::InternalError, e.what());
    }
    return proto::make_error(op_name, request_id, ErrorCode::InternalError, "unhandled operation");
}

json RequestHandler::submit(std::string_view op, const json& request_id, const json& request,
                            const EventSink& sink) const {
    const auto type_it = request.find("task_type");
    if (type_it == request.end() || !type_it->is_string()) {
        throw CoreError(ErrorCode::InvalidParams, "'task_type' (string) is required");
    }
    const auto params_it = request.find("params");
    const json params = params_it == request.end() ? json::object() : *params_it;

    bool notify = false;
    if (const auto it = request.find("notify"); it != request.end() && !it->is_null()) {
        if (!it->is_boolean()) {
            throw CoreError(ErrorCode::InvalidParams, "'notify' must be a boolean");
        }
        notify = it->get<bool>();
    }

    // Validation happens here, on submit, so malformed jobs never reach the
    // queue and the client gets immediate feedback.
    TaskSpec spec = parse_task_spec(type_it->get<std::string>(), params, jobs_.limits());

    Job::CompletionCallback on_complete;
    if (notify && sink) {
        // Captures a copy of the sink (which holds only a weak reference to
        // the connection) and the request id for correlation.
        on_complete = [sink, request_id](const JobSnapshot& snapshot) {
            sink(proto::serialize(
                proto::make_event(proto::kJobFinishedEvent, request_id, to_json(snapshot, true))));
        };
    }

    const SubmitResult submitted = jobs_.submit(std::move(spec), std::move(on_complete));
    if (!submitted.ok()) {
        const std::string message = submitted.error == ErrorCode::QueueFull
                                        ? "task queue is full; retry later"
                                        : "server is shutting down";
        return proto::make_error(op, request_id, submitted.error, message);
    }
    return proto::make_ok(op, request_id,
                          {{"job_id", *submitted.job_id}, {"status", "QUEUED"}, {"notify", notify}});
}

json RequestHandler::job_query(std::string_view op, const json& request_id, const json& request,
                               bool include_result) const {
    const JobId id = require_job_id(request);
    const auto snapshot = jobs_.snapshot(id);
    if (!snapshot) {
        return proto::make_error(op, request_id, ErrorCode::JobNotFound,
                                 "job " + std::to_string(id) + " not found (unknown or evicted)");
    }
    if (include_result && !is_terminal(snapshot->status)) {
        return proto::make_error(op, request_id, ErrorCode::JobNotFinished,
                                 "job " + std::to_string(id) + " is " +
                                     std::string(to_string(snapshot->status)));
    }
    return proto::make_ok(op, request_id, to_json(*snapshot, include_result));
}

json RequestHandler::cancel(std::string_view op, const json& request_id,
                            const json& request) const {
    const JobId id = require_job_id(request);
    const CancelOutcome outcome = jobs_.cancel(id);
    if (outcome == CancelOutcome::NotFound) {
        return proto::make_error(op, request_id, ErrorCode::JobNotFound,
                                 "job " + std::to_string(id) + " not found (unknown or evicted)");
    }
    return proto::make_ok(op, request_id, {{"job_id", id}, {"outcome", to_string(outcome)}});
}

json RequestHandler::stats_to_json(const JobManagerStats& s) {
    return {
        {"uptime_seconds", s.uptime_seconds},
        {"accepting_jobs", s.accepting},
        {"jobs",
         {{"submitted", s.submitted},
          {"rejected", s.rejected},
          {"completed", s.completed},
          {"failed", s.failed},
          {"cancelled", s.cancelled},
          {"running", s.running},
          {"retained", s.retained_jobs}}},
        {"queue", {{"depth", s.queue_depth}, {"capacity", s.queue_capacity}}},
        {"workers", {{"total", s.worker_count}, {"active", s.active_workers}}},
        {"latency_ms",
         {{"avg_execution", s.avg_execution_ms}, {"avg_queue_wait", s.avg_queue_wait_ms}}},
        {"throughput", {{"jobs_per_second", s.jobs_per_second}}},
    };
}

json RequestHandler::server_stats(std::string_view op, const json& request_id) const {
    json stats = stats_to_json(jobs_.stats());
    if (server_stats_) {
        stats.update(server_stats_());
    }
    return proto::make_ok(op, request_id, std::move(stats));
}

}  // namespace concurrentcore
