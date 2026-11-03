#include "concurrentcore/jobs/job_registry.hpp"

#include <stdexcept>
#include <utility>

namespace concurrentcore {

JobRegistry::JobRegistry(std::size_t max_retained_finished) : max_retained_(max_retained_finished) {
    if (max_retained_ == 0) {
        throw std::invalid_argument("JobRegistry retention must be greater than zero");
    }
}

void JobRegistry::insert(std::shared_ptr<Job> job) {
    if (!job) {
        throw std::invalid_argument("JobRegistry::insert: null job");
    }
    const JobId id = job->id();
    std::lock_guard lock(mutex_);
    const auto [it, inserted] = jobs_.emplace(id, std::move(job));
    if (!inserted) {
        throw std::logic_error("JobRegistry::insert: duplicate job id");
    }
}

bool JobRegistry::erase(JobId id) {
    // The erased shared_ptr is moved out and destroyed after the lock is
    // released, so a Job destructor never runs under the registry lock.
    std::shared_ptr<Job> removed;
    {
        std::lock_guard lock(mutex_);
        const auto it = jobs_.find(id);
        if (it == jobs_.end()) {
            return false;
        }
        removed = std::move(it->second);
        jobs_.erase(it);
    }
    return true;
}

std::shared_ptr<Job> JobRegistry::find(JobId id) const {
    std::lock_guard lock(mutex_);
    const auto it = jobs_.find(id);
    return it == jobs_.end() ? nullptr : it->second;
}

std::size_t JobRegistry::mark_finished(JobId id) {
    std::vector<std::shared_ptr<Job>> evicted;  // destroyed outside the lock
    {
        std::lock_guard lock(mutex_);
        if (!jobs_.contains(id)) {
            return 0;
        }
        finished_order_.push_back(id);
        while (finished_order_.size() > max_retained_) {
            const JobId oldest = finished_order_.front();
            finished_order_.pop_front();
            const auto it = jobs_.find(oldest);
            if (it != jobs_.end()) {
                evicted.push_back(std::move(it->second));
                jobs_.erase(it);
            }
        }
    }
    return evicted.size();
}

std::vector<std::shared_ptr<Job>> JobRegistry::all() const {
    std::lock_guard lock(mutex_);
    std::vector<std::shared_ptr<Job>> out;
    out.reserve(jobs_.size());
    for (const auto& [id, job] : jobs_) {
        out.push_back(job);
    }
    return out;
}

std::size_t JobRegistry::size() const {
    std::lock_guard lock(mutex_);
    return jobs_.size();
}

std::size_t JobRegistry::finished_count() const {
    std::lock_guard lock(mutex_);
    return finished_order_.size();
}

}  // namespace concurrentcore
