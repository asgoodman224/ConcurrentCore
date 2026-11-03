#pragma once

#include <memory>
#include <type_traits>
#include <utility>

namespace concurrentcore {

/// A move-only, type-erased `void()` callable.
///
/// std::function requires copyable targets, which rules out storing a
/// std::packaged_task or a lambda that owns a std::unique_ptr. C++23 adds
/// std::move_only_function; this is the minimal C++20 equivalent so the
/// thread pool can queue move-only work without an extra shared_ptr
/// allocation per task.
class UniqueTask {
public:
    UniqueTask() = default;

    template <typename F>
        requires(!std::is_same_v<std::decay_t<F>, UniqueTask> &&
                 std::is_invocable_r_v<void, std::decay_t<F>&>)
    UniqueTask(F&& fn)  // NOLINT(google-explicit-constructor): implicit by design, like std::function
        : impl_(std::make_unique<Model<std::decay_t<F>>>(std::forward<F>(fn))) {}

    UniqueTask(UniqueTask&&) noexcept = default;
    UniqueTask& operator=(UniqueTask&&) noexcept = default;
    UniqueTask(const UniqueTask&) = delete;
    UniqueTask& operator=(const UniqueTask&) = delete;
    ~UniqueTask() = default;

    void operator()() { impl_->invoke(); }

    [[nodiscard]] explicit operator bool() const noexcept { return impl_ != nullptr; }

private:
    struct Concept {
        Concept() = default;
        Concept(const Concept&) = delete;
        Concept& operator=(const Concept&) = delete;
        Concept(Concept&&) = delete;
        Concept& operator=(Concept&&) = delete;
        virtual ~Concept() = default;
        virtual void invoke() = 0;
    };

    template <typename F>
    struct Model final : Concept {
        template <typename G>
        explicit Model(G&& g) : fn(std::forward<G>(g)) {}
        void invoke() override { fn(); }
        F fn;
    };

    std::unique_ptr<Concept> impl_;
};

}  // namespace concurrentcore
