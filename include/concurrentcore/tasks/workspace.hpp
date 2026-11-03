#pragma once

#include <filesystem>
#include <string_view>

namespace concurrentcore {

/// Sandbox that confines every task file operation to one directory tree.
///
/// Client-supplied paths are untrusted. A path is accepted only if:
///   * it is non-empty, relative, and at most 1024 bytes long;
///   * it contains no "..", no root/drive component, no NUL or ':' (Windows
///     drive letters / alternate data streams), and no backslashes;
///   * no component is a Windows reserved device name (CON, NUL, COM1, ...);
///   * after resolving symlinks, the real path still lies inside the root.
///
/// Workspace is immutable after construction, so it is safe to share
/// between threads without synchronization.
///
/// Limitation: checks are path based. A local user who can create symlinks
/// inside the workspace could race a check (TOCTOU). Defending against that
/// requires openat()/O_NOFOLLOW-style descriptor-relative I/O and is out of
/// scope; the workspace should only be writable by the server.
class Workspace {
public:
    /// `root` must exist and be a directory. Throws std::invalid_argument.
    explicit Workspace(const std::filesystem::path& root);

    [[nodiscard]] const std::filesystem::path& root() const noexcept { return root_; }

    /// Lexical validation only (no filesystem access).
    /// Throws CoreError(PathNotAllowed).
    static void validate_relative(std::string_view user_path);

    /// Resolves an existing regular file for reading.
    /// Throws CoreError(PathNotAllowed | FileNotFound).
    [[nodiscard]] std::filesystem::path resolve_input(std::string_view user_path) const;

    /// Resolves a destination path for writing. The parent directory must
    /// already exist inside the workspace; the target itself may or may not
    /// exist but must not be a directory or a symlink.
    /// Throws CoreError(PathNotAllowed | FileNotFound).
    [[nodiscard]] std::filesystem::path resolve_output(std::string_view user_path) const;

private:
    [[nodiscard]] bool contains(const std::filesystem::path& canonical_path) const;

    std::filesystem::path root_;  // canonical
};

}  // namespace concurrentcore
