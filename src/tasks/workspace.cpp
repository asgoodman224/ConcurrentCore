#include "concurrentcore/tasks/workspace.hpp"

#include "concurrentcore/common/error.hpp"

#include <algorithm>
#include <array>
#include <cctype>
#include <stdexcept>
#include <string>
#include <system_error>

namespace fs = std::filesystem;

namespace concurrentcore {
namespace {

constexpr std::size_t kMaxPathBytes = 1024;

[[noreturn]] void reject(std::string_view user_path, std::string_view why) {
    throw CoreError(ErrorCode::PathNotAllowed,
                    "path '" + std::string(user_path) + "' rejected: " + std::string(why));
}

bool is_reserved_device_name(std::string_view component) {
    // Windows treats these as devices regardless of extension ("nul.txt").
    static constexpr std::array<std::string_view, 22> kReserved{
        "CON",  "PRN",  "AUX",  "NUL",  "COM1", "COM2", "COM3", "COM4",
        "COM5", "COM6", "COM7", "COM8", "COM9", "LPT1", "LPT2", "LPT3",
        "LPT4", "LPT5", "LPT6", "LPT7", "LPT8", "LPT9"};
    const std::string_view stem = component.substr(0, component.find('.'));
    std::string upper(stem);
    std::transform(upper.begin(), upper.end(), upper.begin(),
                   [](unsigned char c) { return static_cast<char>(std::toupper(c)); });
    return std::find(kReserved.begin(), kReserved.end(), upper) != kReserved.end();
}

// fs::path from UTF-8 text on every platform.
fs::path from_utf8(std::string_view text) {
    return fs::path(std::u8string(text.begin(), text.end()));
}

}  // namespace

Workspace::Workspace(const fs::path& root) {
    std::error_code ec;
    const fs::path canonical = fs::canonical(root, ec);
    if (ec || !fs::is_directory(canonical, ec)) {
        throw std::invalid_argument("workspace directory does not exist: " + root.string());
    }
    root_ = canonical;
}

void Workspace::validate_relative(std::string_view user_path) {
    if (user_path.empty()) {
        reject(user_path, "empty path");
    }
    if (user_path.size() > kMaxPathBytes) {
        reject(user_path, "path too long");
    }
    if (user_path.find('\0') != std::string_view::npos) {
        reject(user_path, "contains NUL byte");
    }
    if (user_path.find(':') != std::string_view::npos) {
        reject(user_path, "':' is not allowed (drive letters / alternate data streams)");
    }
    if (user_path.find('\\') != std::string_view::npos) {
        reject(user_path, "use '/' as the path separator");
    }
    if (user_path.front() == '/') {
        reject(user_path, "absolute paths are not allowed");
    }
    const fs::path p = from_utf8(user_path);
    if (p.is_absolute() || p.has_root_name() || p.has_root_directory()) {
        reject(user_path, "absolute paths are not allowed");
    }
    for (const auto& component : p) {
        // u8string() never throws on non-ASCII names (string() can on Windows).
        const std::u8string u8 = component.u8string();
        const std::string part(u8.begin(), u8.end());
        if (part == "..") {
            reject(user_path, "'..' components are not allowed");
        }
        if (!part.empty() && part != "." && is_reserved_device_name(part)) {
            reject(user_path, "reserved device name");
        }
    }
}

bool Workspace::contains(const fs::path& canonical_path) const {
    // Component-wise prefix comparison. A plain string prefix test would
    // wrongly accept "/srv/ws-evil" for root "/srv/ws".
    auto root_it = root_.begin();
    auto path_it = canonical_path.begin();
    for (; root_it != root_.end(); ++root_it, ++path_it) {
        if (path_it == canonical_path.end() || *root_it != *path_it) {
            return false;
        }
    }
    return true;
}

fs::path Workspace::resolve_input(std::string_view user_path) const {
    validate_relative(user_path);
    const fs::path joined = root_ / from_utf8(user_path);

    std::error_code ec;
    const fs::path real = fs::canonical(joined, ec);  // resolves symlinks
    if (ec) {
        throw CoreError(ErrorCode::FileNotFound, "file not found: " + std::string(user_path));
    }
    if (!contains(real)) {
        reject(user_path, "resolves outside the workspace");
    }
    if (!fs::is_regular_file(real, ec)) {
        throw CoreError(ErrorCode::FileNotFound, "not a regular file: " + std::string(user_path));
    }
    return real;
}

fs::path Workspace::resolve_output(std::string_view user_path) const {
    validate_relative(user_path);
    const fs::path joined = (root_ / from_utf8(user_path)).lexically_normal();
    const fs::path filename = joined.filename();
    if (filename.empty() || filename == "." || filename == "..") {
        reject(user_path, "output must name a file");
    }

    std::error_code ec;
    const fs::path parent = fs::canonical(joined.parent_path(), ec);
    if (ec) {
        throw CoreError(ErrorCode::FileNotFound,
                        "output directory does not exist: " + std::string(user_path));
    }
    if (!contains(parent)) {
        reject(user_path, "resolves outside the workspace");
    }

    const fs::path target = parent / filename;
    const fs::file_status st = fs::symlink_status(target, ec);
    if (fs::is_symlink(st)) {
        reject(user_path, "output must not be a symlink");
    }
    if (fs::is_directory(st)) {
        reject(user_path, "output is a directory");
    }
    if (fs::exists(st) && !fs::is_regular_file(st)) {
        reject(user_path, "output exists and is not a regular file");
    }
    return target;
}

}  // namespace concurrentcore
