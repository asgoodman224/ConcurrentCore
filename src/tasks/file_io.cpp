#include "file_io.hpp"

#include "concurrentcore/common/error.hpp"

#include <cerrno>
#include <cstdint>
#include <random>
#include <system_error>

#ifdef _WIN32
#include <windows.h>
#else
#include <unistd.h>
#endif

namespace fs = std::filesystem;

namespace concurrentcore::detail {
namespace {

std::string display(const fs::path& p) {
    const std::u8string u8 = p.filename().u8string();
    return std::string(u8.begin(), u8.end());
}

std::string random_suffix() {
    thread_local std::mt19937_64 rng{std::random_device{}()};
    static constexpr char kHex[] = "0123456789abcdef";
    std::uint64_t value = rng();
    std::string out(16, '0');
    for (auto& c : out) {
        c = kHex[value & 0xF];
        value >>= 4;
    }
    return out;
}

}  // namespace

std::FILE* open_file(const fs::path& path, const char* mode) {
#ifdef _WIN32
    std::wstring wmode;
    for (const char* m = mode; *m != '\0'; ++m) {
        wmode.push_back(static_cast<wchar_t>(*m));
    }
    return _wfopen(path.c_str(), wmode.c_str());
#else
    return std::fopen(path.c_str(), mode);
#endif
}

// ---------------------------------------------------------------- InputFile

InputFile::InputFile(const fs::path& path)
    : file_(open_file(path, "rb")), display_name_(display(path)) {
    if (file_ == nullptr) {
        throw CoreError(ErrorCode::IoError, "cannot open '" + display_name_ + "' for reading");
    }
}

InputFile::~InputFile() {
    if (file_ != nullptr) {
        std::fclose(file_);
    }
}

std::size_t InputFile::read(std::span<char> buffer) {
    const std::size_t n = std::fread(buffer.data(), 1, buffer.size(), file_);
    if (n < buffer.size() && std::ferror(file_) != 0) {
        throw CoreError(ErrorCode::IoError, "read error on '" + display_name_ + "'");
    }
    return n;
}

// --------------------------------------------------------- AtomicOutputFile

AtomicOutputFile::AtomicOutputFile(fs::path destination, bool overwrite)
    : destination_(std::move(destination)), overwrite_(overwrite) {
    std::error_code ec;
    if (!overwrite_ && fs::exists(destination_, ec)) {
        // Fail fast before doing any work; commit() re-checks atomically.
        throw CoreError(ErrorCode::OutputExists,
                        "output '" + display(destination_) + "' exists (set overwrite=true)");
    }
    const fs::path dir = destination_.parent_path();
    const std::u8string base = destination_.filename().u8string();
    for (int attempt = 0; attempt < 8 && file_ == nullptr; ++attempt) {
        std::u8string name = u8".";
        name += base;
        name += u8".";
        const std::string suffix = random_suffix();
        name.append(suffix.begin(), suffix.end());
        name += u8".ccpart";
        temp_path_ = dir / fs::path(name);
        // "x": exclusive create; never reuses or follows an existing file.
        file_ = open_file(temp_path_, "wbx");
    }
    if (file_ == nullptr) {
        throw CoreError(ErrorCode::IoError,
                        "cannot create temporary file for '" + display(destination_) + "'");
    }
}

AtomicOutputFile::~AtomicOutputFile() {
    close_handle();
    if (!committed_ && !temp_path_.empty()) {
        std::error_code ec;
        fs::remove(temp_path_, ec);  // best effort; nothing useful to do on failure
    }
}

void AtomicOutputFile::close_handle() noexcept {
    if (file_ != nullptr) {
        std::fclose(file_);
        file_ = nullptr;
    }
}

void AtomicOutputFile::write(std::span<const char> data) {
    if (data.empty()) {
        return;
    }
    if (std::fwrite(data.data(), 1, data.size(), file_) != data.size()) {
        throw CoreError(ErrorCode::IoError, "write error on '" + display(destination_) + "'");
    }
}

void AtomicOutputFile::commit() {
    if (std::fflush(file_) != 0) {
        throw CoreError(ErrorCode::IoError, "flush failed on '" + display(destination_) + "'");
    }
    const bool close_ok = std::fclose(file_) == 0;
    file_ = nullptr;
    if (!close_ok) {
        throw CoreError(ErrorCode::IoError, "close failed on '" + display(destination_) + "'");
    }

#ifdef _WIN32
    const DWORD flags = overwrite_ ? MOVEFILE_REPLACE_EXISTING : 0;
    if (MoveFileExW(temp_path_.c_str(), destination_.c_str(), flags) == 0) {
        const DWORD err = GetLastError();
        if (err == ERROR_ALREADY_EXISTS || err == ERROR_FILE_EXISTS) {
            throw CoreError(ErrorCode::OutputExists,
                            "output '" + display(destination_) + "' exists (set overwrite=true)");
        }
        throw CoreError(ErrorCode::IoError,
                        "cannot publish output '" + display(destination_) +
                            "' (Windows error " + std::to_string(err) + ")");
    }
#else
    if (overwrite_) {
        std::error_code ec;
        fs::rename(temp_path_, destination_, ec);
        if (ec) {
            throw CoreError(ErrorCode::IoError, "cannot publish output '" +
                                                    display(destination_) + "': " + ec.message());
        }
    } else {
        // link() fails with EEXIST if the destination exists: an atomic
        // "rename without replace" that works on every POSIX filesystem
        // supporting hard links.
        if (::link(temp_path_.c_str(), destination_.c_str()) != 0) {
            const int err = errno;
            if (err == EEXIST) {
                throw CoreError(ErrorCode::OutputExists, "output '" + display(destination_) +
                                                             "' exists (set overwrite=true)");
            }
            throw CoreError(ErrorCode::IoError,
                            "cannot publish output '" + display(destination_) +
                                "': " + std::generic_category().message(err));
        }
        std::error_code ec;
        fs::remove(temp_path_, ec);
    }
#endif
    committed_ = true;
}

}  // namespace concurrentcore::detail
