#pragma once

// Internal file helpers shared by the file-based tasks. Not part of the
// public API.

#include <cstdio>
#include <filesystem>
#include <span>
#include <string>

namespace concurrentcore::detail {

/// RAII read-only binary file handle with chunked reads.
class InputFile {
public:
    explicit InputFile(const std::filesystem::path& path);
    ~InputFile();
    InputFile(const InputFile&) = delete;
    InputFile& operator=(const InputFile&) = delete;
    InputFile(InputFile&&) = delete;
    InputFile& operator=(InputFile&&) = delete;

    /// Reads up to buffer.size() bytes. Returns 0 at end of file.
    /// Throws CoreError(IoError) on read failure.
    std::size_t read(std::span<char> buffer);

private:
    std::FILE* file_ = nullptr;
    std::string display_name_;
};

/// Writes to a uniquely named temporary file next to the destination and
/// moves it into place on commit(). If commit() is never reached (error,
/// exception, cancellation) the destructor deletes the temporary file, so a
/// failed job never leaves a partial output behind.
class AtomicOutputFile {
public:
    AtomicOutputFile(std::filesystem::path destination, bool overwrite);
    ~AtomicOutputFile();
    AtomicOutputFile(const AtomicOutputFile&) = delete;
    AtomicOutputFile& operator=(const AtomicOutputFile&) = delete;
    AtomicOutputFile(AtomicOutputFile&&) = delete;
    AtomicOutputFile& operator=(AtomicOutputFile&&) = delete;

    void write(std::span<const char> data);

    /// Flushes, closes and publishes the file. Without `overwrite` the
    /// publish step itself refuses to replace an existing file (atomic
    /// no-replace rename), so two concurrent jobs cannot clobber each other.
    /// Throws CoreError(OutputExists | IoError).
    void commit();

private:
    void close_handle() noexcept;

    std::filesystem::path destination_;
    std::filesystem::path temp_path_;
    bool overwrite_;
    std::FILE* file_ = nullptr;
    bool committed_ = false;
};

/// Opens a std::FILE* from a filesystem path (wide API on Windows so
/// non-ASCII names work).
std::FILE* open_file(const std::filesystem::path& path, const char* mode);

}  // namespace concurrentcore::detail
