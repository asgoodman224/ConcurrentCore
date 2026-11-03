#include "concurrentcore/tasks/workspace.hpp"

#include "concurrentcore/common/error.hpp"
#include "support/test_support.hpp"

#include <gtest/gtest.h>

using namespace concurrentcore;
namespace fs = std::filesystem;

namespace {

ErrorCode code_of(const std::function<void()>& fn) {
    try {
        fn();
    } catch (const CoreError& e) {
        return e.code();
    }
    return ErrorCode::InternalError;  // sentinel: nothing thrown
}

}  // namespace

TEST(Workspace, RequiresExistingDirectory) {
    EXPECT_THROW(Workspace("definitely/not/a/real/dir"), std::invalid_argument);
}

TEST(Workspace, LexicalValidationRejectsUnsafePaths) {
    const std::vector<std::string> bad{
        "",          "/etc/passwd", "../secret",   "a/../../b", "a/..",
        "C:/x",      "C:x",         "a\\b",        "file:ads",  "nul",
        "dir/CON.txt", "com1",      std::string("a\0b", 3), std::string(2000, 'a')};
    for (const auto& p : bad) {
        EXPECT_EQ(code_of([&] { Workspace::validate_relative(p); }), ErrorCode::PathNotAllowed)
            << "path should be rejected: '" << p << "'";
    }
}

TEST(Workspace, LexicalValidationAcceptsOrdinaryRelativePaths) {
    for (const std::string p : {"a.txt", "dir/a.txt", "./a.txt", "dir/sub/file.tar.gz", "connect.txt"}) {
        EXPECT_NO_THROW(Workspace::validate_relative(p)) << p;
    }
}

TEST(Workspace, ResolveInputFindsFilesInsideRoot) {
    cctest::TempDir dir;
    fs::create_directories(dir.path() / "sub");
    cctest::write_file(dir.path() / "sub" / "f.txt", "x");
    const Workspace ws(dir.path());
    const auto p = ws.resolve_input("sub/f.txt");
    EXPECT_EQ(p, fs::canonical(dir.path() / "sub" / "f.txt"));
}

TEST(Workspace, ResolveInputReportsMissingAndNonRegularFiles) {
    cctest::TempDir dir;
    fs::create_directories(dir.path() / "sub");
    const Workspace ws(dir.path());
    EXPECT_EQ(code_of([&] { (void)ws.resolve_input("missing.txt"); }), ErrorCode::FileNotFound);
    EXPECT_EQ(code_of([&] { (void)ws.resolve_input("sub"); }), ErrorCode::FileNotFound);
}

TEST(Workspace, SymlinkEscapingTheRootIsRejected) {
    cctest::TempDir outside;
    cctest::TempDir dir;
    cctest::write_file(outside.path() / "secret.txt", "secret");
    std::error_code ec;
    fs::create_symlink(outside.path() / "secret.txt", dir.path() / "link.txt", ec);
    if (ec) {
        GTEST_SKIP() << "cannot create symlinks here (Windows needs Developer Mode): " << ec.message();
    }
    const Workspace ws(dir.path());
    EXPECT_EQ(code_of([&] { (void)ws.resolve_input("link.txt"); }), ErrorCode::PathNotAllowed);
    EXPECT_EQ(code_of([&] { (void)ws.resolve_output("link.txt"); }), ErrorCode::PathNotAllowed);
}

TEST(Workspace, SiblingDirectorySharingAPrefixIsOutside) {
    // Root ".../ws" must not contain ".../ws-evil" (string-prefix bug).
    cctest::TempDir base;
    fs::create_directories(base.path() / "ws");
    fs::create_directories(base.path() / "ws-evil");
    cctest::write_file(base.path() / "ws-evil" / "x.txt", "x");
    std::error_code ec;
    fs::create_directory_symlink(base.path() / "ws-evil", base.path() / "ws" / "jump", ec);
    if (ec) {
        GTEST_SKIP() << "cannot create symlinks here: " << ec.message();
    }
    const Workspace ws(base.path() / "ws");
    EXPECT_EQ(code_of([&] { (void)ws.resolve_input("jump/x.txt"); }), ErrorCode::PathNotAllowed);
}

TEST(Workspace, ResolveOutputRules) {
    cctest::TempDir dir;
    fs::create_directories(dir.path() / "out");
    const Workspace ws(dir.path());
    EXPECT_EQ(ws.resolve_output("out/new.bin"), fs::canonical(dir.path() / "out") / "new.bin");
    EXPECT_EQ(code_of([&] { (void)ws.resolve_output("missing-dir/new.bin"); }), ErrorCode::FileNotFound);
    EXPECT_EQ(code_of([&] { (void)ws.resolve_output("out"); }), ErrorCode::PathNotAllowed);
    EXPECT_EQ(code_of([&] { (void)ws.resolve_output("."); }), ErrorCode::PathNotAllowed);
}
