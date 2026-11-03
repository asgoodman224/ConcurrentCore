# Sends CTRL+C to a console process on Windows, the equivalent of SIGINT,
# so the server's graceful shutdown path can be exercised from a script.
# Usage: powershell -File scripts/send_ctrl_c.ps1 -ProcessId <pid>
param([Parameter(Mandatory = $true)][int]$ProcessId)
$signature = @'
using System;
using System.Runtime.InteropServices;
public static class CtrlC {
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool AttachConsole(uint pid);
    [DllImport("kernel32.dll", SetLastError = true)] public static extern bool FreeConsole();
    [DllImport("kernel32.dll")] public static extern bool SetConsoleCtrlHandler(IntPtr handler, bool add);
    [DllImport("kernel32.dll")] public static extern bool GenerateConsoleCtrlEvent(uint ev, uint group);
}
'@
Add-Type -TypeDefinition $signature
[CtrlC]::FreeConsole() | Out-Null
if (-not [CtrlC]::AttachConsole([uint32]$ProcessId)) { throw "AttachConsole($ProcessId) failed" }
[CtrlC]::SetConsoleCtrlHandler([IntPtr]::Zero, $true) | Out-Null   # do not kill ourselves
[CtrlC]::GenerateConsoleCtrlEvent(0, 0) | Out-Null                  # 0 = CTRL_C_EVENT
Start-Sleep -Milliseconds 200
[CtrlC]::FreeConsole() | Out-Null
