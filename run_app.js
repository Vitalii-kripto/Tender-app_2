import { spawn } from "child_process";
import { existsSync } from "fs";
import { resolve, join } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname  = resolve(__filename, "..");

const VENV_PYTHON_WIN  = join(__dirname, ".venv", "Scripts", "python.exe");
const VENV_PYTHON_UNIX = join(__dirname, ".venv", "bin", "python");
const PYTHON_CMD = existsSync(VENV_PYTHON_WIN)  ? VENV_PYTHON_WIN
                 : existsSync(VENV_PYTHON_UNIX) ? VENV_PYTHON_UNIX
                 : "python";

const BACKEND_CMD  = ["-m", "uvicorn", "backend.main:app", "--reload", "--port", "8000"];
const FRONTEND_CMD = ["run", "dev:frontend"];

function startProcess(label, cmd, args) {
  console.log(`[${label}] Запуск: ${cmd} ${args.join(" ")}`);
  const proc = spawn(cmd, args, {
    cwd: __dirname,
    shell: process.platform === "win32",
    stdio: "pipe",
    env: { ...process.env },
  });
  proc.stdout.on("data", (d) => d.toString().trimEnd().split("\n").forEach(l => console.log(`[${label}] ${l}`)));
  proc.stderr.on("data", (d) => d.toString().trimEnd().split("\n").forEach(l => console.log(`[${label}] ${l}`)));
  proc.on("error", (e) => console.error(`[${label}] Ошибка: ${e.message}`));
  proc.on("close", (code) => console.log(`[${label}] Завершён с кодом ${code}`));
  return proc;
}

const backend  = startProcess("BACKEND",  PYTHON_CMD, BACKEND_CMD);
const frontend = startProcess("FRONTEND", "npm",      FRONTEND_CMD);

function shutdown() {
  [backend, frontend].forEach(p => {
    if (!p.killed) {
      if (process.platform === "win32") {
        spawn("taskkill", ["/pid", String(p.pid), "/f", "/t"], { shell: true });
      } else {
        p.kill("SIGTERM");
      }
    }
  });
  setTimeout(() => process.exit(0), 1500);
}

process.on("SIGINT",  shutdown);
process.on("SIGTERM", shutdown);
