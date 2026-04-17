// run_app.js
import { spawn } from "child_process";
import { existsSync, mkdirSync, createWriteStream } from "fs";
import { resolve, join } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = resolve(__filename, "..");

const LOG_DIR = join(__dirname, "logs");
const LOG_FILE = join(LOG_DIR, "tendersmart.log");
mkdirSync(LOG_DIR, { recursive: true });

const logStream = createWriteStream(LOG_FILE, {
  flags: "w",
  encoding: "utf8",
});

const VENV_PYTHON_WIN = join(__dirname, ".venv", "Scripts", "python.exe");
const VENV_PYTHON_UNIX = join(__dirname, ".venv", "bin", "python");
const isWin = process.platform === "win32";
const PYTHON_CMD = existsSync(VENV_PYTHON_WIN)
  ? VENV_PYTHON_WIN
  : existsSync(VENV_PYTHON_UNIX)
    ? VENV_PYTHON_UNIX
    : isWin ? "python" : "python3";

const BACKEND_CMD = ["-m", "uvicorn", "backend.main:app", "--reload", "--port", "8000"];
const FRONTEND_CMD = ["run", "dev:frontend"];

const C = {
  reset: "\x1b[0m",
  blue: "\x1b[34m",
  green: "\x1b[32m",
  red: "\x1b[31m",
  yellow: "\x1b[33m",
  cyan: "\x1b[36m",
};

function tag(name, color) {
  return `${color}[${name}]${C.reset}`;
}

function ts() {
  return new Date().toISOString().replace("T", " ").replace("Z", "");
}

function writeFileLog(source, line) {
  const text = String(line ?? "").replace(/\r?\n$/, "");
  if (!text.trim()) return;
  logStream.write(`${ts()} | ${source} | ${text}\n`);
}

function shouldMirrorBackendLineToFile(line) {
  const text = String(line ?? "");
  // backend/logger.py / logging_setup.py уже сами пишут такие строки в tendersmart.log
  if (/^\d{4}-\d{2}-\d{2}\s/.test(text)) return false;
  return true;
}

function writeConsoleLines(label, color, data, options = {}) {
  const {
    writeToFile = false,
    backendMirror = false,
    isError = false,
  } = options;

  const lines = data.toString("utf8").split(/\r?\n/).filter(Boolean);
  for (const line of lines) {
    const prefix = isError ? tag(label, C.red) : tag(label, color);
    console.log(`${prefix} ${line}`);

    if (writeToFile) {
      if (backendMirror) {
        if (shouldMirrorBackendLineToFile(line)) {
          writeFileLog(label, line);
        }
      } else {
        writeFileLog(label, line);
      }
    }
  }
}

function startProcess(label, color, cmd, args, cwd = __dirname) {
  console.log(`${tag(label, color)} Запуск: ${cmd} ${args.join(" ")}`);
  writeFileLog("APP", `${label} start: ${cmd} ${args.join(" ")}`);

  const proc = spawn(cmd, args, {
    cwd,
    shell: process.platform === "win32",
    stdio: "pipe",
    env: { ...process.env },
  });

  proc.stdout.on("data", (data) => {
    writeConsoleLines(label, color, data, {
      writeToFile: label !== "BACKEND" || true,
      backendMirror: label === "BACKEND",
      isError: false,
    });
  });

  proc.stderr.on("data", (data) => {
    const lines = data.toString("utf8").split(/\r?\n/).filter(Boolean);
    for (const line of lines) {
      const isInfoLike = /INFO|WARNING|DEBUG|VITE|ready in|Local:|Network:|DeprecationWarning/.test(line);
      const prefix = isInfoLike ? tag(label, color) : tag(label, C.red);
      console.log(`${prefix} ${line}`);

      if (label === "BACKEND") {
        if (shouldMirrorBackendLineToFile(line)) {
          writeFileLog(label, line);
        }
      } else {
        writeFileLog(label, line);
      }
    }
  });

  proc.on("close", (code) => {
    const message =
      code !== 0 && code !== null
        ? `${label} exited with code ${code}`
        : `${label} stopped`;
    if (code !== 0 && code !== null) {
      console.error(`${tag(label, C.red)} ${message}`);
    } else {
      console.log(`${tag(label, color)} ${message}`);
    }
    writeFileLog("APP", message);
  });

  proc.on("error", (err) => {
    const message = `${label} start error: ${err.message}`;
    console.error(`${tag(label, C.red)} ${message}`);
    writeFileLog("APP", message);
  });

  return proc;
}

function shutdown(signal, children) {
  const message = `Received ${signal}, shutting down child processes`;
  console.log(`${C.yellow}[APP] ${message}${C.reset}`);
  writeFileLog("APP", message);

  for (const p of children) {
    if (!p || p.killed) continue;
    if (process.platform === "win32") {
      spawn("taskkill", ["/pid", String(p.pid), "/f", "/t"], { shell: true });
    } else {
      p.kill("SIGTERM");
    }
  }

  setTimeout(() => {
    writeFileLog("APP", "Launcher stopped");
    logStream.end(() => process.exit(0));
  }, 1200);
}

process.on("warning", (warning) => {
  const message = `${warning.name}: ${warning.message}`;
  console.warn(message);
  writeFileLog("APP", message);
});

process.on("uncaughtException", (err) => {
  const message = `uncaughtException: ${err?.stack || err?.message || String(err)}`;
  console.error(message);
  writeFileLog("APP", message);
});

process.on("unhandledRejection", (reason) => {
  const message = `unhandledRejection: ${String(reason)}`;
  console.error(message);
  writeFileLog("APP", message);
});

function main() {
  console.log(`\n${C.cyan}╔══════════════════════════════════════╗`);
  console.log(`║    TenderSmart — запуск приложения   ║`);
  console.log(`╚══════════════════════════════════════╝${C.reset}\n`);

  console.log(`${C.yellow}Python:${C.reset} ${PYTHON_CMD}`);
  console.log(`${C.yellow}Рабочая папка:${C.reset} ${__dirname}\n`);

  writeFileLog("APP", "TenderSmart launcher started");
  writeFileLog("APP", `Python: ${PYTHON_CMD}`);
  writeFileLog("APP", `Working directory: ${__dirname}`);

  const backend = startProcess("BACKEND", C.blue, PYTHON_CMD, BACKEND_CMD);
  const frontend = startProcess("FRONTEND", C.green, "npm", FRONTEND_CMD);

  process.on("SIGINT", () => shutdown("SIGINT", [backend, frontend]));
  process.on("SIGTERM", () => shutdown("SIGTERM", [backend, frontend]));
}

main();
