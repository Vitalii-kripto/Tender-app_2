// run_app.js
import { spawn } from "child_process";
import { appendFileSync, existsSync, mkdirSync, renameSync, rmSync, statSync } from "fs";
import { resolve, join } from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = resolve(__filename, "..");

const LOG_DIR = join(__dirname, "logs");
const LOG_FILE = join(LOG_DIR, "tendersmart.txt");
const LOG_MAX_BYTES = 20 * 1024 * 1024;
const LOG_BACKUP_COUNT = 5;
mkdirSync(LOG_DIR, { recursive: true });
let consoleBroken = false;
let logFileBroken = false;

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
const FRONTEND_LAUNCH = isWin
  ? { cmd: "cmd.exe", args: ["/d", "/s", "/c", "npm run dev:frontend"] }
  : { cmd: "npm", args: FRONTEND_CMD };

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

function stripAnsi(text) {
  return String(text ?? "").replace(/\x1b\[[0-9;]*m/g, "");
}

function resetCurrentLogFile() {
  try {
    rmSync(LOG_FILE, { force: true });
  } catch (err) {
    logFileBroken = true;
  }
}

function rotateCombinedLog() {
  try {
    rmSync(`${LOG_FILE}.${LOG_BACKUP_COUNT}`, { force: true });
    for (let index = LOG_BACKUP_COUNT - 1; index >= 1; index -= 1) {
      const source = `${LOG_FILE}.${index}`;
      const target = `${LOG_FILE}.${index + 1}`;
      if (existsSync(source)) {
        renameSync(source, target);
      }
    }
    if (existsSync(LOG_FILE)) {
      renameSync(LOG_FILE, `${LOG_FILE}.1`);
    }
  } catch (err) {
    logFileBroken = true;
  }
}

function ensureLogCapacity(nextEntryBytes) {
  if (logFileBroken || !existsSync(LOG_FILE)) {
    return;
  }
  try {
    const currentSize = statSync(LOG_FILE).size;
    if (currentSize + nextEntryBytes > LOG_MAX_BYTES) {
      rotateCombinedLog();
    }
  } catch (err) {
    logFileBroken = true;
  }
}

function writeFileLog(source, line) {
  if (logFileBroken) {
    return;
  }
  const text = String(line ?? "").replace(/\r?\n$/, "");
  if (!text.trim()) return;
  try {
    const record = `${ts()} | ${source} | ${text}\n`;
    ensureLogCapacity(Buffer.byteLength(record, "utf8"));
    appendFileSync(LOG_FILE, record, { encoding: "utf8" });
  } catch (err) {
    logFileBroken = true;
  }
}

function safeConsole(method, message) {
  if (consoleBroken) {
    return;
  }
  try {
    console[method](message);
  } catch (err) {
    if (err?.code === "EPIPE") {
      consoleBroken = true;
      writeFileLog("APP", `console_${method}_epipe`);
      return;
    }
    writeFileLog("APP", `console_${method}_error: ${err?.message || String(err)}`);
  }
}

function shouldMirrorBackendLineToFile(line) {
  const text = stripAnsi(line).trim();
  if (!text) {
    return false;
  }
  if (/\|\s*DEBUG\s*\|/.test(text)) {
    return false;
  }
  if (/\|\s*(INFO|WARNING)\s*\|\s*(sqlalchemy|sqlalchemy\.[^|]*|urllib3\.connectionpool|PIL\.Image|asyncio|watchfiles\.[^|]*)\s*\|/.test(text)) {
    return false;
  }
  if (/^(SELECT|FROM|WHERE|LIMIT|ORDER BY|GROUP BY|JOIN|LEFT JOIN|RIGHT JOIN|INNER JOIN|OUTER JOIN|UNION|VALUES|Col \(|Row \(|\[generated in|\[cached since)/i.test(text)) {
    return false;
  }
  return true;
}

function shouldPrintToConsole(label, line) {
  const text = stripAnsi(line);
  if (label === "BACKEND" && /\|\s*DEBUG\s*\|/.test(text)) {
    return false;
  }
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
    if (shouldPrintToConsole(label, line)) {
      safeConsole("log", `${prefix} ${line}`);
    }

    if (writeToFile) {
      if (backendMirror) {
        if (shouldMirrorBackendLineToFile(line)) {
          writeFileLog(label, stripAnsi(line));
        }
      } else {
        writeFileLog(label, stripAnsi(line));
      }
    }
  }
}

function startProcess(label, color, cmd, args, cwd = __dirname, extraEnv = {}) {
  safeConsole("log", `${tag(label, color)} Запуск: ${cmd} ${args.join(" ")}`);
  writeFileLog("APP", `${label} start: ${cmd} ${args.join(" ")}`);

  const proc = spawn(cmd, args, {
    cwd,
    shell: false,
    stdio: "pipe",
    env: { ...process.env, ...extraEnv },
  });

  proc.stdout.on("data", (data) => {
    writeConsoleLines(label, color, data, {
      writeToFile: true,
      backendMirror: label === "BACKEND",
      isError: false,
    });
  });

  proc.stderr.on("data", (data) => {
    const lines = data.toString("utf8").split(/\r?\n/).filter(Boolean);
    for (const line of lines) {
      const isInfoLike = /INFO|WARNING|DEBUG|VITE|ready in|Local:|Network:|DeprecationWarning/.test(line);
      const prefix = isInfoLike ? tag(label, color) : tag(label, C.red);
      if (shouldPrintToConsole(label, line)) {
        safeConsole(isInfoLike ? "log" : "error", `${prefix} ${line}`);
      }

      if (label === "BACKEND") {
        if (shouldMirrorBackendLineToFile(line)) {
          writeFileLog(label, stripAnsi(line));
        }
      } else {
        writeFileLog(label, stripAnsi(line));
      }
    }
  });

  proc.on("close", (code) => {
    const message =
      code !== 0 && code !== null
        ? `${label} exited with code ${code}`
        : `${label} stopped`;
    if (code !== 0 && code !== null) {
      safeConsole("error", `${tag(label, C.red)} ${message}`);
    } else {
      safeConsole("log", `${tag(label, color)} ${message}`);
    }
    writeFileLog("APP", message);
  });

  proc.on("error", (err) => {
    const message = `${label} start error: ${err.message}`;
    safeConsole("error", `${tag(label, C.red)} ${message}`);
    writeFileLog("APP", message);
  });

  return proc;
}

function shutdown(signal, children) {
  const message = `Received ${signal}, shutting down child processes`;
  safeConsole("log", `${C.yellow}[APP] ${message}${C.reset}`);
  writeFileLog("APP", message);

  for (const p of children) {
    if (!p || p.killed) continue;
    if (process.platform === "win32") {
      spawn("taskkill", ["/pid", String(p.pid), "/f", "/t"], { shell: false });
    } else {
      p.kill("SIGTERM");
    }
  }

  setTimeout(() => {
    writeFileLog("APP", "Launcher stopped");
    process.exit(0);
  }, 1200);
}

process.on("warning", (warning) => {
  const message = `${warning.name}: ${warning.message}`;
  safeConsole("warn", message);
  writeFileLog("APP", message);
});

process.on("uncaughtException", (err) => {
  const message = `uncaughtException: ${err?.stack || err?.message || String(err)}`;
  writeFileLog("APP", message);
  if (err?.code !== "EPIPE") {
    safeConsole("error", message);
  } else {
    consoleBroken = true;
  }
});

process.on("unhandledRejection", (reason) => {
  const message = `unhandledRejection: ${String(reason)}`;
  writeFileLog("APP", message);
  safeConsole("error", message);
});

function main() {
  resetCurrentLogFile();

  safeConsole("log", `\n${C.cyan}╔══════════════════════════════════════╗`);
  safeConsole("log", `║    TenderSmart — запуск приложения   ║`);
  safeConsole("log", `╚══════════════════════════════════════╝${C.reset}\n`);

  safeConsole("log", `${C.yellow}Python:${C.reset} ${PYTHON_CMD}`);
  safeConsole("log", `${C.yellow}Рабочая папка:${C.reset} ${__dirname}\n`);

  writeFileLog("APP", "TenderSmart launcher started");
  writeFileLog("APP", `Python: ${PYTHON_CMD}`);
  writeFileLog("APP", `Working directory: ${__dirname}`);

  const backend = startProcess(
    "BACKEND",
    C.blue,
    PYTHON_CMD,
    BACKEND_CMD,
    __dirname,
    {
      TENDERSMART_LOG_OWNER: "launcher",
      TENDERSMART_STDOUT_LOG_LEVEL: "INFO",
      TENDERSMART_ROOT_LOG_LEVEL: "INFO",
    }
  );
  const frontend = startProcess(
    "FRONTEND",
    C.green,
    FRONTEND_LAUNCH.cmd,
    FRONTEND_LAUNCH.args
  );

  process.on("SIGINT", () => shutdown("SIGINT", [backend, frontend]));
  process.on("SIGTERM", () => shutdown("SIGTERM", [backend, frontend]));
}

main();
