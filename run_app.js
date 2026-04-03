// run_app.js  —  ESM version (совместим с "type": "module" в package.json)
// Запускает Python-бэкенд (uvicorn) и Vite-фронтенд параллельно.

import { spawn } from "child_process";
import { existsSync, mkdirSync, createWriteStream } from "fs";
import { resolve, join } from "path";
import { fileURLToPath } from "url";

// ── Пути ──────────────────────────────────────────────────────────────────────
const __filename = fileURLToPath(import.meta.url);
const __dirname  = resolve(__filename, "..");

const LOGS_DIR = join(__dirname, "logs");
if (!existsSync(LOGS_DIR)) {
    mkdirSync(LOGS_DIR, { recursive: true });
}
const LOG_FILE = join(LOGS_DIR, "tendersmart.log");
const logStream = createWriteStream(LOG_FILE, { flags: "w", encoding: "utf8" });

function writeLog(label, level, data, color) {
    const lines = data.toString().trimEnd().split("\n");
    const now = new Date().toISOString().replace('T', ' ').substring(0, 19);
    for (const line of lines) {
        if (line.trim()) {
            const msg = `${now} | ${level} | RUN_APP | ${label} | ${line.trim()}\n`;
            logStream.write(msg);
            
            const prefix = color ? tag(label, color) : `[${label}]`;
            if (level === 'ERROR') {
                console.error(`${prefix} ${line.trim()}`);
            } else {
                console.log(`${prefix} ${line.trim()}`);
            }
        }
    }
}

// ── Настройки ─────────────────────────────────────────────────────────────────
const VENV_PYTHON_WIN  = join(__dirname, ".venv", "Scripts", "python.exe");
const VENV_PYTHON_UNIX = join(__dirname, ".venv", "bin",     "python");
const PYTHON_CMD       = existsSync(VENV_PYTHON_WIN)  ? VENV_PYTHON_WIN
                       : existsSync(VENV_PYTHON_UNIX) ? VENV_PYTHON_UNIX
                       : "python";                        // системный python — запасной вариант

const BACKEND_CMD  = ["-m", "uvicorn", "backend.main:app", "--reload", "--port", "8000"];
const FRONTEND_CMD = ["run", "dev:frontend"];             // vite

// ── Цвета для консоли ─────────────────────────────────────────────────────────
const C = {
  reset:   "\x1b[0m",
  blue:    "\x1b[34m",
  green:   "\x1b[32m",
  red:     "\x1b[31m",
  yellow:  "\x1b[33m",
  cyan:    "\x1b[36m",
};

function tag(name, color) {
  return `${color}[${name}]${C.reset}`;
}

// ── Запуск дочернего процесса ─────────────────────────────────────────────────
function startProcess(label, color, cmd, args, cwd = __dirname) {
  writeLog(label, "INFO", `Запуск: ${cmd} ${args.join(" ")}`, color);

  const proc = spawn(cmd, args, {
    cwd,
    shell: process.platform === "win32",   // на Windows нужен shell для npm
    stdio: "pipe",
    env: { ...process.env },
  });

  proc.stdout.on("data", (data) => {
    writeLog(label, "INFO", data, color);
  });

  proc.stderr.on("data", (data) => {
    // uvicorn пишет INFO в stderr — не считаем это ошибкой
    const isInfo = /INFO|WARNING|DEBUG/.test(data.toString());
    writeLog(label, isInfo ? "INFO" : "ERROR", data, isInfo ? color : C.red);
  });

  proc.on("close", (code) => {
    if (code !== 0 && code !== null) {
      writeLog(label, "ERROR", `Процесс завершился с кодом ${code}`, C.red);
    } else {
      writeLog(label, "INFO", `Процесс остановлен.`, color);
    }
  });

  proc.on("error", (err) => {
    writeLog(label, "ERROR", `Ошибка запуска: ${err.message}`, C.red);
    if (err.code === "ENOENT") {
      writeLog(label, "ERROR", `Команда не найдена: "${cmd}". Убедитесь, что venv создан и активирован, или Python доступен в PATH.`, C.red);
    }
  });

  return proc;
}

// ── Главная функция ───────────────────────────────────────────────────────────
function main() {
  const header = `\n╔══════════════════════════════════════╗\n║    TenderSmart — запуск приложения   ║\n╚══════════════════════════════════════╝\n\nPython:   ${PYTHON_CMD}\nРабочая папка: ${__dirname}\n`;
  writeLog("APP", "INFO", header, C.cyan);

  const backend  = startProcess("BACKEND",  C.blue,  PYTHON_CMD, BACKEND_CMD);
  const frontend = startProcess("FRONTEND", C.green, "npm",       FRONTEND_CMD);

  // ── Graceful shutdown: Ctrl+C останавливает оба процесса ──────────────────
  function shutdown(signal) {
    writeLog("APP", "INFO", `Получен ${signal}, останавливаем процессы...`, C.yellow);
    [backend, frontend].forEach((p) => {
      if (!p.killed) {
        // На Windows нужно SIGTERM через taskkill, иначе дочерние не гасятся
        if (process.platform === "win32") {
          spawn("taskkill", ["/pid", p.pid.toString(), "/f", "/t"], { shell: true });
        } else {
          p.kill("SIGTERM");
        }
      }
    });
    setTimeout(() => {
        logStream.end();
        process.exit(0);
    }, 2000);
  }

  process.on("SIGINT",  () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main();
