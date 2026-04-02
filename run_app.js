// run_app.js  —  ESM version (совместим с "type": "module" в package.json)
// Запускает Python-бэкенд (uvicorn) и Vite-фронтенд параллельно.

import { spawn } from "child_process";
import { existsSync } from "fs";
import { resolve, join } from "path";
import { fileURLToPath } from "url";

// ── Пути ──────────────────────────────────────────────────────────────────────
const __filename = fileURLToPath(import.meta.url);
const __dirname  = resolve(__filename, "..");

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
  console.log(`${tag(label, color)} Запуск: ${cmd} ${args.join(" ")}`);

  const proc = spawn(cmd, args, {
    cwd,
    shell: process.platform === "win32",   // на Windows нужен shell для npm
    stdio: "pipe",
    env: { ...process.env },
  });

  proc.stdout.on("data", (data) => {
    const lines = data.toString().trimEnd().split("\n");
    lines.forEach((line) => console.log(`${tag(label, color)} ${line}`));
  });

  proc.stderr.on("data", (data) => {
    const lines = data.toString().trimEnd().split("\n");
    lines.forEach((line) => {
      // uvicorn пишет INFO в stderr — не считаем это ошибкой
      const isInfo = /INFO|WARNING|DEBUG/.test(line);
      const prefix = isInfo ? tag(label, color) : tag(label, C.red);
      console.log(`${prefix} ${line}`);
    });
  });

  proc.on("close", (code) => {
    if (code !== 0 && code !== null) {
      console.error(`${tag(label, C.red)} Процесс завершился с кодом ${code}`);
    } else {
      console.log(`${tag(label, color)} Процесс остановлен.`);
    }
  });

  proc.on("error", (err) => {
    console.error(`${tag(label, C.red)} Ошибка запуска: ${err.message}`);
    if (err.code === "ENOENT") {
      console.error(
        `${tag(label, C.red)} Команда не найдена: "${cmd}". ` +
        `Убедитесь, что venv создан и активирован, или Python доступен в PATH.`
      );
    }
  });

  return proc;
}

// ── Главная функция ───────────────────────────────────────────────────────────
function main() {
  console.log(`\n${C.cyan}╔══════════════════════════════════════╗`);
  console.log(`║    TenderSmart — запуск приложения   ║`);
  console.log(`╚══════════════════════════════════════╝${C.reset}\n`);
  console.log(`${C.yellow}Python:${C.reset}   ${PYTHON_CMD}`);
  console.log(`${C.yellow}Рабочая папка:${C.reset} ${__dirname}\n`);

  const backend  = startProcess("BACKEND",  C.blue,  PYTHON_CMD, BACKEND_CMD);
  const frontend = startProcess("FRONTEND", C.green, "npm",       FRONTEND_CMD);

  // ── Graceful shutdown: Ctrl+C останавливает оба процесса ──────────────────
  function shutdown(signal) {
    console.log(`\n${C.yellow}[APP] Получен ${signal}, останавливаем процессы...${C.reset}`);
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
    setTimeout(() => process.exit(0), 2000);
  }

  process.on("SIGINT",  () => shutdown("SIGINT"));
  process.on("SIGTERM", () => shutdown("SIGTERM"));
}

main();
