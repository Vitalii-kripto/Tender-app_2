const fs = require("fs");
const path = require("path");
const { spawn } = require("child_process");

const isWin = process.platform === "win32";
const npmCmd = isWin ? "npm.cmd" : "npm";
const pythonCmd = isWin ? ".\\.venv\\Scripts\\python.exe" : "./.venv/bin/python";

const LOG_DIR = path.join(__dirname, "logs");
const LOG_FILE = path.join(LOG_DIR, "app.log");

fs.mkdirSync(LOG_DIR, { recursive: true });

function writeLog(prefix, message) {
  const line = `${new Date().toISOString()} | ${prefix} | ${message}\n`;
  fs.appendFileSync(LOG_FILE, line, "utf8");
}

function pipeProcessOutput(child, prefix) {
  child.stdout.on("data", (data) => {
    const text = data.toString();
    process.stdout.write(text);
    writeLog(`${prefix}:STDOUT`, text.trimEnd());
  });

  child.stderr.on("data", (data) => {
    const text = data.toString();
    process.stderr.write(text);
    writeLog(`${prefix}:STDERR`, text.trimEnd());
  });

  child.on("exit", (code) => {
    writeLog(prefix, `process exited with code ${code}`);
  });

  child.on("error", (err) => {
    writeLog(prefix, `process error: ${err.stack || err.message}`);
  });
}

console.log("🚀 run_app.js is starting!");
writeLog("APP", "run_app.js is starting");

console.log("🚀 Starting Frontend (npm run dev:frontend)...");
writeLog("APP", "Starting frontend");
const frontend = spawn(npmCmd, ["run", "dev:frontend"], {
  cwd: __dirname,
  shell: false,
  env: process.env,
  stdio: ["ignore", "pipe", "pipe"],
});
pipeProcessOutput(frontend, "FRONTEND");

console.log("🚀 Starting Backend (.venv python run_backend.py)...");
writeLog("APP", "Starting backend");
const backend = spawn(pythonCmd, ["run_backend.py"], {
  cwd: __dirname,
  shell: false,
  env: process.env,
  stdio: ["ignore", "pipe", "pipe"],
});
pipeProcessOutput(backend, "BACKEND");

function shutdown() {
  writeLog("APP", "Shutdown requested");
  if (frontend && !frontend.killed) frontend.kill();
  if (backend && !backend.killed) backend.kill();
  process.exit(0);
}

process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
