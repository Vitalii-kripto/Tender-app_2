import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';
import readline from 'readline';

const ROOT_DIR = process.cwd();
const LOG_DIR = path.join(ROOT_DIR, 'logs');
const LOG_FILE = path.join(LOG_DIR, 'tendersmart.log');

fs.mkdirSync(LOG_DIR, { recursive: true });

const logStream = fs.createWriteStream(LOG_FILE, { flags: 'a', encoding: 'utf8' });

function ts() {
  const d = new Date();
  const pad = (n) => String(n).padStart(2, '0');
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())} ${pad(d.getHours())}:${pad(d.getMinutes())}:${pad(d.getSeconds())}`;
}

function stripAnsi(input) {
  return String(input).replace(/\x1B\[[0-9;]*[A-Za-z]/g, '');
}

function writeLog(scope, line) {
  const text = `[${scope}] ${stripAnsi(line)}`;
  const finalLine = `${ts()} | ${text}`;
  console.log(finalLine);
  logStream.write(finalLine + '\n');
}

function pipeProcessOutput(child, scope) {
  if (child.stdout) {
    const rlOut = readline.createInterface({ input: child.stdout });
    rlOut.on('line', (line) => writeLog(scope, line));
  }

  if (child.stderr) {
    const rlErr = readline.createInterface({ input: child.stderr });
    rlErr.on('line', (line) => writeLog(scope, line));
  }

  child.on('error', (err) => {
    writeLog(scope, `PROCESS_ERROR: ${err.message}`);
  });

  child.on('close', (code, signal) => {
    writeLog(scope, `PROCESS_EXIT: code=${code ?? 'null'} signal=${signal ?? 'null'}`);
  });
}

let pythonCmd = '';
let pythonArgs = [];
let npmCmd = process.platform === 'win32' ? 'npm.cmd' : 'npm';
let npmArgs = ['run', 'dev:frontend'];

if (process.platform === 'win32') {
  const venvPythonPath = path.join(ROOT_DIR, '.venv', 'Scripts', 'python.exe');
  if (!fs.existsSync(venvPythonPath)) {
    writeLog('APP', 'Не найден Python виртуального окружения .venv');
    process.exit(1);
  }
  pythonCmd = venvPythonPath;
  pythonArgs = ['run_backend.py'];
} else {
  const linuxVenvPath = path.join(ROOT_DIR, '.venv', 'bin', 'python');
  pythonCmd = fs.existsSync(linuxVenvPath) ? linuxVenvPath : 'python3';
  pythonArgs = ['run_backend.py'];
}

writeLog('APP', '╔══════════════════════════════════════╗');
writeLog('APP', '║    TenderSmart — запуск приложения   ║');
writeLog('APP', '╚══════════════════════════════════════╝');
writeLog('APP', `Python:   ${pythonCmd}`);
writeLog('APP', `Рабочая папка: ${ROOT_DIR}`);
writeLog('BACKEND', `Запуск: ${pythonCmd} ${pythonArgs.join(' ')}`);
writeLog('FRONTEND', `Запуск: ${npmCmd} ${npmArgs.join(' ')}`);

const frontend = spawn(npmCmd, npmArgs, {
  cwd: ROOT_DIR,
  stdio: ['ignore', 'pipe', 'pipe'],
  shell: false,
  windowsHide: true
});

const backend = spawn(pythonCmd, pythonArgs, {
  cwd: ROOT_DIR,
  stdio: ['ignore', 'pipe', 'pipe'],
  shell: false,
  windowsHide: true
});

pipeProcessOutput(frontend, 'FRONTEND');
pipeProcessOutput(backend, 'BACKEND');

function shutdown() {
  writeLog('APP', 'Получен SIGINT, останавливаем процессы...');
  try { frontend.kill('SIGINT'); } catch {}
  try { backend.kill('SIGINT'); } catch {}

  setTimeout(() => {
    try { frontend.kill('SIGKILL'); } catch {}
    try { backend.kill('SIGKILL'); } catch {}
    try { logStream.end(); } catch {}
    process.exit(0);
  }, 3000);
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
