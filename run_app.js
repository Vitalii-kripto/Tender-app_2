import { spawn } from 'child_process';
import fs from 'fs';
import path from 'path';

console.log('🚀 run_app.js is starting!');

let pythonCmd = '';
let pythonDisplay = '';

if (process.platform === 'win32') {
  const venvPythonPath = path.join(process.cwd(), '.venv', 'Scripts', 'python.exe');
  if (!fs.existsSync(venvPythonPath)) {
    console.error('Не найден Python виртуального окружения .venv. Сначала создайте/установите зависимости в виртуальное окружение.');
    process.exit(1);
  }
  // User explicitly requested this exact string
  pythonCmd = '.\\\\.venv\\\\Scripts\\\\python.exe';
  pythonDisplay = '.\\\\.venv\\\\Scripts\\\\python.exe';
} else {
  const linuxVenvPath = path.join(process.cwd(), '.venv', 'bin', 'python');
  if (fs.existsSync(linuxVenvPath)) {
    pythonCmd = './.venv/bin/python';
    pythonDisplay = './.venv/bin/python';
  } else {
    pythonCmd = 'python3';
    pythonDisplay = 'python3';
  }
}

const npmCmd = process.platform === 'win32' ? 'npm.cmd' : 'npm';
console.log(`🚀 Starting Frontend (${npmCmd} run dev:frontend)...`);

const frontend = spawn(npmCmd, ['run', 'dev:frontend'], {
  stdio: 'inherit',
  shell: true
});

console.log(`🚀 Starting Backend (${pythonDisplay} run_backend.py)...`);

const backend = spawn(pythonCmd, ['run_backend.py'], {
  stdio: 'inherit',
  shell: true
});

backend.on('error', (err) => {
  console.error(`Failed to start backend process: ${err.message}`);
});

process.on('SIGINT', () => {
  frontend.kill('SIGINT');
  backend.kill('SIGINT');
  process.exit();
});
