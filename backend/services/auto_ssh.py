# backend/services/auto_ssh.py
from __future__ import annotations

import atexit
import socket
import subprocess
import time
from dataclasses import dataclass
from typing import Optional, Dict, Any, Tuple
from urllib.parse import urlparse

import requests


@dataclass(frozen=True)
class RfProxyTunnelConfig:
    """
    Конфигурация SSH->SOCKS5 туннеля до офисного ПК (через Tailscale) + правила доменов.
    """
    ssh_host: str                    # например: "100.75.209.12" (офисный ПК в Tailscale)
    ssh_user: str                    # например: "vitt"
    local_socks_host: str = "127.0.0.1"
    local_socks_port: int = 1080
    key_path: Optional[str] = None   # r"C:\Users\vitt7\.ssh\id_ed25519" (если нужно)

    connect_timeout_sec: int = 10
    keepalive_interval_sec: int = 15
    keepalive_count_max: int = 3

    # Разрешённые домены (и все их поддомены). Добавляй сюда по мере необходимости.
    # Пример: ("zakupki.gov.ru", "minfin.gov.ru")
    allowed_domains: Tuple[str, ...] = ("zakupki.gov.ru",)

    # "Разогрев" (можно заменить на другой URL из allowed_domains)
    warmup_url: str = "https://zakupki.gov.ru/epz/main/public/home.html"

    # Заголовки
    user_agent: str = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/121.0 Safari/537.36"
    )
    accept_language: str = "ru-RU,ru;q=0.9"


class SshSocksTunnel:
    """
    Поднимает локальный SOCKS5 через SSH:
      ssh -N -D 127.0.0.1:PORT user@host

    Умеет:
    - start(): поднять (если не поднят)
    - ensure(): гарантировать, что поднят
    - close(): остановить
    """

    def __init__(self, cfg: RfProxyTunnelConfig):
        self.cfg = cfg
        self._proc: Optional[subprocess.Popen] = None
        self._closed = False
        atexit.register(self.close)

    @staticmethod
    def _is_port_open(host: str, port: int, timeout: float = 0.4) -> bool:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.settimeout(timeout)
            try:
                s.connect((host, port))
                return True
            except OSError:
                return False

    def _build_cmd(self) -> list[str]:
        c = self.cfg
        cmd = [
            "ssh",
            "-N",
            "-o", "StrictHostKeyChecking=no",
            "-o", "UserKnownHostsFile=/dev/null",
            "-o", "ExitOnForwardFailure=yes",
            "-o", f"ConnectTimeout={c.connect_timeout_sec}",
            "-o", f"ServerAliveInterval={c.keepalive_interval_sec}",
            "-o", f"ServerAliveCountMax={c.keepalive_count_max}",
            "-D", f"{c.local_socks_host}:{c.local_socks_port}",
        ]
        if c.key_path:
            cmd += ["-i", c.key_path]
        cmd += [f"{c.ssh_user}@{c.ssh_host}"]
        return cmd

    def start(self) -> None:
        if self._closed:
            raise RuntimeError("Tunnel is closed; create a new instance.")

        # Уже поднят?
        if self._is_port_open(self.cfg.local_socks_host, self.cfg.local_socks_port):
            return

        # Если прошлый процесс умер — забываем его
        if self._proc and self._proc.poll() is not None:
            self._proc = None

        cmd = self._build_cmd()

        # Важно: если ключ с passphrase, ssh запросит ввод и повиснет.
        # Для автозапуска используй ключ без passphrase или заранее работающий ssh-agent.
        self._proc = subprocess.Popen(
            cmd,
            stdin=subprocess.DEVNULL,
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL,
            creationflags=getattr(subprocess, "CREATE_NEW_PROCESS_GROUP", 0),
        )

        # Ждём, пока SOCKS порт начнёт слушаться
        deadline = time.time() + 8
        while time.time() < deadline:
            if self._is_port_open(self.cfg.local_socks_host, self.cfg.local_socks_port):
                return
            if self._proc and self._proc.poll() is not None:
                break
            time.sleep(0.2)

        raise RuntimeError(
            f"Не удалось поднять SOCKS5 {self.cfg.local_socks_host}:{self.cfg.local_socks_port}. "
            f"Проверь SSH доступность {self.cfg.ssh_user}@{self.cfg.ssh_host} и ключ (желательно без passphrase)."
        )

    def ensure(self) -> None:
        if self._closed:
            raise RuntimeError("Tunnel is closed; create a new instance.")

        if not self._is_port_open(self.cfg.local_socks_host, self.cfg.local_socks_port):
            if self._proc and self._proc.poll() is not None:
                self._proc = None
            self.start()

    def close(self) -> None:
        if self._closed:
            return
        self._closed = True

        proc = self._proc
        self._proc = None
        if proc and proc.poll() is None:
            try:
                proc.terminate()
            except Exception:
                pass

    def __enter__(self) -> "SshSocksTunnel":
        self.start()
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


class RfProxyHttpClient:
    """
    Один клиент для запросов к любым доменам из allow-list через SOCKS5 туннель.

    - Автоподнимает туннель (ensure)
    - Ограничивает запросы списком allowed_domains (и поддомены)
    - Имеет warmup() для первичного прогрева cookies/редиректов
    """

    def __init__(self, cfg: RfProxyTunnelConfig):
        self.cfg = cfg
        self.tunnel = SshSocksTunnel(cfg)

        self.session = requests.Session()
        self.session.proxies.update(self._build_proxies())
        self.session.headers.update(self._build_headers())

        self._warmed_up = False

    def _build_proxies(self) -> Dict[str, str]:
        h = self.cfg.local_socks_host
        p = self.cfg.local_socks_port
        # socks5h -> DNS резолвится через прокси (важно)
        return {
            "http": f"socks5h://{h}:{p}",
            "https": f"socks5h://{h}:{p}",
        }

    def _build_headers(self) -> Dict[str, str]:
        return {
            "User-Agent": self.cfg.user_agent,
            "Accept-Language": self.cfg.accept_language,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            "Connection": "keep-alive",
        }

    def _is_allowed_host(self, host: str) -> bool:
        host = host.lower().strip(".")
        for domain in self.cfg.allowed_domains:
            d = domain.lower().strip(".")
            if host == d or host.endswith("." + d):
                return True
        return False

    def _assert_allowed_url(self, url: str) -> None:
        host = (urlparse(url).hostname or "").lower()
        if not host or not self._is_allowed_host(host):
            raise ValueError(
                f"Запрещено ходить вне allow-list. host={host!r}, url={url!r}. "
                f"Разрешено: {self.cfg.allowed_domains}"
            )

    def warmup(self) -> None:
        """
        Один раз прогревает сессию на warmup_url (должен быть из allowed_domains).
        Полезно для сайтов с редиректами/куками/защитами.
        """
        self._assert_allowed_url(self.cfg.warmup_url)
        self.tunnel.ensure()

        r = self.session.get(self.cfg.warmup_url, timeout=30, allow_redirects=True)
        r.raise_for_status()
        self._warmed_up = True

    def request(self, method: str, url: str, **kwargs: Any) -> requests.Response:
        """
        Универсальный запрос через SOCKS и allow-list.
        kwargs прокидываются в requests: params, data, json, headers, timeout, allow_redirects и т.д.
        """
        self._assert_allowed_url(url)
        self.tunnel.ensure()

        if not self._warmed_up:
            # если warmup_url не подходит для текущего домена, можно вызвать warmup() вручную
            # или оставить как есть — он прогреется через warmup_url.
            self.warmup()

        kwargs.setdefault("timeout", 30)
        kwargs.setdefault("allow_redirects", True)

        return self.session.request(method=method.upper(), url=url, **kwargs)

    def get(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("GET", url, **kwargs)

    def post(self, url: str, **kwargs: Any) -> requests.Response:
        return self.request("POST", url, **kwargs)

    def close(self) -> None:
        try:
            self.session.close()
        finally:
            self.tunnel.close()

    def __enter__(self) -> "RfProxyHttpClient":
        return self

    def __exit__(self, exc_type, exc, tb) -> None:
        self.close()


# ---- Пример локального запуска ----
if __name__ == "__main__":
    cfg = RfProxyTunnelConfig(
        ssh_host="100.75.209.12",
        ssh_user="vitt",
        local_socks_port=1080,
        allowed_domains=("zakupki.gov.ru",),
        warmup_url="https://zakupki.gov.ru/epz/main/public/home.html",
        # key_path=r"C:\Users\vitt7\.ssh\id_ed25519",  # если нужно
    )

    with RfProxyHttpClient(cfg) as client:
        r = client.get("https://zakupki.gov.ru/epz/main/public/home.html")
        print("status:", r.status_code, "len:", len(r.text), "final:", r.url)
