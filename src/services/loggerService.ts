import { API_BASE_URL } from "./geminiService";

export type LogLevel = "info" | "warning" | "error";

export const logToBackend = async (level: LogLevel, message: string, context: any = {}) => {
    // Also log to console for development
    const consoleMethod = level === "error" ? "error" : level === "warning" ? "warn" : "log";
    console[consoleMethod](`[Frontend ${level.toUpperCase()}] ${message}`, context);

    try {
        await fetch(`${API_BASE_URL}/api/frontend-log`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ level, message, context })
        });
    } catch (e) {
        // Silent fail for logging to avoid infinite loops or breaking the app
        console.error("Failed to send log to backend", e);
    }
};

export const logger = {
    info: (message: string, context?: any) => logToBackend("info", message, context),
    warn: (message: string, context?: any) => logToBackend("warning", message, context),
    error: (message: string, context?: any) => logToBackend("error", message, context),
};
