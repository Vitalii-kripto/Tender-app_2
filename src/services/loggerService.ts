import { API_BASE_URL } from "./geminiService";

export type LogLevel = "info" | "warning" | "error";

const serializeLogValue = (value: any, seen = new WeakSet<object>()): any => {
    if (value instanceof Error) {
        return {
            name: value.name,
            message: value.message,
            stack: value.stack,
        };
    }

    if (Array.isArray(value)) {
        return value.map((item) => serializeLogValue(item, seen));
    }

    if (value && typeof value === "object") {
        if (seen.has(value)) {
            return "[Circular]";
        }
        seen.add(value);

        return Object.fromEntries(
            Object.entries(value).map(([key, nestedValue]) => [
                key,
                serializeLogValue(nestedValue, seen),
            ])
        );
    }

    return value;
};

export const logToBackend = async (level: LogLevel, message: string, context: any = {}) => {
    const normalizedContext = serializeLogValue(context);

    // Also log to console for development
    const consoleMethod = level === "error" ? "error" : level === "warning" ? "warn" : "log";
    console[consoleMethod](`[Frontend ${level.toUpperCase()}] ${message}`, normalizedContext);

    try {
        await fetch(`${API_BASE_URL}/api/frontend-log`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ level, message, context: normalizedContext })
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
