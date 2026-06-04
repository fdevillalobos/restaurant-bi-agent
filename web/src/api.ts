import type { ChatMessage, StoredChatMessage } from "./types";

let csrfToken: string | null = null;

export function setCsrfToken(token?: string | null) {
  csrfToken = token || null;
}

export const api = async <T,>(path: string, init?: RequestInit): Promise<T> => {
  const method = (init?.method || "GET").toUpperCase();
  const headers = {
    "Content-Type": "application/json",
    ...(csrfToken && method !== "GET" ? { "X-CSRF-Token": csrfToken } : {}),
    ...(init?.headers || {})
  };
  const res = await fetch(path, {
    credentials: "include",
    headers,
    ...init
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(data.detail || res.statusText);
  }
  const data = await res.json();
  if (data && typeof data === "object" && "csrf_token" in data) {
    setCsrfToken((data as { csrf_token?: string | null }).csrf_token);
  }
  return data;
};

export function toChatMessages(stored: StoredChatMessage[]): ChatMessage[] {
  return stored.map((message) => ({
    id: `stored-${message.id}`,
    role: message.role,
    content: message.content,
    response: message.payload || undefined,
    created_at: message.created_at
  }));
}
