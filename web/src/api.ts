import type { ChatMessage, StoredChatMessage } from "./types";

export const api = async <T,>(path: string, init?: RequestInit): Promise<T> => {
  const res = await fetch(path, {
    credentials: "include",
    headers: { "Content-Type": "application/json", ...(init?.headers || {}) },
    ...init
  });
  if (!res.ok) {
    const data = await res.json().catch(() => ({ detail: res.statusText }));
    throw new Error(data.detail || res.statusText);
  }
  return res.json();
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
