export type User = {
  id: number;
  email: string;
  role: string;
  dsn_id: number | null;
};

export type MeResponse = {
  user: User;
  dsn: { id: number; name: string } | null;
  restaurants: string[];
  selected_restaurants: string[];
};

export type VeraTable = {
  title: string;
  columns: string[];
  rows: Record<string, unknown>[];
  row_count: number;
};

export type VeraChart = {
  type: string;
  title: string;
  caption?: string | null;
  x?: string | null;
  y?: string | null;
  label?: string | null;
  data: Record<string, unknown>[];
};

export type VeraDebug = {
  sql?: string | null;
  queries?: Array<{ purpose: string; sql: string; row_count: number }>;
  failed_query?: { purpose?: string; sql?: string; params?: Record<string, unknown> };
  error?: string;
};

export type ChatResponse = {
  action: "clarify" | "answer";
  message: string;
  tables: VeraTable[];
  charts: VeraChart[];
  recommendations: string[];
  suggested_next_questions: string[];
  debug?: VeraDebug;
};

export type ChatMessage = {
  id: string;
  role: "user" | "assistant" | "status";
  content: string;
  response?: ChatResponse;
  created_at?: string | null;
};

export type StoredChatMessage = {
  id: number;
  role: "user" | "assistant";
  content: string;
  payload?: ChatResponse | null;
  selected_restaurants: string[];
  created_at?: string | null;
};
