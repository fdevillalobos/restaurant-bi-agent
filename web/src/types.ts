export type User = {
  id: number;
  email: string;
  role: string;
  dsn_id: number | null;
  is_active: boolean;
};

export type MeResponse = {
  user: User;
  dsn: { id: number; name: string } | null;
  restaurants: string[];
  selected_restaurants: string[];
  csrf_token?: string | null;
  language: "en" | "es";
  capabilities: {
    settings: boolean;
    manage_dsns: boolean;
  };
};

export type AdminUser = User & {
  dsn_name: string;
  restaurants: string[];
  created_at?: string;
  updated_at?: string;
};

export type AdminDsn = {
  id: number;
  name: string;
  restaurant_count: number;
  created_at?: string;
  updated_at?: string;
};

export type AdminRestaurant = {
  id: number;
  name: string;
  dsn_id: number;
};

export type AdminInvite = {
  id: number;
  email: string;
  role: string;
  dsn_id: number | null;
  dsn_name?: string | null;
  restaurant_ids: number[];
  restaurant_names: string[];
  created_by?: number | null;
  created_by_email?: string | null;
  expires_at?: string | null;
  accepted_at?: string | null;
  revoked_at?: string | null;
  created_at?: string | null;
  status: "pending" | "expired" | "accepted" | "revoked";
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
