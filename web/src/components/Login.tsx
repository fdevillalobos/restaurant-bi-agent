import React from "react";
import { BarChart3 } from "lucide-react";
import { api } from "../api";
import type { MeResponse } from "../types";
import { Button } from "./ui/button";

export function Login({ onLogin }: { onLogin: (me: MeResponse) => void }) {
  const [email, setEmail] = React.useState("");
  const [password, setPassword] = React.useState("");
  const [error, setError] = React.useState("");
  const [loading, setLoading] = React.useState(false);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setLoading(true);
    setError("");
    try {
      const me = await api<MeResponse>("/api/login", {
        method: "POST",
        body: JSON.stringify({ email, password })
      });
      onLogin(me);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Login failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <main className="grid min-h-screen place-items-center bg-[radial-gradient(circle_at_top_left,_#e5f4f1,_transparent_34%),#f6f8fb] p-6">
      <form className="grid w-full max-w-md gap-5 rounded-md border border-border bg-card p-7 shadow-workspace" onSubmit={submit}>
        <div className="flex h-12 w-12 items-center justify-center rounded-md bg-primary text-primary-foreground">
          <BarChart3 className="h-6 w-6" />
        </div>
        <div>
          <p className="mb-1 text-xs font-semibold uppercase text-emerald-700">Restaurant BI</p>
          <h1 className="text-3xl font-semibold tracking-normal">Vera</h1>
          <p className="mt-2 text-sm text-muted-foreground">Sign in to analyze sales, products, channels, and operating patterns.</p>
        </div>
        <label className="grid gap-2 text-sm font-medium">
          Email
          <input className="h-10 rounded-md border border-input bg-background px-3 outline-none focus:ring-2 focus:ring-ring" value={email} onChange={(event) => setEmail(event.target.value)} autoComplete="email" />
        </label>
        <label className="grid gap-2 text-sm font-medium">
          Password
          <input className="h-10 rounded-md border border-input bg-background px-3 outline-none focus:ring-2 focus:ring-ring" value={password} onChange={(event) => setPassword(event.target.value)} type="password" autoComplete="current-password" />
        </label>
        {error && <p className="rounded-md border border-destructive/20 bg-destructive/10 p-3 text-sm text-destructive">{error}</p>}
        <Button type="submit" disabled={loading}>{loading ? "Signing in" : "Sign in"}</Button>
      </form>
    </main>
  );
}
