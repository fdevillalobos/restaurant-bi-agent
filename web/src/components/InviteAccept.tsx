import React from "react";
import { KeyRound } from "lucide-react";
import { api } from "../api";
import type { AdminInvite } from "../types";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";

export function InviteAccept({ token }: { token: string }) {
  const [invite, setInvite] = React.useState<AdminInvite | null>(null);
  const [password, setPassword] = React.useState("");
  const [error, setError] = React.useState("");
  const [loading, setLoading] = React.useState(true);
  const [saving, setSaving] = React.useState(false);
  const [accepted, setAccepted] = React.useState(false);

  React.useEffect(() => {
    api<{ invite: AdminInvite }>(`/api/invites/${token}`)
      .then((data) => setInvite(data.invite))
      .catch((err) => setError(err instanceof Error ? err.message : "Invite not found"))
      .finally(() => setLoading(false));
  }, [token]);

  async function submit(event: React.FormEvent) {
    event.preventDefault();
    setSaving(true);
    setError("");
    try {
      await api(`/api/invites/${token}/accept`, {
        method: "POST",
        body: JSON.stringify({ password })
      });
      setAccepted(true);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not accept invite");
    } finally {
      setSaving(false);
    }
  }

  return (
    <main className="grid min-h-screen place-items-center bg-[radial-gradient(circle_at_top_left,_#e5f4f1,_transparent_34%),#f6f8fb] p-6">
      <form className="grid w-full max-w-md gap-5 rounded-md border border-border bg-card p-7 shadow-workspace" onSubmit={submit}>
        <div className="flex h-12 w-12 items-center justify-center rounded-md bg-primary text-primary-foreground">
          <KeyRound className="h-6 w-6" />
        </div>
        <div>
          <p className="mb-1 text-xs font-semibold uppercase text-emerald-700">Vera invite</p>
          <h1 className="text-2xl font-semibold tracking-normal">Create your password</h1>
          <p className="mt-2 text-sm text-muted-foreground">Your email, role, DSN, and restaurant access are fixed by the invite.</p>
        </div>
        {loading ? (
          <p className="text-sm text-muted-foreground">Loading invite...</p>
        ) : invite ? (
          <div className="grid gap-2 rounded-md border border-border p-3 text-sm">
            <div className="flex items-center justify-between gap-2">
              <span className="font-medium">{invite.email}</span>
              <Badge variant="outline">{invite.role}</Badge>
            </div>
            <p className="text-muted-foreground">{invite.dsn_name || "No DSN assigned"}</p>
            <p className="text-muted-foreground">{invite.restaurant_names.length ? `${invite.restaurant_names.length} restaurant restrictions` : "All restaurants in this DSN"}</p>
            <Badge variant={invite.status === "pending" ? "success" : "outline"}>{invite.status}</Badge>
          </div>
        ) : null}
        {invite?.status === "pending" && !accepted && (
          <label className="grid gap-2 text-sm font-medium">
            Password
            <input
              className="h-10 rounded-md border border-input bg-background px-3 outline-none focus:ring-2 focus:ring-ring"
              value={password}
              onChange={(event) => setPassword(event.target.value)}
              type="password"
              autoComplete="new-password"
            />
          </label>
        )}
        {accepted && (
          <div className="grid gap-3 rounded-md border border-emerald-200 bg-emerald-50 p-3 text-sm text-emerald-900">
            <p className="font-medium">Password created.</p>
            <Button type="button" onClick={() => { window.location.href = "/"; }}>Go to login</Button>
          </div>
        )}
        {error && <p className="rounded-md border border-destructive/20 bg-destructive/10 p-3 text-sm text-destructive">{error}</p>}
        {invite?.status === "pending" && !accepted && <Button type="submit" disabled={saving}>{saving ? "Creating password" : "Accept invite"}</Button>}
      </form>
    </main>
  );
}
