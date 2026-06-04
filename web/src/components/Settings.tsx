import React from "react";
import { Copy, RefreshCw, RotateCcw, Search, Settings as SettingsIcon, Shield, UserPlus } from "lucide-react";
import { api } from "../api";
import type { AdminDsn, AdminInvite, AdminRestaurant, AdminUser, MeResponse } from "../types";
import { cn } from "../lib/utils";
import { Badge } from "./ui/badge";
import { Button } from "./ui/button";
import { ScrollArea } from "./ui/scroll-area";
import { Sheet, SheetContent, SheetTitle } from "./ui/sheet";
import { Tabs, TabsContent, TabsList, TabsTrigger } from "./ui/tabs";

const ROLE_OPTIONS = ["user", "db_admin", "admin", "superuser"];

function roleOptionsFor(me: MeResponse) {
  return me.user.role === "superuser" ? ROLE_OPTIONS : ["user", "db_admin"];
}

function statusVariant(status: string) {
  if (status === "pending") return "default";
  if (status === "accepted") return "secondary";
  return "outline";
}

function Field({ label, children }: { label: string; children: React.ReactNode }) {
  return (
    <label className="grid gap-2 text-sm font-medium">
      {label}
      {children}
    </label>
  );
}

function TextInput(props: React.InputHTMLAttributes<HTMLInputElement>) {
  return <input className="h-10 rounded-md border border-input bg-background px-3 text-sm outline-none focus:ring-2 focus:ring-ring" {...props} />;
}

function Select(props: React.SelectHTMLAttributes<HTMLSelectElement>) {
  return <select className="h-10 rounded-md border border-input bg-background px-3 text-sm outline-none focus:ring-2 focus:ring-ring" {...props} />;
}

function RestaurantChecks({
  restaurants,
  selected,
  onChange
}: {
  restaurants: AdminRestaurant[];
  selected: number[];
  onChange: (ids: number[]) => void;
}) {
  return (
    <div className="grid max-h-56 gap-2 overflow-auto rounded-md border border-border p-2">
      {restaurants.length === 0 ? (
        <p className="p-2 text-sm text-muted-foreground">No synced restaurants.</p>
      ) : restaurants.map((restaurant) => {
        const checked = selected.includes(restaurant.id);
        return (
          <label key={restaurant.id} className="flex items-center gap-2 rounded-md px-2 py-1 text-sm hover:bg-accent">
            <input
              type="checkbox"
              checked={checked}
              onChange={() => onChange(checked ? selected.filter((id) => id !== restaurant.id) : [...selected, restaurant.id])}
            />
            <span>{restaurant.name}</span>
          </label>
        );
      })}
    </div>
  );
}

function UsersTab({
  me,
  users,
  dsns,
  onRefresh
}: {
  me: MeResponse;
  users: AdminUser[];
  dsns: AdminDsn[];
  onRefresh: () => Promise<void>;
}) {
  const [query, setQuery] = React.useState("");
  const [editing, setEditing] = React.useState<AdminUser | null>(null);
  const [role, setRole] = React.useState("user");
  const [dsnId, setDsnId] = React.useState<number | null>(me.user.dsn_id);
  const [isActive, setIsActive] = React.useState(true);
  const [restaurantIds, setRestaurantIds] = React.useState<number[]>([]);
  const [restaurants, setRestaurants] = React.useState<AdminRestaurant[]>([]);
  const [error, setError] = React.useState("");
  const [saving, setSaving] = React.useState(false);

  const filtered = users.filter((user) => {
    const needle = `${user.email} ${user.role} ${user.dsn_name}`.toLowerCase();
    return needle.includes(query.toLowerCase());
  });

  async function loadRestaurants(nextDsnId: number | null) {
    if (!nextDsnId) {
      setRestaurants([]);
      return;
    }
    const data = await api<{ restaurants: AdminRestaurant[] }>(`/api/admin/dsns/${nextDsnId}/restaurants`);
    setRestaurants(data.restaurants);
  }

  async function openUser(user: AdminUser) {
    setEditing(user);
    setRole(user.role);
    setDsnId(user.dsn_id);
    setIsActive(user.is_active);
    setRestaurantIds([]);
    setError("");
    await loadRestaurants(user.dsn_id);
    if (user.dsn_id) {
      const data = await api<{ restaurants: AdminRestaurant[] }>(`/api/admin/dsns/${user.dsn_id}/restaurants`);
      const byName = new Map(data.restaurants.map((restaurant) => [restaurant.name, restaurant.id]));
      setRestaurants(data.restaurants);
      setRestaurantIds(user.restaurants.map((name) => byName.get(name)).filter((id): id is number => typeof id === "number"));
    }
  }

  async function save() {
    if (!editing) return;
    setSaving(true);
    setError("");
    try {
      await api(`/api/admin/users/${editing.id}`, {
        method: "PATCH",
        body: JSON.stringify({ role, dsn_id: dsnId, is_active: isActive, restaurant_ids: restaurantIds })
      });
      await onRefresh();
      setEditing(null);
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not update user");
    } finally {
      setSaving(false);
    }
  }

  return (
    <div className="grid min-h-0 gap-4">
      <div className="flex items-center gap-2 rounded-md border border-border bg-card px-3 py-2">
        <Search className="h-4 w-4 text-muted-foreground" />
        <input className="min-w-0 flex-1 bg-transparent text-sm outline-none" placeholder="Search users" value={query} onChange={(event) => setQuery(event.target.value)} />
      </div>
      <div className="overflow-hidden rounded-md border border-border bg-card">
        <table className="w-full text-left text-sm">
          <thead className="bg-muted text-xs uppercase text-muted-foreground">
            <tr>
              <th className="px-4 py-3">Email</th>
              <th className="px-4 py-3">Role</th>
              <th className="px-4 py-3">DSN</th>
              <th className="px-4 py-3">Access</th>
              <th className="px-4 py-3">Status</th>
            </tr>
          </thead>
          <tbody>
            {filtered.map((user) => (
              <tr key={user.id} className="cursor-pointer border-t border-border hover:bg-accent" onClick={() => void openUser(user)}>
                <td className="px-4 py-3 font-medium">{user.email}</td>
                <td className="px-4 py-3"><Badge variant="outline">{user.role}</Badge></td>
                <td className="px-4 py-3">{user.dsn_name}</td>
                <td className="px-4 py-3 text-muted-foreground">{user.restaurants.length ? `${user.restaurants.length} restricted` : "All restaurants"}</td>
                <td className="px-4 py-3"><Badge variant={user.is_active ? "secondary" : "outline"}>{user.is_active ? "Active" : "Inactive"}</Badge></td>
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <Sheet open={!!editing} onOpenChange={(open) => !open && setEditing(null)}>
        <SheetContent>
          <SheetTitle className="mb-4 text-xl font-semibold">Edit User</SheetTitle>
          {editing && (
            <div className="grid gap-4">
              <div className="rounded-md border border-border p-3">
                <p className="font-medium">{editing.email}</p>
                <p className="text-sm text-muted-foreground">Changes are audited and take effect immediately.</p>
              </div>
              <Field label="Role">
                <Select value={role} onChange={(event) => setRole(event.target.value)}>
                  {roleOptionsFor(me).map((option) => <option key={option} value={option}>{option}</option>)}
                </Select>
              </Field>
              <Field label="DSN">
                <Select
                  disabled={me.user.role !== "superuser"}
                  value={dsnId || ""}
                  onChange={(event) => {
                    const next = event.target.value ? Number(event.target.value) : null;
                    setDsnId(next);
                    setRestaurantIds([]);
                    void loadRestaurants(next);
                  }}
                >
                  <option value="">No DSN</option>
                  {dsns.map((dsn) => <option key={dsn.id} value={dsn.id}>{dsn.name}</option>)}
                </Select>
              </Field>
              <Field label="Restaurant restrictions">
                <RestaurantChecks restaurants={restaurants} selected={restaurantIds} onChange={setRestaurantIds} />
              </Field>
              <label className="flex items-center gap-2 text-sm font-medium">
                <input type="checkbox" checked={isActive} onChange={(event) => setIsActive(event.target.checked)} />
                Active user
              </label>
              {error && <p className="rounded-md border border-destructive/20 bg-destructive/10 p-3 text-sm text-destructive">{error}</p>}
              <Button onClick={() => void save()} disabled={saving}>{saving ? "Saving" : "Save changes"}</Button>
            </div>
          )}
        </SheetContent>
      </Sheet>
    </div>
  );
}

function InvitesTab({ me, dsns, invites, onRefresh }: { me: MeResponse; dsns: AdminDsn[]; invites: AdminInvite[]; onRefresh: () => Promise<void> }) {
  const [email, setEmail] = React.useState("");
  const [role, setRole] = React.useState("user");
  const [dsnId, setDsnId] = React.useState<number | null>(me.user.dsn_id || dsns[0]?.id || null);
  const [restaurants, setRestaurants] = React.useState<AdminRestaurant[]>([]);
  const [restaurantIds, setRestaurantIds] = React.useState<number[]>([]);
  const [inviteUrl, setInviteUrl] = React.useState("");
  const [error, setError] = React.useState("");
  const [creating, setCreating] = React.useState(false);

  React.useEffect(() => {
    if (!dsnId) return;
    api<{ restaurants: AdminRestaurant[] }>(`/api/admin/dsns/${dsnId}/restaurants`)
      .then((data) => setRestaurants(data.restaurants))
      .catch(() => setRestaurants([]));
  }, [dsnId]);

  async function create() {
    setCreating(true);
    setError("");
    setInviteUrl("");
    try {
      const data = await api<{ invite: AdminInvite; invite_url: string }>("/api/admin/invites", {
        method: "POST",
        body: JSON.stringify({ email, role, dsn_id: dsnId, restaurant_ids: restaurantIds })
      });
      setInviteUrl(data.invite_url);
      setEmail("");
      setRestaurantIds([]);
      await onRefresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not create invite");
    } finally {
      setCreating(false);
    }
  }

  async function revoke(inviteId: number) {
    await api(`/api/admin/invites/${inviteId}`, { method: "DELETE" });
    await onRefresh();
  }

  return (
    <div className="grid gap-5 lg:grid-cols-[360px_minmax(0,1fr)]">
      <section className="grid gap-4 rounded-md border border-border bg-card p-4">
        <div>
          <h2 className="flex items-center gap-2 text-sm font-semibold"><UserPlus className="h-4 w-4" /> Create invite</h2>
          <p className="mt-1 text-sm text-muted-foreground">Links expire in 24 hours and can be used once.</p>
        </div>
        <Field label="Email"><TextInput value={email} onChange={(event) => setEmail(event.target.value)} /></Field>
        <Field label="Role">
          <Select value={role} onChange={(event) => setRole(event.target.value)}>
            {roleOptionsFor(me).map((option) => <option key={option} value={option}>{option}</option>)}
          </Select>
        </Field>
        <Field label="DSN">
          <Select disabled={me.user.role !== "superuser"} value={dsnId || ""} onChange={(event) => { setDsnId(Number(event.target.value)); setRestaurantIds([]); }}>
            {dsns.map((dsn) => <option key={dsn.id} value={dsn.id}>{dsn.name}</option>)}
          </Select>
        </Field>
        <Field label="Restaurant restrictions">
          <RestaurantChecks restaurants={restaurants} selected={restaurantIds} onChange={setRestaurantIds} />
        </Field>
        {error && <p className="rounded-md border border-destructive/20 bg-destructive/10 p-3 text-sm text-destructive">{error}</p>}
        {inviteUrl && (
          <div className="grid gap-2 rounded-md border border-emerald-200 bg-emerald-50 p-3 text-sm">
            <p className="font-medium text-emerald-900">Invite link ready</p>
            <code className="break-all text-emerald-950">{inviteUrl}</code>
            <Button variant="outline" onClick={() => void navigator.clipboard.writeText(inviteUrl)}><Copy className="h-4 w-4" /> Copy link</Button>
          </div>
        )}
        <Button onClick={() => void create()} disabled={creating}>{creating ? "Creating" : "Create invite"}</Button>
      </section>
      <section className="overflow-hidden rounded-md border border-border bg-card">
        <table className="w-full text-left text-sm">
          <thead className="bg-muted text-xs uppercase text-muted-foreground">
            <tr>
              <th className="px-4 py-3">Email</th>
              <th className="px-4 py-3">Role</th>
              <th className="px-4 py-3">DSN</th>
              <th className="px-4 py-3">Status</th>
              <th className="px-4 py-3">Action</th>
            </tr>
          </thead>
          <tbody>
            {invites.map((invite) => (
              <tr key={invite.id} className="border-t border-border">
                <td className="px-4 py-3 font-medium">{invite.email}</td>
                <td className="px-4 py-3">{invite.role}</td>
                <td className="px-4 py-3">{invite.dsn_name || "-"}</td>
                <td className="px-4 py-3"><Badge variant={statusVariant(invite.status)}>{invite.status}</Badge></td>
                <td className="px-4 py-3">
                  {invite.status === "pending" && <Button variant="ghost" size="sm" onClick={() => void revoke(invite.id)}>Revoke</Button>}
                </td>
              </tr>
            ))}
          </tbody>
        </table>
      </section>
    </div>
  );
}

function DsnsTab({ dsns, onRefresh }: { dsns: AdminDsn[]; onRefresh: () => Promise<void> }) {
  const [name, setName] = React.useState("");
  const [dsn, setDsn] = React.useState("");
  const [editing, setEditing] = React.useState<AdminDsn | null>(null);
  const [editName, setEditName] = React.useState("");
  const [editDsn, setEditDsn] = React.useState("");
  const [error, setError] = React.useState("");

  async function create() {
    setError("");
    try {
      await api("/api/admin/dsns", { method: "POST", body: JSON.stringify({ name, dsn }) });
      setName("");
      setDsn("");
      await onRefresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not create DSN");
    }
  }

  async function save() {
    if (!editing) return;
    setError("");
    try {
      await api(`/api/admin/dsns/${editing.id}`, {
        method: "PATCH",
        body: JSON.stringify({ name: editName || undefined, dsn: editDsn || undefined })
      });
      setEditing(null);
      setEditDsn("");
      await onRefresh();
    } catch (err) {
      setError(err instanceof Error ? err.message : "Could not update DSN");
    }
  }

  async function sync(dsnId: number) {
    await api(`/api/admin/dsns/${dsnId}/sync-restaurants`, { method: "POST" });
    await onRefresh();
  }

  return (
    <div className="grid gap-5 lg:grid-cols-[380px_minmax(0,1fr)]">
      <section className="grid gap-4 rounded-md border border-border bg-card p-4">
        <h2 className="text-sm font-semibold">Create DSN</h2>
        <Field label="Name"><TextInput value={name} onChange={(event) => setName(event.target.value)} /></Field>
        <Field label="Postgres DSN"><TextInput type="password" value={dsn} onChange={(event) => setDsn(event.target.value)} placeholder="Stored write-only" /></Field>
        {error && <p className="rounded-md border border-destructive/20 bg-destructive/10 p-3 text-sm text-destructive">{error}</p>}
        <Button onClick={() => void create()}>Test, save, and sync</Button>
      </section>
      <section className="grid gap-3">
        {dsns.map((item) => (
          <div key={item.id} className="flex items-center justify-between gap-3 rounded-md border border-border bg-card p-4">
            <div>
              <p className="font-medium">{item.name}</p>
              <p className="text-sm text-muted-foreground">{item.restaurant_count} synced restaurants</p>
            </div>
            <div className="flex gap-2">
              <Button variant="outline" onClick={() => void sync(item.id)}><RotateCcw className="h-4 w-4" /> Sync</Button>
              <Button variant="ghost" onClick={() => { setEditing(item); setEditName(item.name); setEditDsn(""); }}>Edit</Button>
            </div>
          </div>
        ))}
      </section>
      <Sheet open={!!editing} onOpenChange={(open) => !open && setEditing(null)}>
        <SheetContent>
          <SheetTitle className="mb-4 text-xl font-semibold">Edit DSN</SheetTitle>
          {editing && (
            <div className="grid gap-4">
              <Field label="Name"><TextInput value={editName} onChange={(event) => setEditName(event.target.value)} /></Field>
              <Field label="Replace DSN"><TextInput type="password" value={editDsn} onChange={(event) => setEditDsn(event.target.value)} placeholder="Leave blank to keep current DSN" /></Field>
              <p className="text-sm text-muted-foreground">Current DSN values are never displayed after saving.</p>
              {error && <p className="rounded-md border border-destructive/20 bg-destructive/10 p-3 text-sm text-destructive">{error}</p>}
              <Button onClick={() => void save()}>Save DSN</Button>
            </div>
          )}
        </SheetContent>
      </Sheet>
    </div>
  );
}

export function Settings({ me, onBack }: { me: MeResponse; onBack: () => void }) {
  const [users, setUsers] = React.useState<AdminUser[]>([]);
  const [dsns, setDsns] = React.useState<AdminDsn[]>([]);
  const [invites, setInvites] = React.useState<AdminInvite[]>([]);
  const [loading, setLoading] = React.useState(true);

  async function refresh() {
    const [userData, dsnData, inviteData] = await Promise.all([
      api<{ users: AdminUser[] }>("/api/admin/users"),
      api<{ dsns: AdminDsn[] }>("/api/admin/dsns"),
      api<{ invites: AdminInvite[] }>("/api/admin/invites")
    ]);
    setUsers(userData.users);
    setDsns(dsnData.dsns);
    setInvites(inviteData.invites);
  }

  React.useEffect(() => {
    refresh().catch(() => undefined).finally(() => setLoading(false));
  }, []);

  return (
    <section className="grid h-screen min-h-0 grid-rows-[auto_minmax(0,1fr)] bg-background">
      <header className="flex items-center justify-between gap-3 border-b border-border bg-card px-6 py-4">
        <div>
          <p className="flex items-center gap-2 text-sm font-semibold"><SettingsIcon className="h-4 w-4" /> Settings</p>
          <p className="text-xs text-muted-foreground">Manage users, invite links, DSNs, and restaurant access.</p>
        </div>
        <div className="flex gap-2">
          <Button variant="outline" onClick={() => void refresh()}><RefreshCw className="h-4 w-4" /> Refresh</Button>
          <Button variant="ghost" onClick={onBack}>Back to chat</Button>
        </div>
      </header>
      <ScrollArea className="min-h-0">
        <div className="mx-auto grid w-full max-w-7xl gap-5 p-6">
          <div className="rounded-md border border-border bg-card p-4">
            <p className="flex items-center gap-2 text-sm font-medium"><Shield className="h-4 w-4" /> Signed in as {me.user.email}</p>
            <p className="mt-1 text-sm text-muted-foreground">
              {me.user.role === "superuser" ? "Superusers can manage all DSNs and roles." : "Scoped admins can manage user and db_admin accounts in their own DSN."}
            </p>
          </div>
          {loading ? (
            <div className="rounded-md border border-border bg-card p-8 text-sm text-muted-foreground">Loading settings...</div>
          ) : (
            <Tabs defaultValue="users">
              <TabsList>
                <TabsTrigger value="users">Users</TabsTrigger>
                <TabsTrigger value="invites">Invites</TabsTrigger>
                {me.capabilities.manage_dsns && <TabsTrigger value="dsns">DSNs</TabsTrigger>}
              </TabsList>
              <TabsContent value="users"><UsersTab me={me} users={users} dsns={dsns} onRefresh={refresh} /></TabsContent>
              <TabsContent value="invites"><InvitesTab me={me} dsns={dsns} invites={invites} onRefresh={refresh} /></TabsContent>
              {me.capabilities.manage_dsns && <TabsContent value="dsns"><DsnsTab dsns={dsns} onRefresh={refresh} /></TabsContent>}
            </Tabs>
          )}
        </div>
      </ScrollArea>
    </section>
  );
}
