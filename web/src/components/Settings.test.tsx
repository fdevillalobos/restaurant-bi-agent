import { render, screen, waitFor } from "@testing-library/react";
import { describe, expect, it, vi } from "vitest";
import type { MeResponse } from "../types";
import { Settings } from "./Settings";

vi.mock("../api", () => ({
  api: vi.fn(async (path: string) => {
    if (path === "/api/admin/users") return { users: [] };
    if (path === "/api/admin/dsns") return { dsns: [{ id: 10, name: "Client", restaurant_count: 2 }] };
    if (path === "/api/admin/invites") return { invites: [] };
    return {};
  })
}));

const baseMe: MeResponse = {
  user: { id: 1, email: "admin@example.com", role: "admin", dsn_id: 10, is_active: true },
  dsn: { id: 10, name: "Client" },
  restaurants: ["A"],
  selected_restaurants: ["A"],
  csrf_token: "csrf",
  capabilities: { settings: true, manage_dsns: false }
};

describe("Settings", () => {
  it("hides DSN administration from scoped admins", async () => {
    render(<Settings me={baseMe} onBack={() => undefined} />);

    await waitFor(() => expect(screen.getByRole("tab", { name: "Users" })).toBeInTheDocument());
    expect(screen.queryByRole("tab", { name: "DSNs" })).not.toBeInTheDocument();
  });

  it("shows DSN administration to superusers", async () => {
    render(
      <Settings
        me={{
          ...baseMe,
          user: { ...baseMe.user, role: "superuser", dsn_id: null },
          capabilities: { settings: true, manage_dsns: true }
        }}
        onBack={() => undefined}
      />
    );

    await waitFor(() => expect(screen.getByRole("tab", { name: "DSNs" })).toBeInTheDocument());
  });
});
