import { render, screen, waitFor } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { beforeEach, describe, expect, it, vi } from "vitest";
import { InviteAccept } from "./InviteAccept";

describe("InviteAccept", () => {
  beforeEach(() => {
    vi.restoreAllMocks();
  });

  it("previews fixed invite details and accepts a password", async () => {
    const fetchMock = vi.fn(async (path: string, init?: RequestInit) => {
      if (!init?.method || init.method === "GET") {
        return Response.json({
          invite: {
            id: 1,
            email: "new@example.com",
            role: "user",
            dsn_id: 10,
            dsn_name: "Client",
            restaurant_ids: [],
            restaurant_names: [],
            status: "pending"
          }
        });
      }
      return Response.json({ ok: true });
    });
    vi.stubGlobal("fetch", fetchMock);

    render(<InviteAccept token="abc" />);

    await waitFor(() => expect(screen.getByText("new@example.com")).toBeInTheDocument());
    expect(screen.getByText("Client")).toBeInTheDocument();

    await userEvent.type(screen.getByLabelText("Password"), "strong-password");
    await userEvent.click(screen.getByRole("button", { name: "Accept invite" }));

    await waitFor(() => expect(screen.getByText("Password created.")).toBeInTheDocument());
    expect(fetchMock).toHaveBeenLastCalledWith(
      "/api/invites/abc/accept",
      expect.objectContaining({
        body: JSON.stringify({ password: "strong-password" }),
        method: "POST"
      })
    );
  });
});
