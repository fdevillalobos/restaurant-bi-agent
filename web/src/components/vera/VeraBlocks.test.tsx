import { render, screen, within } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import type { ChatResponse } from "../../types";
import { VeraResponseBlocks } from "./VeraBlocks";

vi.mock("echarts-for-react", () => ({
  default: ({ option }: { option: { series?: Array<{ type: string }> } }) => (
    <div data-testid="echarts">{option.series?.[0]?.type}</div>
  )
}));

const response: ChatResponse = {
  action: "answer",
  message: "### Sales summary\n\nGross sales improved.",
  tables: [
    {
      title: "Query result",
      columns: ["month", "num_sales", "gross_sales", "avg_ticket"],
      rows: [{ month: "2026-05", num_sales: 236, gross_sales: 15420000, avg_ticket: 65355.9322 }],
      row_count: 1
    }
  ],
  charts: [
    {
      type: "bar",
      title: "Monthly gross sales",
      x: "month",
      y: "gross_sales",
      data: [{ month: "2026-05", gross_sales: 15420000 }]
    }
  ],
  recommendations: ["Compare dayparts."],
  suggested_next_questions: ["Which products drove it?"]
};

describe("VeraResponseBlocks", () => {
  it("renders markdown, chart, table, recommendations, and formatted metrics", () => {
    render(<VeraResponseBlocks response={response} onAsk={() => undefined} />);

    expect(screen.getByRole("heading", { name: "Sales summary" })).toBeInTheDocument();
    expect(screen.getByText("Monthly gross sales")).toBeInTheDocument();
    expect(screen.getByTestId("echarts")).toHaveTextContent("bar");
    expect(screen.getAllByText("$15,420,000.00").length).toBeGreaterThan(0);
    expect(screen.getAllByText("$65,355.93").length).toBeGreaterThan(0);
    expect(screen.getAllByText("236").length).toBeGreaterThan(0);
    expect(screen.getByText("Compare dayparts.")).toBeInTheDocument();
  });

  it("sends suggested next questions", async () => {
    const onAsk = vi.fn();
    render(<VeraResponseBlocks response={response} onAsk={onAsk} />);

    await userEvent.click(screen.getByRole("button", { name: "Which products drove it?" }));
    expect(onAsk).toHaveBeenCalledWith("Which products drove it?");
  });

  it("renders timeout/error assistant payloads without tables or charts", () => {
    render(
      <VeraResponseBlocks
        response={{
          action: "answer",
          message: "I could not complete that analysis because the database query timed out.",
          tables: [],
          charts: [],
          recommendations: ["Try narrowing the date range."],
          suggested_next_questions: []
        }}
        onAsk={() => undefined}
      />
    );

    expect(screen.getByText(/query timed out/i)).toBeInTheDocument();
    expect(screen.getByText("Try narrowing the date range.")).toBeInTheDocument();
    expect(screen.queryByRole("table")).not.toBeInTheDocument();
  });

  it("sorts table rows from sortable headers", async () => {
    render(
      <VeraResponseBlocks
        response={{
          ...response,
          tables: [{
            title: "Query result",
            columns: ["month", "gross_sales"],
            rows: [
              { month: "2026-05", gross_sales: 10 },
              { month: "2026-04", gross_sales: 20 }
            ],
            row_count: 2
          }],
          charts: []
        }}
        onAsk={() => undefined}
      />
    );

    const rowsBefore = screen.getAllByRole("row");
    expect(within(rowsBefore[1]).getByText("2026-04")).toBeInTheDocument();
    await userEvent.click(screen.getByRole("button", { name: /Month/i }));
    const rowsAfter = screen.getAllByRole("row");
    expect(within(rowsAfter[1]).getByText("2026-05")).toBeInTheDocument();
  });
});
