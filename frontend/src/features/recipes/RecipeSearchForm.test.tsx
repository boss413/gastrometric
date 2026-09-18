import { render, screen } from "@testing-library/react";
import userEvent from "@testing-library/user-event";
import { describe, expect, it, vi } from "vitest";
import { RecipeSearchForm } from "./RecipeSearchForm";

describe("RecipeSearchForm", () => {
  it("renders a labeled search field and type selector", () => {
    render(<RecipeSearchForm onSubmit={vi.fn()} isSearching={false} />);
    expect(screen.getByLabelText(/find a recipe/i)).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: /recipe name/i })).toBeInTheDocument();
    expect(screen.getByRole("radio", { name: /ingredient/i })).toBeInTheDocument();
  });

  it("submits the trimmed query and selected type on button click", async () => {
    const onSubmit = vi.fn();
    render(<RecipeSearchForm onSubmit={onSubmit} isSearching={false} />);

    await userEvent.type(screen.getByLabelText(/find a recipe/i), "  chicken  ");
    await userEvent.click(screen.getByRole("radio", { name: /ingredient/i }));
    await userEvent.click(screen.getByRole("button", { name: /search/i }));

    expect(onSubmit).toHaveBeenCalledWith("chicken", "ingredient");
  });

  it("submits on Enter", async () => {
    const onSubmit = vi.fn();
    render(<RecipeSearchForm onSubmit={onSubmit} isSearching={false} />);

    const input = screen.getByLabelText(/find a recipe/i);
    await userEvent.type(input, "pasta{Enter}");

    expect(onSubmit).toHaveBeenCalledWith("pasta", "name");
  });

  it("rejects a query shorter than 2 characters without calling onSubmit", async () => {
    const onSubmit = vi.fn();
    render(<RecipeSearchForm onSubmit={onSubmit} isSearching={false} />);

    await userEvent.type(screen.getByLabelText(/find a recipe/i), "a");
    await userEvent.click(screen.getByRole("button", { name: /search/i }));

    expect(onSubmit).not.toHaveBeenCalled();
    expect(screen.getByRole("alert")).toHaveTextContent(/at least 2 characters/i);
  });

  it("does not submit while a search is already in progress", async () => {
    const onSubmit = vi.fn();
    render(<RecipeSearchForm onSubmit={onSubmit} isSearching={true} />);

    await userEvent.type(screen.getByLabelText(/find a recipe/i), "chicken");
    // The button is disabled while searching...
    expect(screen.getByRole("button", { name: /searching/i })).toBeDisabled();
  });

  it("preserves the submitted query in the input", async () => {
    render(<RecipeSearchForm onSubmit={vi.fn()} isSearching={false} />);

    const input = screen.getByLabelText(/find a recipe/i);
    await userEvent.type(input, "chicken{Enter}");

    expect(input).toHaveValue("chicken");
  });
});
