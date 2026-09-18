import { QueryClient, QueryClientProvider } from "@tanstack/react-query";
import { BrowserRouter, Route, Routes } from "react-router-dom";
import { AppShell } from "./components/AppShell";
import { InventoryPage } from "./pages/InventoryPage";
import { RecipeDetailPage } from "./pages/RecipeDetailPage";
import { RecipesPage } from "./pages/RecipesPage";

// Single application-level QueryClient (FE-01 §1.5). Configuration is kept
// minimal: no persistence, no polling, no speculative prefetching.
const queryClient = new QueryClient();

export default function App() {
  return (
    <QueryClientProvider client={queryClient}>
      <BrowserRouter>
        <Routes>
          <Route element={<AppShell />}>
            {/* "/" and "/recipes" resolve to the same Recipes page. */}
            <Route path="/" element={<RecipesPage />} />
            <Route path="/recipes" element={<RecipesPage />} />
            <Route path="/recipes/:recipeId" element={<RecipeDetailPage />} />
            <Route path="/inventory" element={<InventoryPage />} />
          </Route>
        </Routes>
      </BrowserRouter>
    </QueryClientProvider>
  );
}
