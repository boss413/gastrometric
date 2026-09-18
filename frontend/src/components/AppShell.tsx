import { Outlet } from "react-router-dom";
import { NavBar } from "./NavBar";

/**
 * Application shell shared by every route: identity header, primary
 * navigation, and the routed content area. Visual treatment is
 * intentionally basic (work order §7); final presentation is later work.
 */
export function AppShell() {
  return (
    <div className="app-shell">
      <header className="app-header">
        <span className="app-title">Gastrometric Kitchen</span>
        <NavBar />
      </header>
      <main className="app-content">
        <Outlet />
      </main>
    </div>
  );
}
