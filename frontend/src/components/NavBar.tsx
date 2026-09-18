import { NavLink, useLocation } from "react-router-dom";

/**
 * Primary navigation, present in the application shell on every screen.
 * Uses real router links (`NavLink`), not manual URL manipulation.
 */
export function NavBar() {
  const location = useLocation();
  const onRecipes = location.pathname === "/" || location.pathname.startsWith("/recipes");

  return (
    <nav className="primary-nav" aria-label="Primary">
      <ul>
        <li>
          <NavLink to="/recipes" className={() => (onRecipes ? "active" : "")}>
            Recipes
          </NavLink>
        </li>
        <li>
          <NavLink to="/inventory" className={({ isActive }) => (isActive ? "active" : "")}>
            Inventory
          </NavLink>
        </li>
      </ul>
    </nav>
  );
}
