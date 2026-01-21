import { FilterX, LogOut } from "lucide-react";
import { BRAND_EYEBROW, BRAND_SUBTITLE, BRAND_TITLE, THEMES } from "../constants";
import type { AuthState, DateFilter, Theme, TypeFilter, UserRole } from "../types";
import { CollapseIcon, ExpandIcon } from "./icons";

type HeaderProps = {
  auth: AuthState;
  username: string;
  userRole: UserRole;
  theme: Theme;
  showTrash: boolean;
  filtersOpen: boolean;
  filtersActive: boolean;
  typeFilter: TypeFilter;
  sizeMinMb: string;
  sizeMaxMb: string;
  dateFilter: DateFilter;
  onThemeChange: (theme: Theme) => void;
  onLogout: () => void;
  onToggleFilters: () => void;
  onTypeFilterChange: (value: TypeFilter) => void;
  onSizeMinChange: (value: string) => void;
  onSizeMaxChange: (value: string) => void;
  onDateFilterChange: (value: DateFilter) => void;
  onClearFilters: () => void;
};

export function Header({
  auth,
  username,
  userRole,
  theme,
  showTrash,
  filtersOpen,
  filtersActive,
  typeFilter,
  sizeMinMb,
  sizeMaxMb,
  dateFilter,
  onThemeChange,
  onLogout,
  onToggleFilters,
  onTypeFilterChange,
  onSizeMinChange,
  onSizeMaxChange,
  onDateFilterChange,
  onClearFilters,
}: HeaderProps) {
  return (
    <header className="header">
      <div>
        {/* <p className="eyebrow">{BRAND_EYEBROW}</p> */}
        <div className="brand-title">
          <img className="brand-logo" src="/logo.png" alt={`${BRAND_TITLE} logo`} />
          {/* <h1>{BRAND_TITLE}</h1> */}
        </div>
        {/* <p className="subtitle">{BRAND_SUBTITLE}</p> */}
        {/* {auth === "authed" ? (
          <p className="meta">
            Signed in as {username || "unknown"} ({userRole})
          </p>
        ) : null} */}
      </div>
      <div className="header-actions">
        <div className="header-controls">
          {auth === "authed" ? (
            <button className="ghost" onClick={onLogout} aria-label="Logout" title="Logout">
              <LogOut size={16} strokeWidth={1.8} aria-hidden="true" />
            </button>
          ) : null}
        </div>
      </div>
    </header>
  );
}
