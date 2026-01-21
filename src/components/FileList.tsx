import { Archive, ChevronDown, ChevronUp, File, FileText, Image, Music, Video } from "lucide-react";
import type { Entry, SortMode, TrashItem, ViewMode } from "../types";
import { formatBytes, formatDate } from "../utils/format";
import { getFileCategory, isImagePreviewable } from "../utils/fileTypes";
import { joinPath } from "../utils/path";
import { API_BASE } from "../constants";
import { FolderIcon } from "./icons";
import { Pagination, type PaginationProps } from "./Pagination";

type FileListProps = {
  showTrash: boolean;
  loading: boolean;
  trashItems: TrashItem[];
  filtered: Entry[];
  path: string;
  viewMode: ViewMode;
  selectedNames: string[];
  activeName?: string | null;
  allSelected: boolean;
  dragActive: boolean;
  actionLoading: boolean;
  canWrite: boolean;
  sortMode: SortMode;
  onSortModeChange: (value: SortMode) => void;
  pagination?: PaginationProps;
  showPaginationTop?: boolean;
  onToggleSelectAll: () => void;
  onToggleSelect: (entry: Entry) => void;
  onEntryClick: (entry: Entry) => void;
  onEntryDoubleClick: (entry: Entry) => void;
  onRestore: (item: TrashItem) => void;
};

export function FileList({
  showTrash,
  loading,
  trashItems,
  filtered,
  path,
  viewMode,
  selectedNames,
  activeName,
  allSelected,
  dragActive,
  actionLoading,
  canWrite,
  sortMode,
  onSortModeChange,
  pagination,
  showPaginationTop = false,
  onToggleSelectAll,
  onToggleSelect,
  onEntryClick,
  onEntryDoubleClick,
  onRestore,
}: FileListProps) {
  const isGridView = viewMode === "grid" && !showTrash;
  const getEntryIcon = (entry: Entry | TrashItem) => {
    if (entry.type === "dir") {
      return { icon: <FolderIcon />, className: "dir" };
    }
    const category = getFileCategory(entry.name);
    if (category === "image") {
      return { icon: <Image aria-hidden="true" />, className: "image" };
    }
    if (category === "document") {
      return { icon: <FileText aria-hidden="true" />, className: "document" };
    }
    if (category === "audio") {
      return { icon: <Music aria-hidden="true" />, className: "audio" };
    }
    if (category === "video") {
      return { icon: <Video aria-hidden="true" />, className: "video" };
    }
    if (category === "archive") {
      return { icon: <Archive aria-hidden="true" />, className: "archive" };
    }
    return { icon: <File aria-hidden="true" />, className: "other" };
  };
  const sortState = {
    name: sortMode === "name-asc" ? "asc" : sortMode === "name-desc" ? "desc" : null,
    size: sortMode === "size-asc" ? "asc" : sortMode === "size-desc" ? "desc" : null,
    date: sortMode === "date-asc" ? "asc" : sortMode === "date-desc" ? "desc" : null,
  } as const;
  const handleSortToggle = (column: "name" | "size" | "date") => {
    const nextMode =
      column === "name"
        ? sortState.name === "asc"
          ? "name-desc"
          : sortState.name === "desc"
            ? "default"
            : "name-asc"
        : column === "size"
          ? sortState.size === "asc"
            ? "size-desc"
            : sortState.size === "desc"
              ? "default"
              : "size-asc"
          : sortState.date === "asc"
            ? "date-desc"
            : sortState.date === "desc"
              ? "default"
              : "date-asc";
    onSortModeChange(nextMode);
  };

  return (
    <div className={`card list ${dragActive ? "dragging" : ""}${isGridView ? " is-grid" : ""}`} style={{border: 'none'}}>
      {pagination && showPaginationTop ? <Pagination {...pagination} compact /> : null}
      {showTrash ? (
        <>
          <div className="list-header trash">
            <span>Item</span>
            <span>Original</span>
            <span>Deleted</span>
            <span>Action</span>
          </div>
          {loading ? (
            <div className="empty">Loading trash...</div>
          ) : trashItems.length === 0 ? (
            <div className="empty">Trash is empty.</div>
          ) : (
            trashItems.map((item, index) => {
              const entryIcon = getEntryIcon(item);
              return (
                <div
                  key={item.id}
                  className="row trash"
                  style={{ animationDelay: `${index * 20}ms` }}
                >
                  <div className="name">
                    <span className={`icon ${entryIcon.className}`}>{entryIcon.icon}</span>
                    {item.name}
                  </div>
                  <span className="muted">{item.originalPath}</span>
                  <span>{formatDate(item.deletedAt)}</span>
                  <button
                    className="ghost"
                    onClick={() => onRestore(item)}
                    disabled={actionLoading || !canWrite}
                  >
                    Restore
                  </button>
                </div>
              );
            })
          )}
        </>
      ) : (
        <>
          <div className={`list-header${isGridView ? " is-grid" : ""}`}>
            <span>
              <input type="checkbox" checked={allSelected} onChange={onToggleSelectAll} />
            </span>
            <button
              type="button"
              className={`sort-button${sortState.name ? " is-active" : ""}`}
              onClick={() => handleSortToggle("name")}
              aria-pressed={Boolean(sortState.name)}
            >
              Name
              <span className={`sort-indicator${sortState.name ? ` ${sortState.name}` : ""}`}>
                <ChevronUp className="sort-arrow sort-arrow-up" aria-hidden="true" />
                <ChevronDown className="sort-arrow sort-arrow-down" aria-hidden="true" />
              </span>
            </button>
            <button
              type="button"
              className={`sort-button${sortState.size ? " is-active" : ""}`}
              onClick={() => handleSortToggle("size")}
              aria-pressed={Boolean(sortState.size)}
            >
              Size
              <span className={`sort-indicator${sortState.size ? ` ${sortState.size}` : ""}`}>
                <ChevronUp className="sort-arrow sort-arrow-up" aria-hidden="true" />
                <ChevronDown className="sort-arrow sort-arrow-down" aria-hidden="true" />
              </span>
            </button>
            <button
              type="button"
              className={`sort-button${sortState.date ? " is-active" : ""}`}
              onClick={() => handleSortToggle("date")}
              aria-pressed={Boolean(sortState.date)}
            >
              Modified
              <span className={`sort-indicator${sortState.date ? ` ${sortState.date}` : ""}`}>
                <ChevronUp className="sort-arrow sort-arrow-up" aria-hidden="true" />
                <ChevronDown className="sort-arrow sort-arrow-down" aria-hidden="true" />
              </span>
            </button>
          </div>
          {loading ? (
            <div className="empty">Loading directory...</div>
          ) : filtered.length === 0 ? (
            <div className="empty">Nothing here yet.</div>
          ) : isGridView ? (
            <div className="thumb-grid">
              {filtered.map((entry, index) => {
                const isSelected = selectedNames.includes(entry.name) || entry.name === activeName;
                const entryPath = joinPath(path, entry.name);
                const isImage = entry.type === "file" && isImagePreviewable(entry.name);
                const entryIcon = getEntryIcon(entry);
                return (
                  <div
                    key={`${entry.type}-${entry.name}`}
                    className={`thumb-card ${entry.type}${isSelected ? " selected" : ""}`}
                    style={{ animationDelay: `${index * 20}ms` }}
                  >
                    <div className="thumb-top">
                      <input
                        type="checkbox"
                        checked={isSelected}
                        onChange={() => onToggleSelect(entry)}
                        aria-label={`Select ${entry.name}`}
                      />
                      <span className="thumb-type">{entry.type === "dir" ? "Folder" : "File"}</span>
                    </div>
                    <button
                      className="thumb-media"
                      onClick={() => onEntryClick(entry)}
                      onDoubleClick={() => onEntryDoubleClick(entry)}
                    >
                      {isImage ? (
                        <img
                          src={`${API_BASE}/image?path=${encodeURIComponent(entryPath)}`}
                          alt={entry.name}
                          loading="lazy"
                        />
                      ) : (
                        <span className={`thumb-icon ${entryIcon.className}`}>
                          {entryIcon.icon}
                        </span>
                      )}
                    </button>
                    <button
                      className="thumb-name"
                      onClick={() => onEntryClick(entry)}
                      onDoubleClick={() => onEntryDoubleClick(entry)}
                    >
                      {entry.name}
                    </button>
                    <div className="thumb-meta">
                      <span>{entry.type === "file" ? formatBytes(entry.size) : "Folder"}</span>
                      <span>{formatDate(entry.mtime)}</span>
                    </div>
                  </div>
                );
              })}
            </div>
          ) : (
            filtered.map((entry, index) => {
              const entryIcon = getEntryIcon(entry);
              return (
                <div
                  key={`${entry.type}-${entry.name}`}
                  className={`row ${entry.type} ${
                    selectedNames.includes(entry.name) || entry.name === activeName ? "selected" : ""
                  }`}
                  style={{ animationDelay: `${index * 20}ms` }}
                >
                  <span>
                    <input
                      type="checkbox"
                      checked={selectedNames.includes(entry.name)}
                      onChange={() => onToggleSelect(entry)}
                    />
                  </span>
                  <button
                    className="name"
                    onClick={() => onEntryClick(entry)}
                    onDoubleClick={() => onEntryDoubleClick(entry)}
                  >
                    <span className={`icon ${entryIcon.className}`}>{entryIcon.icon}</span>
                    {entry.name}
                  </button>
                  <span>{entry.type === "file" ? formatBytes(entry.size) : "--"}</span>
                  <span>{formatDate(entry.mtime)}</span>
                </div>
              );
            })
          )}
          {dragActive ? <div className="drop-overlay">Drop files to upload</div> : null}
        </>
      )}
      {pagination ? <Pagination {...pagination} /> : null}
    </div>
  );
}
