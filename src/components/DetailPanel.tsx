import { X } from "lucide-react";
import type { Entry } from "../types";
import { formatBytes, formatDate } from "../utils/format";

type DetailPanelProps = {
  showTrash: boolean;
  selected: Entry | null;
  canTextPreview: boolean;
  canImagePreview: boolean;
  error: string | null;
  onClose?: () => void;
};

export function DetailPanel({
  showTrash,
  selected,
  canTextPreview,
  canImagePreview,
  error,
  onClose,
}: DetailPanelProps) {
  return (
    <div className="card detail">
      <div className="detail-header">
        <p className="label">Selection</p>
        {onClose ? (
          <button className="ghost detail-close" onClick={onClose} aria-label="Close details">
            <X size={16} strokeWidth={1.8} aria-hidden="true" />
          </button>
        ) : null}
      </div>
      {showTrash ? (
        <div className="empty">Restore items from trash to bring them back.</div>
      ) : selected ? (
        <div className="detail-body">
          <h3>{selected.name}</h3>
          <p className="meta">
            {selected.type === "file" ? formatBytes(selected.size) : "Folder"}
          </p>
          <p className="meta">Updated {formatDate(selected.mtime)}</p>
          {selected.type === "file" ? (
            <p className="hint">
              {canTextPreview
                ? "Use Preview in Actions to open the popup."
                : canImagePreview
                  ? "Use Image Preview in Actions to pop out the image."
                  : "Preview available for .txt, .php, .js, .html only."}
            </p>
          ) : null}
        </div>
      ) : (
        <div className="empty">Select a file to inspect it.</div>
      )}
      {error ? <p className="error">{error}</p> : null}
    </div>
  );
}
