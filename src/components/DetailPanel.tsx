import { X } from "lucide-react";
import type { Entry } from "../types";
import { formatBytes, formatDate } from "../utils/format";

type DetailPanelProps = {
  showTrash: boolean;
  selected: Entry | null;
  canTextPreview: boolean;
  canImagePreview: boolean;
  error: string | null;
  share?: {
    status: "idle" | "checking" | "creating" | "ready" | "error";
    url: string | null;
    error: string | null;
  };
  onShareCreate?: () => void;
  onShareCopy?: () => void;
  onShareOpen?: () => void;
  onClose?: () => void;
};

export function DetailPanel({
  showTrash,
  selected,
  canTextPreview,
  canImagePreview,
  error,
  share,
  onShareCreate,
  onShareCopy,
  onShareOpen,
  onClose,
}: DetailPanelProps) {
  const showShare = Boolean(selected && selected.type === "file" && onShareCreate);
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
          {showShare ? (
            <div className="detail-share">
              <p className="label">Share</p>
              <p className="meta">Create a link to open this file in shared view.</p>
              <div className="share-actions">
                <button
                  className="primary"
                  onClick={onShareCreate}
                  disabled={share?.status === "creating" || share?.status === "checking"}
                >
                  {share?.status === "creating"
                    ? "Creating..."
                    : share?.status === "checking"
                      ? "Checking..."
                    : share?.url
                      ? "Regenerate link"
                      : "Create share link"}
                </button>
                {share?.url && onShareOpen ? (
                  <button className="ghost" onClick={onShareOpen}>
                    Open
                  </button>
                ) : null}
              </div>
              {share?.url ? (
                <div className="share-link-row">
                  <input className="share-link" type="text" readOnly value={share.url} />
                  <button className="ghost" onClick={onShareCopy} disabled={!onShareCopy}>
                    Copy
                  </button>
                </div>
              ) : null}
              {share?.error ? <p className="error">{share.error}</p> : null}
            </div>
          ) : null}
        </div>
      ) : (
        <div className="empty">Select a file to inspect it.</div>
      )}
      {error ? <p className="error">{error}</p> : null}
    </div>
  );
}
