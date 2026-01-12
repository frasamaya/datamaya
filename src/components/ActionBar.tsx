import {
  Archive,
  ClipboardPaste,
  Copy,
  Download,
  Eye,
  Image,
  MoveRight,
  Pencil,
  Trash2,
} from "lucide-react";
import type { MouseEvent } from "react";
import type { Entry } from "../types";

type ActionBarProps = {
  showTrash: boolean;
  selectionCount: number;
  trashCount: number;
  clipboardCount: number;
  actionLoading: boolean;
  canWrite: boolean;
  selected: Entry | null;
  previewLoading: boolean;
  canTextPreview: boolean;
  canImagePreview: boolean;
  downloadHref: string | null;
  archiveHref: string | null;
  onCopy: () => void;
  onPaste: () => void;
  onRename: () => void;
  onMove: () => void;
  onArchiveClick: () => void;
  onDelete: () => void;
  onPreview: () => void;
  onImagePreview: () => void;
};

export function ActionBar({
  showTrash,
  selectionCount,
  trashCount,
  clipboardCount,
  actionLoading,
  canWrite,
  selected,
  previewLoading,
  canTextPreview,
  canImagePreview,
  downloadHref,
  archiveHref,
  onCopy,
  onPaste,
  onRename,
  onMove,
  onArchiveClick,
  onDelete,
  onPreview,
  onImagePreview,
}: ActionBarProps) {
  const iconProps = {
    size: 16,
    strokeWidth: 1.8,
    "aria-hidden": true,
  } as const;
  const archiveDisabled = actionLoading || showTrash || selectionCount === 0 || !archiveHref;
  const handleArchiveClick = (event: MouseEvent<HTMLAnchorElement>) => {
    if (archiveDisabled) {
      event.preventDefault();
      return;
    }
    onArchiveClick();
  };

  return (
    <div className="card action-bar">
      <div>
        <p className="label">Actions</p>
        <p className="meta">
          {showTrash ? `${trashCount} item(s) in trash` : `${selectionCount} item(s) selected`}
        </p>
        {clipboardCount > 0 ? <p className="meta">Clipboard: {clipboardCount} item(s)</p> : null}
      </div>
      <div className="action-buttons">
        <button
          className="ghost"
          onClick={onCopy}
          disabled={actionLoading || showTrash || selectionCount === 0}
        >
          <Copy {...iconProps} />
          Copy
        </button>
        <button
          className="ghost"
          onClick={onPaste}
          disabled={actionLoading || showTrash || clipboardCount === 0 || !canWrite}
        >
          <ClipboardPaste {...iconProps} />
          Paste
        </button>
        <button
          className="ghost"
          onClick={onRename}
          disabled={actionLoading || showTrash || selectionCount !== 1 || !canWrite}
        >
          <Pencil {...iconProps} />
          Rename
        </button>
        <button
          className="ghost"
          onClick={onMove}
          disabled={actionLoading || showTrash || selectionCount === 0 || !canWrite}
        >
          <MoveRight {...iconProps} />
          Move
        </button>
        <a
          className={`ghost${archiveDisabled ? " is-disabled" : ""}`}
          href={archiveHref ?? "#"}
          onClick={handleArchiveClick}
          aria-disabled={archiveDisabled}
          tabIndex={archiveDisabled ? -1 : undefined}
        >
          <Archive {...iconProps} />
          Download Zip
        </a>
        <button
          className="danger"
          onClick={onDelete}
          disabled={actionLoading || showTrash || selectionCount === 0 || !canWrite}
        >
          <Trash2 {...iconProps} />
          Delete
        </button>
        {!showTrash && selected && selected.type === "file" ? (
          <>
            <span className="action-divider" aria-hidden="true" />
            {canTextPreview ? (
              <button className="ghost" onClick={onPreview} disabled={previewLoading}>
                <Eye {...iconProps} />
                {previewLoading ? "Loading..." : "Preview"}
              </button>
            ) : null}
            {canImagePreview ? (
              <button className="ghost" onClick={onImagePreview}>
                <Image {...iconProps} />
                Image Preview
              </button>
            ) : null}
            {downloadHref ? (
              <a className="ghost" href={downloadHref}>
                <Download {...iconProps} />
                Download
              </a>
            ) : null}
          </>
        ) : null}
      </div>
    </div>
  );
}
