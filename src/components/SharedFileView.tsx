import { useEffect, useMemo, useState } from "react";
import { BRAND_EYEBROW, BRAND_TITLE } from "../constants";
import type { SharedFile } from "../types";
import { formatBytes, formatDate } from "../utils/format";
import { getFileExtension } from "../utils/path";
import { getFileCategory } from "../utils/fileTypes";

type SharedFileViewProps = {
  token: string;
};

export function SharedFileView({ token }: SharedFileViewProps) {
  const [data, setData] = useState<SharedFile | null>(null);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const [preview, setPreview] = useState<string | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [previewError, setPreviewError] = useState<string | null>(null);

  const downloadUrl = useMemo(() => `/api/share/${token}/download`, [token]);
  const imageUrl = useMemo(() => `/api/share/${token}/image`, [token]);
  const previewUrl = useMemo(() => `/api/share/${token}/preview`, [token]);
  const fileUrl = useMemo(() => `/api/share/${token}/file`, [token]);
  const shareUrl = useMemo(() => {
    if (typeof window === "undefined") {
      return "";
    }
    return `${window.location.origin}/share/${token}`;
  }, [token]);

  const extension = useMemo(() => getFileExtension(data?.name ?? ""), [data?.name]);
  const category = useMemo(
    () => (data?.name ? getFileCategory(data.name) : "other"),
    [data?.name]
  );
  const canPdfPreview = extension === ".pdf";
  const canWordPreview = extension === ".doc" || extension === ".docx";
  const canAudioPreview = category === "audio";
  const canVideoPreview = category === "video";
  const canImagePreview = Boolean(data?.canImagePreview);

  useEffect(() => {
    let active = true;
    const fetchShare = async () => {
      setLoading(true);
      setError(null);
      try {
        const response = await fetch(`/api/share/${token}`);
        if (!response.ok) {
          const message = response.status === 404 ? "Share link not found." : "Unable to load share.";
          if (active) {
            setError(message);
          }
          return;
        }
        const payload = (await response.json()) as SharedFile;
        if (active) {
          setData(payload);
        }
      } catch {
        if (active) {
          setError("Unable to load share.");
        }
      } finally {
        if (active) {
          setLoading(false);
        }
      }
    };

    void fetchShare();
    return () => {
      active = false;
    };
  }, [token]);

  useEffect(() => {
    if (!data?.canTextPreview) {
      setPreview(null);
      setPreviewError(null);
      return;
    }
    let active = true;
    const fetchPreview = async () => {
      setPreviewLoading(true);
      setPreviewError(null);
      try {
        const response = await fetch(previewUrl);
        if (!response.ok) {
          const message = response.status === 413 ? "Preview too large." : "Preview unavailable.";
          if (active) {
            setPreviewError(message);
          }
          return;
        }
        const payload = (await response.json()) as { content?: string };
        if (active) {
          setPreview(payload.content ?? "");
        }
      } catch {
        if (active) {
          setPreviewError("Preview unavailable.");
        }
      } finally {
        if (active) {
          setPreviewLoading(false);
        }
      }
    };

    void fetchPreview();
    return () => {
      active = false;
    };
  }, [data?.canTextPreview, previewUrl]);

  return (
    <div className="shared-shell">
      <div className="shared-card card">
        <div className="shared-header">
          <div>
          {/* <p className="label">{BRAND_EYEBROW}</p> */}
          <h1 className="shared-title">{data?.name ?? "Shared file"}</h1>
          {/* <p className="shared-subtitle">{BRAND_TITLE} shared file view</p> */}
          {data ? (
            <div className="shared-meta">
              <p className="meta">{formatBytes(data.size)}</p>
              <p className="meta">Updated {formatDate(data.mtime)}</p>
            </div>
          ) : null}
          </div>
            <div className="shared-side">
              <div className="actions shared-actions">
                <a href={downloadUrl}>Download</a>
              </div>
            </div>
        </div>

        {loading ? <p className="meta">Loading shared file...</p> : null}
        {error ? <p className="error">{error}</p> : null}

        {data && !loading && !error ? (
          <div className="shared-body">

            {canImagePreview ? (
              <div className="shared-preview-block">
                <p className="label">Image preview</p>
                <img className="shared-image" src={imageUrl} alt={data.name} />
              </div>
            ) : null}

            {canPdfPreview ? (
              <div className="shared-preview-block">
                <p className="label">PDF preview</p>
                <iframe className="shared-doc" src={fileUrl} title={`${data.name} preview`} />
              </div>
            ) : null}

            {canVideoPreview ? (
              <div className="shared-preview-block">
                <p className="label">Video preview</p>
                <video className="shared-media" src={fileUrl} controls preload="metadata" />
              </div>
            ) : null}

            {canAudioPreview ? (
              <div className="shared-preview-block">
                <p className="label">Audio preview</p>
                <audio className="shared-media" src={fileUrl} controls preload="metadata" />
              </div>
            ) : null}

            {canWordPreview ? (
              <div className="shared-preview-block">
                <p className="label">Word preview</p>
                {shareUrl.includes("localhost") ? (
                  <p className="meta">Word preview requires public access. Use download instead.</p>
                ) : (
                  <iframe
                    className="shared-doc"
                    src={`https://view.officeapps.live.com/op/embed.aspx?src=${encodeURIComponent(
                      shareUrl
                    )}`}
                    title={`${data.name} preview`}
                  />
                )}
              </div>
            ) : null}

            {data.canTextPreview ? (
              <div className="shared-preview-block">
                <p className="label">Text preview</p>
                {previewLoading ? <p className="meta">Loading preview...</p> : null}
                {previewError ? <p className="error">{previewError}</p> : null}
                {preview !== null && !previewLoading ? (
                  <pre className="shared-preview">{preview}</pre>
                ) : null}
              </div>
            ) : null}
          </div>
        ) : null}
      </div>
    </div>
  );
}
