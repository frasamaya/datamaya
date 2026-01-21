import { Home } from "lucide-react";
import {
  useCallback,
  useEffect,
  useMemo,
  useRef,
  useState,
  type DragEvent,
  type ChangeEvent,
  type FormEvent,
} from "react";

import { Header } from "./components/Header";
import { DetailPanel } from "./components/DetailPanel";
import { EditorModal } from "./components/EditorModal";
import { ImagePreviewModal } from "./components/ImagePreviewModal";
import { LandingPage } from "./components/LandingPage";
import { LoginForm } from "./components/LoginForm";
import { FileList } from "./components/FileList";
import { SharedFileView } from "./components/SharedFileView";
import { TextPreviewModal } from "./components/TextPreviewModal";
import { Toasts } from "./components/Toasts";
import { Toolbar } from "./components/Toolbar";
import {
  API_BASE,
  DATE_RANGE_MS,
  DEFAULT_PAGE_SIZE,
  LAST_PATH_STORAGE_KEY,
  PAGE_SIZE_OPTIONS,
  SHORTCUTS_ENABLED,
  DEFAULT_VIEW_MODE,
  VIEW_MODE_STORAGE_KEY,
} from "./constants";
import { useContentSearch } from "./hooks/useContentSearch";
import { useKeyboardShortcuts } from "./hooks/useKeyboardShortcuts";
import { useTheme } from "./hooks/useTheme";
import { useToasts } from "./hooks/useToasts";
import { apiFetch, readJson } from "./services/api";
import type {
  AuthState,
  Breadcrumb,
  ClipboardItem,
  DateFilter,
  EditorFile,
  Entry,
  ListResponse,
  Preview,
  SortMode,
  StorageStats,
  TrashItem,
  TrashResponse,
  TypeFilter,
  UserRole,
  ViewMode,
} from "./types";
import { parseSizeInput } from "./utils/filters";
import { formatBytes } from "./utils/format";
import {
  getFileCategory,
  isImagePreviewable,
  isTextEditableName,
  isTextPreviewableName,
  matchesTypeFilter,
} from "./utils/fileTypes";
import { joinPath, normalizeInputPath } from "./utils/path";
import { sortEntries } from "./utils/sort";

function getStoredPath() {
  if (typeof window === "undefined") {
    return null;
  }
  const stored = window.localStorage.getItem(LAST_PATH_STORAGE_KEY);
  if (!stored || !stored.startsWith("/")) {
    return null;
  }
  return stored;
}

function setStoredPath(value: string) {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(LAST_PATH_STORAGE_KEY, value);
  } catch {}
}

function clearStoredPath() {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.removeItem(LAST_PATH_STORAGE_KEY);
  } catch {}
}

function getStoredViewMode(): ViewMode {
  if (typeof window === "undefined") {
    return DEFAULT_VIEW_MODE;
  }
  const stored = window.localStorage.getItem(VIEW_MODE_STORAGE_KEY);
  if (stored === "list" || stored === "grid") {
    return stored;
  }
  return DEFAULT_VIEW_MODE;
}

function setStoredViewMode(value: ViewMode) {
  if (typeof window === "undefined") {
    return;
  }
  try {
    window.localStorage.setItem(VIEW_MODE_STORAGE_KEY, value);
  } catch {}
}

function createUploadId() {
  if (typeof crypto !== "undefined" && "randomUUID" in crypto) {
    return crypto.randomUUID();
  }
  return `${Date.now()}-${Math.random().toString(16).slice(2)}`;
}

async function uploadChunkWithProgress(
  form: FormData,
  totalSize: number,
  onProgress: (value: { loaded: number; total: number; percent: number } | null) => void
) {
  return await new Promise<Response>((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("POST", `${API_BASE}/upload/chunk`);
    xhr.withCredentials = true;

    xhr.upload.onprogress = (event) => {
      const total = event.total || totalSize || 0;
      const loaded = event.loaded || 0;
      const percent = total > 0 ? Math.round((loaded / total) * 100) : 0;
      onProgress({ loaded, total, percent });
    };

    xhr.onload = () => {
      onProgress(null);
      const response = new Response(xhr.responseText, { status: xhr.status });
      resolve(response);
    };

    xhr.onerror = () => {
      onProgress(null);
      reject(new Error("Upload failed."));
    };

    xhr.send(form);
  });
}

export default function App() {
  const [auth, setAuth] = useState<AuthState>("unknown");
  const [path, setPath] = useState("/");
  const [parent, setParent] = useState<string | null>(null);
  const [entries, setEntries] = useState<Entry[]>([]);
  const [selected, setSelected] = useState<Entry | null>(null);
  const [preview, setPreview] = useState<Preview | null>(null);
  const [editorFile, setEditorFile] = useState<EditorFile | null>(null);
  const [editorOpen, setEditorOpen] = useState(false);
  const [editorLoading, setEditorLoading] = useState(false);
  const [editorSaving, setEditorSaving] = useState(false);
  const [editorContent, setEditorContent] = useState("");
  const [editorInitialContent, setEditorInitialContent] = useState("");
  const [pendingEditorPath, setPendingEditorPath] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [actionLoading, setActionLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [query, setQuery] = useState("");
  const [password, setPassword] = useState("");
  const [loginUsername, setLoginUsername] = useState("");
  const [username, setUsername] = useState("");
  const [userRole, setUserRole] = useState<UserRole>("read-write");
  const [selectedNames, setSelectedNames] = useState<string[]>([]);
  const [clipboard, setClipboard] = useState<ClipboardItem[] | null>(null);
  const [showTrash, setShowTrash] = useState(false);
  const [trashItems, setTrashItems] = useState<TrashItem[]>([]);
  const [dragActive, setDragActive] = useState(false);
  const [typeFilter, setTypeFilter] = useState<TypeFilter>("all");
  const [sizeMinMb, setSizeMinMb] = useState("");
  const [sizeMaxMb, setSizeMaxMb] = useState("");
  const [dateFilter, setDateFilter] = useState<DateFilter>("any");
  const [sortMode, setSortMode] = useState<SortMode>("default");
  const [contentSearch, setContentSearch] = useState(false);
  const [filtersOpen, setFiltersOpen] = useState(false);
  const [page, setPage] = useState(1);
  const [pageSize, setPageSize] = useState(DEFAULT_PAGE_SIZE);
  const [imagePreviewPath, setImagePreviewPath] = useState<string | null>(null);
  const [imagePreviewName, setImagePreviewName] = useState<string | null>(null);
  const [textPreviewOpen, setTextPreviewOpen] = useState(false);
  const [viewMode, setViewMode] = useState<ViewMode>(() => getStoredViewMode());
  const [storageStats, setStorageStats] = useState<StorageStats | null>(null);
  const [uploadJobs, setUploadJobs] = useState<
    Array<{
      id: string;
      name: string;
      loaded: number;
      total: number;
      percent: number;
      status: "uploading" | "done" | "error";
    }>
  >([]);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const dragDepth = useRef(0);
  const storageStatsRef = useRef<StorageStats | null>(null);
  const menuMetricsRef = useRef<{
    all: { count: number; bytes: number };
    recent: { count: number; bytes: number };
    docs: { count: number; bytes: number };
    photos: { count: number; bytes: number };
    audio: { count: number; bytes: number };
    video: { count: number; bytes: number };
    archive: { count: number; bytes: number };
    trash: { count: number; bytes: number };
  } | null>(null);

  const shareToken = useMemo(() => {
    if (typeof window === "undefined") {
      return null;
    }
    const match = window.location.pathname.match(/^\/share\/([^/]+)\/?$/);
    return match ? match[1] : null;
  }, []);
  const [shareLinks, setShareLinks] = useState<Record<string, string>>({});
  const [shareLoading, setShareLoading] = useState(false);
  const [shareLookupLoading, setShareLookupLoading] = useState(false);
  const [shareError, setShareError] = useState<string | null>(null);
  const [sidebarOpen, setSidebarOpen] = useState(false);
  const [isMobile, setIsMobile] = useState(false);
  const isLoginRoute = useMemo(() => {
    if (typeof window === "undefined") {
      return false;
    }
    return window.location.pathname === "/login";
  }, []);

  const [theme, setTheme] = useTheme();
  const { toasts, pushToast } = useToasts();

  const handleUnauthorized = useCallback(() => {
    setAuth("logged_out");
  }, []);

  const notifyError = useCallback(
    (message: string) => {
      setError(message);
      pushToast(message, "error");
    },
    [pushToast]
  );

  const {
    matches: contentMatches,
    loading: contentLoading,
    reset: resetContentSearch,
  } = useContentSearch({
    enabled: contentSearch,
    query,
    path,
    showTrash,
    onUnauthorized: handleUnauthorized,
    onError: notifyError,
  });

  const loadPath = useCallback(async (targetPath: string) => {
    setLoading(true);
    setError(null);
    setSelected(null);
    setPreview(null);
    setTextPreviewOpen(false);
    setImagePreviewPath(null);
    setImagePreviewName(null);
    setSelectedNames([]);
    setShowTrash(false);
    setDragActive(false);

    const response = await apiFetch(`/list?path=${encodeURIComponent(targetPath)}`);
    if (response.status === 401) {
      setAuth("logged_out");
      setLoading(false);
      return false;
    }

    if (!response.ok) {
      const data = await readJson(response);
      setError(data?.error ?? "Failed to load directory.");
      setLoading(false);
      return false;
    }

    const data = (await response.json()) as ListResponse;
    setEntries(data.entries);
    setPath(data.path);
    setParent(data.parent);
    setUsername(data.user);
    setUserRole(data.role);
    setAuth("authed");
    setLoading(false);
    setStoredPath(data.path);
    return true;
  }, []);

  const loadTrash = useCallback(async () => {
    setLoading(true);
    setError(null);
    setSelected(null);
    setPreview(null);
    setTextPreviewOpen(false);
    setImagePreviewPath(null);
    setImagePreviewName(null);
    setSelectedNames([]);

    const response = await apiFetch("/trash");
    if (response.status === 401) {
      setAuth("logged_out");
      setLoading(false);
      return;
    }

    if (!response.ok) {
      const data = await readJson(response);
      setError(data?.error ?? "Failed to load trash.");
      setLoading(false);
      return;
    }

    const data = (await response.json()) as TrashResponse;
    setTrashItems(data.items ?? []);
    setUsername(data.user);
    setUserRole(data.role);
    setShowTrash(true);
    setAuth("authed");
    setLoading(false);
  }, []);

  const handleLogin = useCallback(
    async (event: FormEvent) => {
      event.preventDefault();
      setError(null);

      const response = await apiFetch("/login", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username: loginUsername.trim(), password }),
      });

      if (!response.ok) {
        const data = await readJson(response);
        notifyError(data?.error ?? "Login failed.");
        return;
      }

      setPassword("");
      setAuth("authed");
      pushToast("Signed in.", "success");
      loadPath("/");
    },
    [password, loginUsername, loadPath, notifyError, pushToast]
  );

  const resetEditorState = useCallback(() => {
    setEditorOpen(false);
    setEditorFile(null);
    setEditorContent("");
    setEditorInitialContent("");
    setEditorLoading(false);
    setEditorSaving(false);
  }, []);

  const handleLogout = useCallback(async () => {
    await apiFetch("/logout", { method: "POST" });
    setAuth("logged_out");
    setEntries([]);
    setSelected(null);
    setPreview(null);
    setTextPreviewOpen(false);
    setImagePreviewPath(null);
    setImagePreviewName(null);
    resetEditorState();
    setSelectedNames([]);
    setClipboard(null);
    setShowTrash(false);
    setTrashItems([]);
    setUsername("");
    setUserRole("read-write");
    pushToast("Signed out.", "info");
  }, [pushToast, resetEditorState]);

  const openEditorByPath = useCallback(
    async (targetPath: string, targetName?: string) => {
      if (editorOpen && editorContent !== editorInitialContent) {
        const confirmClose = window.confirm("Discard unsaved changes?");
        if (!confirmClose) {
          return;
        }
      }

      setEditorLoading(true);
      setError(null);
      setEditorOpen(true);
      setEditorFile({
        name: targetName ?? targetPath.split("/").filter(Boolean).pop() ?? "untitled",
        path: targetPath,
        content: "",
        size: 0,
        mtime: Date.now(),
      });

      const response = await apiFetch(`/edit?path=${encodeURIComponent(targetPath)}`);

      if (!response.ok) {
        const data = await readJson(response);
        notifyError(data?.error ?? "Failed to open editor.");
        setEditorLoading(false);
        return;
      }

      const data = (await response.json()) as EditorFile;
      setEditorFile(data);
      setEditorContent(data.content);
      setEditorInitialContent(data.content);
      setEditorLoading(false);
    },
    [editorOpen, editorContent, editorInitialContent, notifyError]
  );

  const handleEntryClick = useCallback(
    (entry: Entry) => {
      if (entry.type === "dir") {
        loadPath(joinPath(path, entry.name));
      } else {
        setSelected(entry);
        setPreview(null);
        setTextPreviewOpen(false);
        setImagePreviewPath(null);
        setImagePreviewName(null);
      }
    },
    [path, loadPath]
  );

  const openPreviewForEntry = useCallback(
    async (entry: Entry) => {
      if (entry.type !== "file") {
        return;
      }
      if (isImagePreviewable(entry.name)) {
        setImagePreviewPath(joinPath(path, entry.name));
        setImagePreviewName(entry.name);
        return;
      }
      if (!isTextPreviewableName(entry.name)) {
        notifyError("Preview not available for this file type.");
        return;
      }

      setPreviewLoading(true);
      setError(null);

      const response = await apiFetch(
        `/preview?path=${encodeURIComponent(joinPath(path, entry.name))}`
      );

      if (!response.ok) {
        const data = await readJson(response);
        notifyError(data?.error ?? "Preview failed.");
        setPreviewLoading(false);
        return;
      }

      const data = (await response.json()) as Preview;
      setPreview(data);
      setTextPreviewOpen(true);
      setPreviewLoading(false);
    },
    [path, notifyError]
  );

  const handleEntryDoubleClick = useCallback(
    (entry: Entry) => {
      if (entry.type !== "file") {
        return;
      }
      setSelected(entry);
      setPreview(null);
      setTextPreviewOpen(false);
      setImagePreviewPath(null);
      setImagePreviewName(null);
      void openPreviewForEntry(entry);
    },
    [openPreviewForEntry]
  );

  const handlePreview = useCallback(async () => {
    if (!selected || selected.type !== "file") {
      return;
    }
    if (!isTextPreviewableName(selected.name)) {
      notifyError("Preview available for .txt, .php, .js, .html only.");
      return;
    }

    setPreviewLoading(true);
    setError(null);

    const response = await apiFetch(
      `/preview?path=${encodeURIComponent(joinPath(path, selected.name))}`
    );

    if (!response.ok) {
      const data = await readJson(response);
      notifyError(data?.error ?? "Preview failed.");
      setPreviewLoading(false);
      return;
    }

    const data = (await response.json()) as Preview;
    setPreview(data);
    setTextPreviewOpen(true);
    setPreviewLoading(false);
  }, [path, selected, notifyError]);

  const handleImagePreview = useCallback(() => {
    if (!selected || selected.type !== "file") {
      return;
    }
    if (!isImagePreviewable(selected.name)) {
      notifyError("Image preview not available for this file type.");
      return;
    }
    setImagePreviewPath(joinPath(path, selected.name));
    setImagePreviewName(selected.name);
  }, [path, selected, notifyError]);

  const closeImagePreview = useCallback(() => {
    setImagePreviewPath(null);
    setImagePreviewName(null);
  }, []);

  const closeTextPreview = useCallback(() => {
    setPreview(null);
    setTextPreviewOpen(false);
  }, []);

  const selectedEntries = useMemo(() => {
    return entries.filter((entry) => selectedNames.includes(entry.name));
  }, [entries, selectedNames]);

  const selectionTargets = selectedEntries;
  const selectionCount = selectedEntries.length;
  const canWrite = userRole !== "read-only";
  const editTarget = selectionTargets.length === 1 ? selectionTargets[0] : null;
  const selectedPath = useMemo(
    () => (selected ? joinPath(path, selected.name) : null),
    [path, selected]
  );
  const shareLink = selectedPath ? shareLinks[selectedPath] ?? null : null;
  const canEditTarget =
    !showTrash &&
    editTarget?.type === "file" &&
    isTextEditableName(editTarget.name);
  const editDisabled =
    actionLoading || editorLoading || editorSaving || !canWrite || !canEditTarget;

  const refreshView = useCallback(async () => {
    if (showTrash) {
      await loadTrash();
    } else {
      await loadPath(path);
    }
  }, [showTrash, loadTrash, loadPath, path]);

  const closeEditor = useCallback(() => {
    if (editorOpen && editorContent !== editorInitialContent) {
      const confirmClose = window.confirm("Discard unsaved changes?");
      if (!confirmClose) {
        return;
      }
    }
    resetEditorState();
  }, [editorOpen, editorContent, editorInitialContent, resetEditorState]);

  const handleOpenEditor = useCallback(async () => {
    if (!editTarget || editTarget.type !== "file") {
      notifyError("Select a single file to edit.");
      return;
    }
    if (!isTextEditableName(editTarget.name)) {
      notifyError("Editor supports common web file types only.");
      return;
    }

    await openEditorByPath(joinPath(path, editTarget.name), editTarget.name);
  }, [editTarget, path, notifyError, openEditorByPath]);

  const openEditorInNewTab = useCallback(() => {
    if (!editorFile || typeof window === "undefined") {
      return;
    }
    const url = new URL(window.location.href);
    url.searchParams.set("edit", editorFile.path);
    const next = window.open(url.toString(), "_blank", "noopener,noreferrer");
    if (next) {
      next.opener = null;
    }
  }, [editorFile]);

  const handleSaveEditor = useCallback(async () => {
    if (!editorFile) {
      return;
    }
    if (!canWrite) {
      notifyError("Read-only account.");
      return;
    }

    setEditorSaving(true);
    setError(null);

    const response = await apiFetch("/edit", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path: editorFile.path, content: editorContent }),
    });

    if (!response.ok) {
      const data = await readJson(response);
      notifyError(data?.error ?? "Save failed.");
      setEditorSaving(false);
      return;
    }

    setEditorInitialContent(editorContent);
    setEditorSaving(false);
    pushToast("File saved.", "success");
    await refreshView();
  }, [editorFile, editorContent, canWrite, notifyError, pushToast, refreshView]);

  const handleImageError = useCallback(() => {
    notifyError("Image preview failed to load.");
    closeImagePreview();
  }, [notifyError, closeImagePreview]);

  const handleClearSelection = useCallback(() => {
    setSelectedNames([]);
    setSelected(null);
    setPreview(null);
    setTextPreviewOpen(false);
    setImagePreviewPath(null);
    setImagePreviewName(null);
  }, []);

  const requireWrite = useCallback(() => {
    if (!canWrite) {
      notifyError("Read-only account.");
      return false;
    }
    return true;
  }, [canWrite, notifyError]);

  const toggleSelect = useCallback((entry: Entry) => {
    setSelectedNames((prev) =>
      prev.includes(entry.name) ? prev.filter((name) => name !== entry.name) : [...prev, entry.name]
    );
  }, []);

  const handleUploadClick = useCallback(() => {
    if (showTrash || !requireWrite()) {
      return;
    }
    fileInputRef.current?.click();
  }, [showTrash, requireWrite]);

  const uploadFiles = useCallback(
    async (files: File[], overwrite = false) => {
      if (!canWrite) {
        notifyError("Read-only account.");
        return;
      }
      if (files.length === 0) {
        return;
      }
      setActionLoading(true);
      setError(null);

      const startJob = (file: File) => {
        const id = createUploadId();
        setUploadJobs((prev) => [
          ...prev,
          {
            id,
            name: file.name,
            loaded: 0,
            total: file.size,
            percent: 0,
            status: "uploading",
          },
        ]);
        return id;
      };

      const updateJob = (
        id: string,
        progress: { loaded: number; total: number; percent: number }
      ) => {
        setUploadJobs((prev) =>
          prev.map((job) => (job.id === id ? { ...job, ...progress } : job))
        );
      };

      const finalizeJob = (id: string, status: "done" | "error") => {
        setUploadJobs((prev) =>
          prev.map((job) =>
            job.id === id ? { ...job, status, percent: status === "done" ? 100 : job.percent } : job
          )
        );
        window.setTimeout(() => {
          setUploadJobs((prev) => prev.filter((job) => job.id !== id));
        }, status === "done" ? 1500 : 4000);
      };

      const CHUNK_SIZE = 5 * 1024 * 1024;
      const MAX_CONCURRENCY = 5;
      let uploadedAny = false;

      for (const file of files) {
        const id = startJob(file);
        const totalChunks = Math.max(1, Math.ceil(file.size / CHUNK_SIZE));

        let initResponse: Response;
        try {
          initResponse = await apiFetch("/upload/init", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({
              path,
              name: file.name,
              size: file.size,
              totalChunks,
              overwrite,
            }),
          });
        } catch {
          finalizeJob(id, "error");
          notifyError(`Upload failed for ${file.name}.`);
          continue;
        }

        if (initResponse.status === 409 && !overwrite) {
          const data = await readJson(initResponse);
          finalizeJob(id, "error");
          if (window.confirm(`${data?.error ?? "File exists."} Overwrite?`)) {
            await uploadFiles([file], true);
          }
          continue;
        }

        if (!initResponse.ok) {
          const data = await readJson(initResponse);
          finalizeJob(id, "error");
          notifyError(data?.error ?? `Upload failed for ${file.name}.`);
          continue;
        }

        const initData = (await initResponse.json()) as { uploadId: string; key?: string };
        const statusParams = new URLSearchParams({ uploadId: initData.uploadId });
        if (initData.key) {
          statusParams.set("key", initData.key);
        }
        let uploadedParts: number[] = [];
        try {
          const statusResponse = await apiFetch(`/upload/status?${statusParams.toString()}`);
          if (statusResponse.ok) {
            const data = (await statusResponse.json()) as { uploadedParts?: number[] };
            uploadedParts = data.uploadedParts ?? [];
          }
        } catch {}

        const uploadedSet = new Set(uploadedParts);
        const getPartSize = (part: number) => {
          const start = (part - 1) * CHUNK_SIZE;
          return Math.min(CHUNK_SIZE, Math.max(0, file.size - start));
        };
        let uploadedBytes = uploadedParts.reduce((sum, part) => sum + getPartSize(part), 0);
        updateJob(id, {
          loaded: uploadedBytes,
          total: file.size,
          percent: file.size > 0 ? Math.round((uploadedBytes / file.size) * 100) : 0,
        });

        let failed = false;
        const inflight = new Map<number, number>();
        const updateAggregatedProgress = () => {
          let inflightLoaded = 0;
          for (const value of inflight.values()) {
            inflightLoaded += value;
          }
          const loaded = Math.min(file.size, uploadedBytes + inflightLoaded);
          updateJob(id, {
            loaded,
            total: file.size,
            percent: file.size > 0 ? Math.round((loaded / file.size) * 100) : 0,
          });
        };

        const partsToUpload: number[] = [];
        for (let part = 1; part <= totalChunks; part += 1) {
          if (!uploadedSet.has(part)) {
            partsToUpload.push(part);
          }
        }

        const uploadPart = async (part: number) => {
          const start = (part - 1) * CHUNK_SIZE;
          const end = Math.min(start + CHUNK_SIZE, file.size);
          const chunk = file.slice(start, end);
          const form = new FormData();
          form.set("uploadId", initData.uploadId);
          form.set("partNumber", String(part));
          form.set("totalChunks", String(totalChunks));
          if (initData.key) {
            form.set("key", initData.key);
          }
          form.append("chunk", chunk, file.name);

          inflight.set(part, 0);
          updateAggregatedProgress();
          try {
            const chunkResponse = await uploadChunkWithProgress(form, chunk.size, (progress) => {
              if (!progress) {
                return;
              }
              inflight.set(part, progress.loaded);
              updateAggregatedProgress();
            });
            inflight.delete(part);
            if (!chunkResponse.ok) {
              throw new Error("Chunk failed");
            }
            uploadedBytes += chunk.size;
            updateAggregatedProgress();
          } catch {
            inflight.delete(part);
            updateAggregatedProgress();
            throw new Error("Chunk failed");
          }
        };

        let cursor = 0;
        const worker = async () => {
          while (cursor < partsToUpload.length && !failed) {
            const part = partsToUpload[cursor];
            cursor += 1;
            try {
              await uploadPart(part);
            } catch {
              failed = true;
            }
          }
        };

        await Promise.allSettled(
          Array.from({ length: Math.min(MAX_CONCURRENCY, partsToUpload.length) }, () => worker())
        );

        if (failed) {
          finalizeJob(id, "error");
          notifyError(`Upload failed for ${file.name}.`);
          continue;
        }

        const completeResponse = await apiFetch("/upload/complete", {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            uploadId: initData.uploadId,
            key: initData.key,
            totalChunks,
          }),
        });

        if (!completeResponse.ok) {
          const data = await readJson(completeResponse);
          finalizeJob(id, "error");
          notifyError(data?.error ?? `Upload failed for ${file.name}.`);
          continue;
        }

        finalizeJob(id, "done");
        uploadedAny = true;
      }

      if (uploadedAny) {
        await refreshView();
      }
      setActionLoading(false);
      if (uploadedAny) {
        pushToast(`Uploaded ${files.length} file(s).`, "success");
      }
    },
    [path, refreshView, canWrite, notifyError, pushToast]
  );

  const handleUploadChange = useCallback(
    async (event: ChangeEvent<HTMLInputElement>) => {
      const files = event.target.files ? Array.from(event.target.files) : [];
      await uploadFiles(files);
      event.target.value = "";
    },
    [uploadFiles]
  );

  const handleCreateFolder = useCallback(async () => {
    if (!requireWrite()) {
      return;
    }
    const name = window.prompt("New folder name");
    if (!name) {
      return;
    }
    setActionLoading(true);
    setError(null);

    const response = await apiFetch("/mkdir", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path, name }),
    });

    if (!response.ok) {
      const data = await readJson(response);
      notifyError(data?.error ?? "Failed to create folder.");
      setActionLoading(false);
      return;
    }

    await refreshView();
    setActionLoading(false);
    pushToast("Folder created.", "success");
  }, [path, refreshView, requireWrite, notifyError, pushToast]);

  const handleRename = useCallback(async () => {
    if (!requireWrite()) {
      return;
    }
    const renameTarget = selectionTargets.length === 1 ? selectionTargets[0] : null;
    if (!renameTarget) {
      notifyError("Select a single item to rename.");
      return;
    }
    const name = window.prompt("Rename to", renameTarget.name);
    if (!name || name === renameTarget.name) {
      return;
    }
    setActionLoading(true);
    setError(null);

    const response = await apiFetch("/move", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({
        from: joinPath(path, renameTarget.name),
        to: joinPath(path, name),
      }),
    });

    if (!response.ok) {
      const data = await readJson(response);
      notifyError(data?.error ?? "Rename failed.");
      setActionLoading(false);
      return;
    }

    await refreshView();
    setActionLoading(false);
    pushToast("Item renamed.", "success");
  }, [path, selectionTargets, refreshView, requireWrite, notifyError, pushToast]);

  const handleMove = useCallback(async () => {
    if (!requireWrite()) {
      return;
    }
    if (selectionTargets.length === 0) {
      notifyError("Select items to move.");
      return;
    }
    const destination = window.prompt("Move to folder (absolute or relative path)", path);
    if (!destination) {
      return;
    }
    const targetFolder = normalizeInputPath(destination, path);

    setActionLoading(true);
    setError(null);

    for (const entry of selectionTargets) {
      const response = await apiFetch("/move", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          from: joinPath(path, entry.name),
          to: joinPath(targetFolder, entry.name),
        }),
      });

      if (!response.ok) {
        const data = await readJson(response);
        notifyError(data?.error ?? "Move failed.");
        setActionLoading(false);
        return;
      }
    }

    await refreshView();
    setActionLoading(false);
    pushToast(`Moved ${selectionTargets.length} item(s).`, "success");
  }, [path, selectionTargets, refreshView, requireWrite, notifyError, pushToast]);

  const handleCopy = useCallback(() => {
    if (selectionTargets.length === 0) {
      notifyError("Select items to copy.");
      return;
    }
    const items = selectionTargets.map((entry) => ({
      name: entry.name,
      path: joinPath(path, entry.name),
    }));
    setClipboard(items);
    pushToast(`Copied ${items.length} item(s).`, "info");
  }, [path, selectionTargets, notifyError, pushToast]);

  const handlePaste = useCallback(async () => {
    if (!requireWrite()) {
      return;
    }
    if (!clipboard || clipboard.length === 0) {
      notifyError("Clipboard is empty.");
      return;
    }
    setActionLoading(true);
    setError(null);

    for (const item of clipboard) {
      const response = await apiFetch("/copy", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({
          from: item.path,
          to: joinPath(path, item.name),
        }),
      });

      if (!response.ok) {
        const data = await readJson(response);
        notifyError(data?.error ?? "Copy failed.");
        setActionLoading(false);
        return;
      }
    }

    await refreshView();
    setActionLoading(false);
    pushToast(`Pasted ${clipboard.length} item(s).`, "success");
  }, [clipboard, path, refreshView, requireWrite, notifyError, pushToast]);

  const handleDelete = useCallback(async () => {
    if (!requireWrite()) {
      return;
    }
    if (selectionTargets.length === 0) {
      notifyError("Select items to delete.");
      return;
    }
    const confirmation = window.confirm(`Move ${selectionTargets.length} item(s) to Trash?`);
    if (!confirmation) {
      return;
    }
    setActionLoading(true);
    setError(null);

    for (const entry of selectionTargets) {
      const response = await apiFetch("/trash", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ path: joinPath(path, entry.name) }),
      });

      if (!response.ok) {
        const data = await readJson(response);
        notifyError(data?.error ?? "Delete failed.");
        setActionLoading(false);
        return;
      }
    }

    await refreshView();
    setActionLoading(false);
    pushToast(`Moved ${selectionTargets.length} item(s) to trash.`, "success");
  }, [path, selectionTargets, refreshView, requireWrite, notifyError, pushToast]);

  const archiveHref = useMemo(() => {
    if (selectionTargets.length === 0) {
      return null;
    }
    const params = new URLSearchParams();
    for (const entry of selectionTargets) {
      params.append("path", joinPath(path, entry.name));
    }
    params.set("format", "zip");
    return `${API_BASE}/archive?${params.toString()}`;
  }, [path, selectionTargets]);

  const handleArchiveClick = useCallback(() => {
    if (selectionTargets.length === 0) {
      notifyError("Select items to zip.");
      return;
    }
    pushToast("Zip download started.", "info");
  }, [selectionTargets, notifyError, pushToast]);

  const handleShareCreate = useCallback(async () => {
    if (!selected || selected.type !== "file" || !selectedPath) {
      notifyError("Select a file to share.");
      return;
    }
    setShareLoading(true);
    setShareError(null);

    const response = await apiFetch("/share", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ path: selectedPath, force: Boolean(shareLink) }),
    });

    if (response.status === 401) {
      setAuth("logged_out");
      setShareLoading(false);
      return;
    }

    if (!response.ok) {
      const data = await readJson(response);
      const message = data?.error ?? "Failed to create share link.";
      setShareError(message);
      notifyError(message);
      setShareLoading(false);
      return;
    }

    const data = (await response.json()) as { token?: string };
    if (!data?.token) {
      const message = "Share link unavailable.";
      setShareError(message);
      notifyError(message);
      setShareLoading(false);
      return;
    }

    const origin = typeof window === "undefined" ? "" : window.location.origin;
    setShareLinks((prev) => ({
      ...prev,
      [selectedPath]: `${origin}/share/${data.token}`,
    }));
    setShareLoading(false);
    pushToast("Share link ready.", "success");
  }, [selected, selectedPath, shareLink, notifyError, pushToast, setAuth]);

  const handleShareCopy = useCallback(async () => {
    if (!shareLink) {
      return;
    }
    if (!navigator.clipboard) {
      window.prompt("Share link", shareLink);
      return;
    }
    try {
      await navigator.clipboard.writeText(shareLink);
      pushToast("Share link copied.", "success");
    } catch {
      notifyError("Unable to copy share link.");
    }
  }, [shareLink, notifyError, pushToast]);

  const handleShareOpen = useCallback(() => {
    if (!shareLink) {
      return;
    }
    window.open(shareLink, "_blank", "noopener,noreferrer");
  }, [shareLink]);

  const handleRestore = useCallback(
    async (item: TrashItem) => {
      if (!requireWrite()) {
        return;
      }
      setActionLoading(true);
      setError(null);
      const response = await apiFetch("/trash/restore", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ id: item.id }),
      });

      if (!response.ok) {
        const data = await readJson(response);
        notifyError(data?.error ?? "Restore failed.");
        setActionLoading(false);
        return;
      }

      await loadTrash();
      setActionLoading(false);
      pushToast(`Restored ${item.name}.`, "success");
    },
    [loadTrash, requireWrite, notifyError, pushToast]
  );

  const handleToggleTrash = useCallback(() => {
    if (showTrash) {
      loadPath(path);
    } else {
      loadTrash();
    }
  }, [showTrash, loadPath, loadTrash, path]);

  const handleDragEnter = useCallback(
    (event: DragEvent<HTMLDivElement>) => {
      if (showTrash || !canWrite) {
        return;
      }
      event.preventDefault();
      dragDepth.current += 1;
      setDragActive(true);
    },
    [showTrash, canWrite]
  );

  const handleDragOver = useCallback(
    (event: DragEvent<HTMLDivElement>) => {
      if (showTrash || !canWrite) {
        return;
      }
      event.preventDefault();
    },
    [showTrash, canWrite]
  );

  const handleDragLeave = useCallback(() => {
    if (dragDepth.current === 0) {
      return;
    }
    dragDepth.current -= 1;
    if (dragDepth.current <= 0) {
      dragDepth.current = 0;
      setDragActive(false);
    }
  }, []);

  const handleDrop = useCallback(
    async (event: DragEvent<HTMLDivElement>) => {
      if (showTrash || !canWrite) {
        return;
      }
      event.preventDefault();
      dragDepth.current = 0;
      setDragActive(false);
      const files = Array.from(event.dataTransfer.files);
      await uploadFiles(files);
    },
    [showTrash, uploadFiles, canWrite]
  );

  const handleClearFilters = useCallback(() => {
    setTypeFilter("all");
    setSizeMinMb("");
    setSizeMaxMb("");
    setDateFilter("any");
    setSortMode("default");
    setContentSearch(false);
    resetContentSearch();
  }, [resetContentSearch]);

  const exitTrashView = useCallback(() => {
    if (showTrash) {
      void loadPath(path);
    }
  }, [showTrash, loadPath, path]);

  const handleSidebarAll = useCallback(() => {
    exitTrashView();
    setTypeFilter("all");
    setDateFilter("any");
    setQuery("");
    void loadPath("/");
    setSidebarOpen(false);
  }, [exitTrashView, loadPath]);

  const handleSidebarRecent = useCallback(() => {
    exitTrashView();
    setTypeFilter("all");
    setDateFilter("7d");
    setSidebarOpen(false);
  }, [exitTrashView]);

  const handleSidebarDocs = useCallback(() => {
    exitTrashView();
    setTypeFilter("document");
    setSidebarOpen(false);
  }, [exitTrashView]);

  const handleSidebarPhotos = useCallback(() => {
    exitTrashView();
    setTypeFilter("image");
    setSidebarOpen(false);
  }, [exitTrashView]);

  const handleSidebarAudio = useCallback(() => {
    exitTrashView();
    setTypeFilter("audio");
    setSidebarOpen(false);
  }, [exitTrashView]);

  const handleSidebarVideo = useCallback(() => {
    exitTrashView();
    setTypeFilter("video");
    setSidebarOpen(false);
  }, [exitTrashView]);

  const handleSidebarArchive = useCallback(() => {
    exitTrashView();
    setTypeFilter("archive");
    setSidebarOpen(false);
  }, [exitTrashView]);

  const canTextPreview = useMemo(() => {
    return Boolean(selected && selected.type === "file" && isTextPreviewableName(selected.name));
  }, [selected]);

  const canImagePreview = useMemo(() => {
    return Boolean(selected && selected.type === "file" && isImagePreviewable(selected.name));
  }, [selected]);

  const storageOverview = useMemo(() => {
    const buckets = {
      image: { label: "Images", bytes: 0, count: 0 },
      document: { label: "Documents", bytes: 0, count: 0 },
      media: { label: "Media Files", bytes: 0, count: 0 },
      archive: { label: "Archives", bytes: 0, count: 0 },
      other: { label: "Other Files", bytes: 0, count: 0 },
    };
    let totalBytes = 0;
    let totalFiles = 0;

    for (const entry of entries) {
      if (entry.type !== "file") {
        continue;
      }
      totalBytes += entry.size;
      totalFiles += 1;

      const category = getFileCategory(entry.name);
      if (category === "image") {
        buckets.image.bytes += entry.size;
        buckets.image.count += 1;
      } else if (category === "document") {
        buckets.document.bytes += entry.size;
        buckets.document.count += 1;
      } else if (category === "audio" || category === "video") {
        buckets.media.bytes += entry.size;
        buckets.media.count += 1;
      } else if (category === "archive") {
        buckets.archive.bytes += entry.size;
        buckets.archive.count += 1;
      } else {
        buckets.other.bytes += entry.size;
        buckets.other.count += 1;
      }
    }

    return {
      totalBytes,
      totalFiles,
      items: [
        { key: "image", ...buckets.image },
        { key: "document", ...buckets.document },
        { key: "media", ...buckets.media },
        { key: "archive", ...buckets.archive },
        { key: "other", ...buckets.other },
      ],
    };
  }, [entries]);

  const menuMetrics = useMemo(() => {
    const recentCutoff = Date.now() - (DATE_RANGE_MS["7d"] ?? 0);
    let recentBytes = 0;
    let recentCount = 0;
    let audioBytes = 0;
    let audioCount = 0;
    let videoBytes = 0;
    let videoCount = 0;
    let archiveBytes = 0;
    let archiveCount = 0;
    for (const entry of entries) {
      if (entry.type !== "file") {
        continue;
      }
      if (entry.mtime >= recentCutoff) {
        recentBytes += entry.size;
        recentCount += 1;
      }
      const category = getFileCategory(entry.name);
      if (category === "audio") {
        audioBytes += entry.size;
        audioCount += 1;
      } else if (category === "video") {
        videoBytes += entry.size;
        videoCount += 1;
      } else if (category === "archive") {
        archiveBytes += entry.size;
        archiveCount += 1;
      }
    }

    const docs = storageOverview.items.find((item) => item.key === "document");
    const images = storageOverview.items.find((item) => item.key === "image");
    const trashBytes = trashItems.reduce((sum, item) => sum + (item.size ?? 0), 0);

    return {
      all: {
        count: storageStats?.totalFiles ?? storageOverview.totalFiles,
        bytes: storageStats?.totalBytes ?? storageOverview.totalBytes,
      },
      recent: { count: recentCount, bytes: recentBytes },
      docs: { count: docs?.count ?? 0, bytes: docs?.bytes ?? 0 },
      photos: { count: images?.count ?? 0, bytes: images?.bytes ?? 0 },
      audio: { count: audioCount, bytes: audioBytes },
      video: { count: videoCount, bytes: videoBytes },
      archive: { count: archiveCount, bytes: archiveBytes },
      trash: { count: trashItems.length, bytes: trashBytes },
    };
  }, [entries, storageOverview, trashItems, storageStats]);

  const storageFallback = storageStats ?? storageStatsRef.current;
  const storageTotalBytes = storageFallback?.totalBytes ?? storageOverview.totalBytes;
  const storageTotalFiles = storageFallback?.totalFiles ?? storageOverview.totalFiles;
  const showDetailPanel = Boolean(selected && selected.type === "file" && !showTrash && !isMobile);
  const shareStatus = shareLoading
    ? "creating"
    : shareLookupLoading
      ? "checking"
      : shareError
        ? "error"
        : shareLink
          ? "ready"
          : "idle";

  useEffect(() => {
    setShareError(null);
    setShareLoading(false);
    setShareLookupLoading(false);
  }, [selectedPath]);

  useEffect(() => {
    if (!selectedPath || shareToken || shareLinks[selectedPath]) {
      return;
    }
    let active = true;
    const fetchExistingShare = async () => {
      setShareLookupLoading(true);
      setShareError(null);
      const response = await apiFetch(`/share?path=${encodeURIComponent(selectedPath)}`);
      if (response.status === 401) {
        setAuth("logged_out");
        return;
      }
      if (!response.ok) {
        if (active) {
          setShareLookupLoading(false);
        }
        return;
      }
      const data = (await response.json()) as { token?: string | null };
      if (active) {
        if (data?.token) {
          const origin = typeof window === "undefined" ? "" : window.location.origin;
          setShareLinks((prev) => ({
            ...prev,
            [selectedPath]: `${origin}/share/${data.token}`,
          }));
        }
        setShareLookupLoading(false);
      }
    };
    void fetchExistingShare();
    return () => {
      active = false;
    };
  }, [selectedPath, shareToken, shareLinks, setAuth]);

  useEffect(() => {
    const hasData =
      menuMetrics.all.count > 0 ||
      menuMetrics.all.bytes > 0 ||
      menuMetrics.docs.count > 0 ||
      menuMetrics.docs.bytes > 0 ||
      menuMetrics.photos.count > 0 ||
      menuMetrics.photos.bytes > 0 ||
      menuMetrics.audio.count > 0 ||
      menuMetrics.audio.bytes > 0 ||
      menuMetrics.video.count > 0 ||
      menuMetrics.video.bytes > 0 ||
      menuMetrics.archive.count > 0 ||
      menuMetrics.archive.bytes > 0 ||
      menuMetrics.recent.count > 0 ||
      menuMetrics.recent.bytes > 0 ||
      menuMetrics.trash.count > 0 ||
      menuMetrics.trash.bytes > 0;
    if (hasData) {
      menuMetricsRef.current = menuMetrics;
    }
  }, [menuMetrics]);

  const displayedMenuMetrics = useMemo(() => {
    const totalBytes =
      menuMetrics.all.bytes +
      menuMetrics.docs.bytes +
      menuMetrics.photos.bytes +
      menuMetrics.audio.bytes +
      menuMetrics.video.bytes +
      menuMetrics.archive.bytes +
      menuMetrics.recent.bytes +
      menuMetrics.trash.bytes;
    if (totalBytes === 0 && menuMetricsRef.current) {
      return menuMetricsRef.current;
    }
    return menuMetrics;
  }, [menuMetrics]);

  useEffect(() => {
    if (storageStats) {
      storageStatsRef.current = storageStats;
      return;
    }
    if (!storageStatsRef.current && storageOverview.totalBytes > 0) {
      storageStatsRef.current = {
        totalBytes: storageOverview.totalBytes,
        totalFiles: storageOverview.totalFiles,
      };
    }
  }, [storageStats, storageOverview]);

  const filtered = useMemo(() => {
    const trimmed = query.trim().toLowerCase();
    const hasQuery = trimmed.length > 0;
    const minBytes = parseSizeInput(sizeMinMb);
    const maxBytes = parseSizeInput(sizeMaxMb);
    const dateRange = DATE_RANGE_MS[dateFilter];
    const now = Date.now();

    let results = entries.filter((entry) => {
      if (hasQuery) {
        const nameMatch = entry.name.toLowerCase().includes(trimmed);
        const contentMatch =
          contentSearch && contentMatches.size > 0 && contentMatches.has(entry.name);
        if (contentSearch) {
          if (!nameMatch && !contentMatch) {
            return false;
          }
        } else if (!nameMatch) {
          return false;
        }
      }

      if (!matchesTypeFilter(entry, typeFilter)) {
        return false;
      }

      if (entry.type === "file") {
        if (minBytes !== null && entry.size < minBytes) {
          return false;
        }
        if (maxBytes !== null && entry.size > maxBytes) {
          return false;
        }
      }

      if (dateRange !== null && now - entry.mtime > dateRange) {
        return false;
      }

      return true;
    });

    return sortEntries(results, sortMode);
  }, [
    entries,
    query,
    typeFilter,
    sizeMinMb,
    sizeMaxMb,
    dateFilter,
    sortMode,
    contentSearch,
    contentMatches,
  ]);

  const totalItems = showTrash ? trashItems.length : filtered.length;
  const totalPages = Math.max(1, Math.ceil(totalItems / pageSize));
  const currentPage = Math.min(page, totalPages);
  const pageStart = (currentPage - 1) * pageSize;
  const pageEnd = pageStart + pageSize;

  const pagedEntries = useMemo(() => {
    if (showTrash) {
      return [];
    }
    return filtered.slice(pageStart, pageEnd);
  }, [showTrash, filtered, pageStart, pageEnd]);

  const pagedTrashItems = useMemo(() => {
    if (!showTrash) {
      return trashItems;
    }
    return trashItems.slice(pageStart, pageEnd);
  }, [showTrash, trashItems, pageStart, pageEnd]);

  const selectedNameSet = useMemo(() => new Set(selectedNames), [selectedNames]);
  const filteredNames = useMemo(() => filtered.map((entry) => entry.name), [filtered]);
  const filteredNameSet = useMemo(() => new Set(filteredNames), [filteredNames]);

  const allSelected =
    filteredNames.length > 0 && filteredNames.every((name) => selectedNameSet.has(name));

  const toggleSelectAll = useCallback(() => {
    setSelectedNames((prev) => {
      if (filteredNames.length === 0) {
        return prev;
      }
      const prevSet = new Set(prev);
      const hasAll = filteredNames.every((name) => prevSet.has(name));
      if (hasAll) {
        return prev.filter((name) => !filteredNameSet.has(name));
      }
      const merged = new Set(prev);
      for (const name of filteredNames) {
        merged.add(name);
      }
      return Array.from(merged);
    });
  }, [filteredNames, filteredNameSet]);

  const handlePageChange = useCallback(
    (nextPage: number) => {
      const clamped = Math.min(Math.max(nextPage, 1), totalPages);
      setPage(clamped);
    },
    [totalPages]
  );

  const handlePageSizeChange = useCallback((nextSize: number) => {
    setPageSize(nextSize);
    setPage(1);
  }, []);

  useEffect(() => {
    if (page !== currentPage) {
      setPage(currentPage);
    }
  }, [page, currentPage]);

  useEffect(() => {
    setPage(1);
  }, [path, showTrash]);

  useEffect(() => {
    if (typeof window === "undefined" || !window.matchMedia) {
      return;
    }
    const media = window.matchMedia("(max-width: 900px)");
    const update = () => setIsMobile(media.matches);
    update();
    if (typeof media.addEventListener === "function") {
      media.addEventListener("change", update);
    } else {
      media.addListener(update);
    }
    return () => {
      if (typeof media.removeEventListener === "function") {
        media.removeEventListener("change", update);
      } else {
        media.removeListener(update);
      }
    };
  }, []);

  useEffect(() => {
    setSidebarOpen(false);
  }, [path, showTrash]);

  const breadcrumbs = useMemo<Breadcrumb[]>(() => {
    if (path === "/") {
      return [{ label: "Home", path: "/" }];
    }

    const parts = path.split("/").filter(Boolean);
    const crumbs: Breadcrumb[] = [{ label: "Home", path: "/" }];
    let current = "";
    for (const part of parts) {
      current = `${current}/${part}`;
      crumbs.push({ label: part, path: current });
    }
    return crumbs;
  }, [path]);
  const currentPathLabel = breadcrumbs[breadcrumbs.length - 1]?.label ?? "Home";

  const filtersActive =
    typeFilter !== "all" ||
    sizeMinMb !== "" ||
    sizeMaxMb !== "" ||
    dateFilter !== "any" ||
    sortMode !== "default" ||
    contentSearch;

  useKeyboardShortcuts({
    enabled: SHORTCUTS_ENABLED && !shareToken,
    showTrash,
    selectionTargets,
    handlers: {
      onSelectAll: toggleSelectAll,
      onCopy: handleCopy,
      onPaste: handlePaste,
      onCreateFolder: handleCreateFolder,
      onUpload: handleUploadClick,
      onEdit: handleOpenEditor,
      onRename: handleRename,
      onDelete: handleDelete,
      onToggleTrash: handleToggleTrash,
      onOpenSelection: handleEntryClick,
      onClearSelection: handleClearSelection,
    },
  });

  useEffect(() => {
    if (shareToken) {
      return;
    }
    if (auth === "unknown") {
      const initialPath = getStoredPath() ?? "/";
      const loadInitialPath = async () => {
        const ok = await loadPath(initialPath);
        if (!ok && initialPath !== "/") {
          clearStoredPath();
          await loadPath("/");
        }
      };
      void loadInitialPath();
    }
  }, [auth, loadPath, shareToken]);

  useEffect(() => {
    if (shareToken || typeof window === "undefined") {
      return;
    }
    const params = new URLSearchParams(window.location.search);
    const editPath = params.get("edit");
    if (editPath) {
      setPendingEditorPath(editPath);
    }
  }, [shareToken]);

  useEffect(() => {
    if (shareToken) {
      return;
    }
    if (auth !== "authed" || !pendingEditorPath) {
      return;
    }
    void openEditorByPath(pendingEditorPath);
    setPendingEditorPath(null);
  }, [auth, pendingEditorPath, openEditorByPath, shareToken]);

  useEffect(() => {
    if (shareToken) {
      return;
    }
    if (auth === "logged_out") {
      resetEditorState();
    }
  }, [auth, resetEditorState, shareToken]);

  useEffect(() => {
    setStoredViewMode(viewMode);
  }, [viewMode]);

  useEffect(() => {
    if (shareToken) {
      return;
    }
    if (auth !== "authed") {
      return;
    }
    let active = true;
    const fetchStats = async () => {
      const response = await apiFetch("/storage");
      if (response.status === 401) {
        setAuth("logged_out");
        return;
      }
      if (!response.ok) {
        return;
      }
      const data = (await response.json()) as StorageStats;
      if (active) {
        setStorageStats(data);
        storageStatsRef.current = data;
      }
    };
    void fetchStats();
    return () => {
      active = false;
    };
  }, [auth, shareToken]);

  if (shareToken) {
    return <SharedFileView token={shareToken} />;
  }

  return (
    <div
      className="app"
      onDragEnter={handleDragEnter}
      onDragOver={handleDragOver}
      onDragLeave={handleDragLeave}
      onDrop={handleDrop}
    >
      <Toasts toasts={toasts} />
      {uploadJobs.length > 0 ? (
        <div className="upload-popups">
          {uploadJobs.map((job) => (
            <div key={job.id} className={`upload-popup ${job.status}`}>
              <div className="upload-popup-head">
                <span className="upload-popup-name">{job.name}</span>
                <span className="upload-popup-percent">{job.percent}%</span>
              </div>
              <div className="upload-popup-bar">
                <div className="upload-popup-fill" style={{ width: `${job.percent}%` }} />
              </div>
              <div className="upload-popup-meta">
                <span>
                  {formatBytes(job.loaded)} / {formatBytes(job.total || job.loaded)}
                </span>
                <span className="upload-popup-status">
                  {job.status === "uploading"
                    ? "Uploading"
                    : job.status === "done"
                      ? "Completed"
                      : "Failed"}
                </span>
              </div>
            </div>
          ))}
        </div>
      ) : null}
      <EditorModal
        open={editorOpen}
        file={editorFile}
        dirty={editorOpen && editorContent !== editorInitialContent}
        loading={editorLoading}
        saving={editorSaving}
        canWrite={canWrite}
        onOpenInNewTab={openEditorInNewTab}
        onChange={setEditorContent}
        onSave={handleSaveEditor}
        onClose={closeEditor}
      />
      <ImagePreviewModal
        path={imagePreviewPath}
        name={imagePreviewName}
        onClose={closeImagePreview}
        onError={handleImageError}
      />
      <TextPreviewModal preview={preview} open={textPreviewOpen} onClose={closeTextPreview} />
      <div
        className={
          auth === "logged_out" ? (isLoginRoute ? "login-shell" : "landing-shell-wrap") : "shell"
        }
      >
        {auth === "logged_out" ? (
          isLoginRoute ? (
            <LoginForm
              loginUsername={loginUsername}
              password={password}
              error={error}
              onUsernameChange={setLoginUsername}
              onPasswordChange={setPassword}
              onSubmit={handleLogin}
            />
          ) : (
            <LandingPage />
          )
        ) : (
          <div
            className={`workspace${showDetailPanel ? " has-detail" : ""}${
              sidebarOpen ? " sidebar-open" : ""
            }`}
          >
            <aside className="sidebar">
              {/* <div className="sidebar-card">
                <div className="workspace-pill">
                  <span className="workspace-avatar">
                    {(username || "W").slice(0, 1).toUpperCase()}
                  </span>
                  <div>
                    <p className="workspace-name">{username || "Workspace"}</p>
                    <p className="workspace-meta">{userRole}</p>
                  </div>
                </div>
              </div> */}

              <Header
                auth={auth}
                username={username}
                userRole={userRole}
                theme={theme}
                showTrash={showTrash}
                filtersOpen={filtersOpen}
                filtersActive={filtersActive}
                typeFilter={typeFilter}
                sizeMinMb={sizeMinMb}
                sizeMaxMb={sizeMaxMb}
                dateFilter={dateFilter}
                onThemeChange={setTheme}
                onLogout={handleLogout}
                onToggleFilters={() => setFiltersOpen((prev) => !prev)}
                onTypeFilterChange={setTypeFilter}
                onSizeMinChange={setSizeMinMb}
                onSizeMaxChange={setSizeMaxMb}
                onDateFilterChange={setDateFilter}
                onClearFilters={handleClearFilters}
              />
              {/* <div className="sidebar-section">
                <p className="sidebar-label">General</p>
                <button type="button" className="nav-button" onClick={handleSidebarAll}>
                  Overview
                </button>
                <button type="button" className="nav-button is-muted" disabled>
                  Settings
                </button>
              </div> */}

              <div className="sidebar-section">
                <p className="sidebar-label">Main menu</p>
                <button
                  type="button"
                  className={`menu-row${
                    !showTrash && typeFilter === "all" && dateFilter === "any" ? " is-active" : ""
                  }`}
                  onClick={handleSidebarAll}
                >
                  <div className="menu-icon all">all</div>
                  <div className="menu-info">
                    <p>All files</p>
                    <span>{storageTotalFiles} files</span>
                  </div>
                  <span className="menu-size">{formatBytes(displayedMenuMetrics.all.bytes)}</span>
                </button>
                <button
                  type="button"
                  className={`menu-row${dateFilter === "7d" ? " is-active" : ""}`}
                  onClick={handleSidebarRecent}
                >
                  <div className="menu-icon recent">new</div>
                  <div className="menu-info">
                    <p>Recent</p>
                    <span>{displayedMenuMetrics.recent.count} files</span>
                  </div>
                  <span className="menu-size">
                    {formatBytes(displayedMenuMetrics.recent.bytes)}
                  </span>
                </button>
                <button
                  type="button"
                  className={`menu-row${typeFilter === "document" ? " is-active" : ""}`}
                  onClick={handleSidebarDocs}
                >
                  <div className="menu-icon docs">doc</div>
                  <div className="menu-info">
                    <p>Docs</p>
                    <span>{displayedMenuMetrics.docs.count} files</span>
                  </div>
                  <span className="menu-size">{formatBytes(displayedMenuMetrics.docs.bytes)}</span>
                </button>
                <button
                  type="button"
                  className={`menu-row${typeFilter === "image" ? " is-active" : ""}`}
                  onClick={handleSidebarPhotos}
                >
                  <div className="menu-icon photos">img</div>
                  <div className="menu-info">
                    <p>Photos</p>
                    <span>{displayedMenuMetrics.photos.count} files</span>
                  </div>
                  <span className="menu-size">
                    {formatBytes(displayedMenuMetrics.photos.bytes)}
                  </span>
                </button>
                <button
                  type="button"
                  className={`menu-row${typeFilter === "audio" ? " is-active" : ""}`}
                  onClick={handleSidebarAudio}
                >
                  <div className="menu-icon audio">aud</div>
                  <div className="menu-info">
                    <p>Audio</p>
                    <span>{displayedMenuMetrics.audio.count} files</span>
                  </div>
                  <span className="menu-size">{formatBytes(displayedMenuMetrics.audio.bytes)}</span>
                </button>
                <button
                  type="button"
                  className={`menu-row${typeFilter === "video" ? " is-active" : ""}`}
                  onClick={handleSidebarVideo}
                >
                  <div className="menu-icon video">vid</div>
                  <div className="menu-info">
                    <p>Video</p>
                    <span>{displayedMenuMetrics.video.count} files</span>
                  </div>
                  <span className="menu-size">{formatBytes(displayedMenuMetrics.video.bytes)}</span>
                </button>
                <button
                  type="button"
                  className={`menu-row${typeFilter === "archive" ? " is-active" : ""}`}
                  onClick={handleSidebarArchive}
                >
                  <div className="menu-icon archive">zip</div>
                  <div className="menu-info">
                    <p>Archives</p>
                    <span>{displayedMenuMetrics.archive.count} files</span>
                  </div>
                  <span className="menu-size">
                    {formatBytes(displayedMenuMetrics.archive.bytes)}
                  </span>
                </button>
                <button
                  type="button"
                  className={`menu-row${showTrash ? " is-active" : ""}`}
                  onClick={handleToggleTrash}
                >
                  <div className="menu-icon trash">bin</div>
                  <div className="menu-info">
                    <p>Trash</p>
                    <span>{displayedMenuMetrics.trash.count} files</span>
                  </div>
                  <span className="menu-size">{formatBytes(displayedMenuMetrics.trash.bytes)}</span>
                </button>
              </div>

            </aside>
            {sidebarOpen ? (
              <button
                type="button"
                className="sidebar-overlay"
                onClick={() => setSidebarOpen(false)}
                aria-label="Close menu"
              />
            ) : null}

            <main className="main">

              <div className="stack">
                <Toolbar
                  query={query}
                  currentPathLabel={currentPathLabel}
                  onQueryChange={setQuery}
                  onUp={() => parent && loadPath(parent)}
                  onRefresh={() => loadPath(path)}
                  onUploadClick={handleUploadClick}
                  onCreateFolder={handleCreateFolder}
                  onToggleTrash={handleToggleTrash}
                  showTrash={showTrash}
                  viewMode={viewMode}
                  onViewModeChange={setViewMode}
                  showEdit={canEditTarget}
                  editDisabled={editDisabled}
                  editLoading={editorLoading}
                  onEdit={handleOpenEditor}
                  actionLoading={actionLoading}
                  canWrite={canWrite}
                  selectionCount={selectionCount}
                  clipboardCount={clipboard?.length ?? 0}
                  archiveHref={archiveHref}
                  parent={parent}
                  fileInputRef={fileInputRef}
                  onUploadChange={handleUploadChange}
                  onCopy={handleCopy}
                  onPaste={handlePaste}
                  onRename={handleRename}
                  onMove={handleMove}
                  onArchiveClick={handleArchiveClick}
                  onDelete={handleDelete}
                  onClearSelection={handleClearSelection}
                  onToggleSidebar={() => setSidebarOpen((prev) => !prev)}
                />
                <div className="breadcrumbs-bar">
                  <div className="breadcrumbs">
                    <button
                      type="button"
                      className="crumb crumb-home"
                      onClick={() => loadPath("/")}
                      aria-label="Back to Home"
                    >
                      <Home size={16} strokeWidth={1.8} aria-hidden="true" />
                    </button>
                    {breadcrumbs.map((crumb, index) => (
                      <button
                        key={crumb.path}
                        type="button"
                        className="crumb"
                        onClick={() => loadPath(crumb.path)}
                      >
                        {crumb.label}
                        {index < breadcrumbs.length - 1 ? <span>/</span> : null}
                      </button>
                    ))}
                  </div>
                </div>

                <FileList
                  showTrash={showTrash}
                  loading={loading}
                  trashItems={pagedTrashItems}
                  filtered={pagedEntries}
                  path={path}
                  viewMode={viewMode}
                  selectedNames={selectedNames}
                  activeName={selected?.name ?? null}
                  allSelected={allSelected}
                  dragActive={dragActive}
                  actionLoading={actionLoading}
                  canWrite={canWrite}
                  sortMode={sortMode}
                  onSortModeChange={setSortMode}
                  pagination={{
                    page,
                    pageSize,
                    totalItems,
                    pageSizeOptions: PAGE_SIZE_OPTIONS,
                    onPageChange: handlePageChange,
                    onPageSizeChange: handlePageSizeChange,
                  }}
                  showPaginationTop
                  onToggleSelectAll={toggleSelectAll}
                  onToggleSelect={toggleSelect}
                  onEntryClick={handleEntryClick}
                  onEntryDoubleClick={handleEntryDoubleClick}
                  onRestore={handleRestore}
                />
              </div>
            </main>

            {showDetailPanel ? (
              <aside className="detail-panel">
                <DetailPanel
                  showTrash={showTrash}
                  selected={selected}
                  canTextPreview={canTextPreview}
                  canImagePreview={canImagePreview}
                  error={error}
                  share={{
                    status: shareStatus,
                    url: shareLink,
                    error: shareError,
                  }}
                  onShareCreate={handleShareCreate}
                  onShareCopy={handleShareCopy}
                  onShareOpen={handleShareOpen}
                  onClose={() => setSelected(null)}
                />
              </aside>
            ) : null}
          </div>
        )}
      </div>
    </div>
  );
}
