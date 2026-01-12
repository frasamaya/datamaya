# Changelog

All notable changes to this project will be documented in this file.

The format is based on Keep a Changelog, and this project follows SemVer.

## [Unreleased]
### Added
- Zip archive downloads for selected files/folders (archive endpoint supports `format=zip` or `format=targz`).
- Archive action now provides a direct download link.
- Large zip archives switch to store mode at/above 100 MB (`ARCHIVE_LARGE_MB`).

## [0.1.0] - 2026-01-12
### Added
- Auth with admin password and local user support.
- Read/write file operations (browse, upload, download, rename, delete/restore, copy/paste).
- Safe path resolution with symlink and traversal protections.
- Text preview with size cap and image preview popup.
- Search, filters, sorting, and pagination for large directories.
- Audit logging and configurable root paths.
