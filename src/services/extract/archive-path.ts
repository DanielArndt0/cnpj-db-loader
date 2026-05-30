import path from "node:path";

const FIRST_SPLIT_VOLUME_SUFFIX = ".zip.001";

export function isSupportedArchiveFileName(entryName: string): boolean {
  const normalizedName = entryName.toLowerCase();
  return (
    normalizedName.endsWith(".zip") ||
    normalizedName.endsWith(FIRST_SPLIT_VOLUME_SUFFIX)
  );
}

export function archiveOutputFolderName(archivePath: string): string {
  const archiveName = path.basename(archivePath);
  const normalizedName = archiveName.toLowerCase();

  if (normalizedName.endsWith(FIRST_SPLIT_VOLUME_SUFFIX)) {
    return archiveName.slice(0, -FIRST_SPLIT_VOLUME_SUFFIX.length);
  }

  return path.basename(archiveName, path.extname(archiveName));
}
