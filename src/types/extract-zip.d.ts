declare module "extract-zip" {
  export type ExtractOptions = {
    dir: string;
    defaultDirMode?: number;
    defaultFileMode?: number;
    onEntry?: (entry: unknown, zipfile: unknown) => void;
  };

  export default function extract(
    source: string,
    options: ExtractOptions,
  ): Promise<void>;
}
