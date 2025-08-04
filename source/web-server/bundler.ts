import type { ConsoleLogger } from '@xf-common/facilities/logging';
import { spawn } from 'child_process';
import { watch } from 'node:fs';
import * as FS from 'node:fs/promises';
import * as OS from 'node:os';
import * as Path from 'node:path';
import type { WebRequest } from './web-request';
import { debounce } from '@xf-common/general/timing';
import { isUndefined } from '@xf-common/general/type-checking';

interface BundlerOptions {
  /**
   * Absolute path to a project directory containing a tsconfig.json file.
   */
  readonly projectDir: string;
  /**
   * Path to the entrypoint file, relative to the project directory.
   */
  readonly entrypointPath: string;
  readonly log: ConsoleLogger;
  readonly onClientScriptUpdated?: (bundleFile: Bun.BunFile) => void;
}

export async function initializeScriptBundleRouteHandler (options: BundlerOptions) {
  let bundleFile: Bun.BunFile;
  await initializeScriptBundle({
    ...options,
    onClientScriptUpdated: (_bundleFile) => {
      bundleFile = _bundleFile;
      options.onClientScriptUpdated?.(_bundleFile);
    },
  });
  return function serveBundle (req: WebRequest) {
    req.serve(new Response(bundleFile));
  };
}

export async function initializeScriptBundle (options: BundlerOptions): Promise<Bun.BunFile> {
  const { log, onClientScriptUpdated, projectDir } = options;
  const entrypointPath = Path.resolve(projectDir, options.entrypointPath);

  const tempDir = Path.join(OS.tmpdir(), `bun-bundle-${crypto.randomUUID()}`);
  await FS.mkdir(tempDir, { recursive: true });

  const bundlePath = Path.join(tempDir, 'bundle.js');

  const proc = Bun.spawn([
    'bun', 'build', entrypointPath,
    '--outfile', bundlePath,
    '--watch',
    '--sourcemap=inline',
    '--no-clear-screen',
  ], {
    cwd: projectDir,
    stdout: 'ignore',
  });

  if (OS.platform() === 'linux' || OS.platform() === 'darwin') {
    // When Bun restarts in watch mode, for some reason old child processes are orphaned and not killed. The code below
    // will kill any existing Bun build processes that target `entrypointPath`.
    try {
      for await (const line of Bun.$`ps aux | grep 'bun build ${entrypointPath}'`.lines()) {
        const parts = line.trim().split(/\s+/);
        if (parts.length < 2) continue;
        const pid = parseInt(parts[1]);
        if (pid !== proc.pid) {
          try {
            process.kill(pid, 'SIGTERM');
            log.good(`Killed existing Bun build process with pid ${pid}.`);
          }
          catch (error) {
            if (error.code !== 'ESRCH') {
              log.warn(`Failed to kill process ${pid}: ${error.message ?? error}`);
            }
          }
        }
      }
    }
    catch (error) {
      log.verbose(`Failed to check for existing Bun processes: ${error}`);
    }
  }

  log.verbose(`Bun build watcher started with pid ${proc.pid}.`);

  const ready = Promise.withResolvers<void>();
  const bundleFile = Bun.file(bundlePath);
  await FS.mkdir(tempDir, { recursive: true });
  await bundleFile.write(''); // Make sure the file exists before we start watching it.
  let lastModified = 0;

  const watcher = watch(bundlePath, { recursive: true });
  const dateFormatter = new Intl.DateTimeFormat('en-US', {
    year: 'numeric',
    month: '2-digit',
    day: '2-digit',
    hour: '2-digit',
    minute: '2-digit',
    second: '2-digit',
  });
  watcher.on('change', debounce(async (eventType) => {
    const stat = await bundleFile.stat();
    ready.resolve();
    if (stat.mtimeMs > lastModified) {
      lastModified = stat.mtimeMs;
      log.good(`Bundle file has been updated (${eventType}) at ${dateFormatter.format(stat.mtimeMs)}.`);
      onClientScriptUpdated?.(bundleFile);
    }
  }, 500));

  await ready.promise;

  return bundleFile;
}
