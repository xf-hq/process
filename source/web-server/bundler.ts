import type { ConsoleLogger } from '@xf-common/facilities/logging';
import { spawn } from 'child_process';
import { watch } from 'node:fs';
import * as FS from 'node:fs/promises';
import * as OS from 'node:os';
import * as Path from 'node:path';
import type { WebRequest } from './web-request';
import { debounce } from '@xf-common/general/timing';

export interface BundlerOptions {
  /**
   * Absolute path to a project directory containing a tsconfig.json file.
   */
  readonly projectDir: string;
  /**
   * Path to the entrypoint file, relative to the project directory.
   */
  readonly entrypointPath: string;
  readonly log: ConsoleLogger;
  readonly onClientScriptUpdated?: () => void;
}

export async function initializeScriptBundle (options: BundlerOptions) {
  const { log, onClientScriptUpdated, projectDir, entrypointPath } = options;

  // [TEMPORARY BUN ISSUE WORKAROUND] Kill any existing Bun build processes to prevent orphaned processes:
  try {
    const { stdout } = Bun.spawn(['ps', 'aux'], {
      stdout: 'pipe',
      stderr: 'pipe',
    });

    const processes = await new Response(stdout).text();
    const lines = processes.split('\n');

    for (let i = 0; i < lines.length; i++) {
      const line = lines[i];
      if (line.includes('bun') && line.includes('build') && line.includes('--watch')) {
        const parts = line.trim().split(/\s+/);
        if (parts.length >= 2) {
          const pid = parseInt(parts[1], 10);
          if (!isNaN(pid) && pid !== process.pid) {
            try {
              process.kill(pid, 'SIGTERM');
              log.info(`Killed existing Bun build process with pid ${pid}.`);
            }
            catch (error) {
              // Process might have already been killed or we don't have permission
              log.warn(`Failed to kill process ${pid}: ${error}`);
            }
          }
        }
      }
    }
  }
  catch (error) {
    log.verbose(`Failed to check for existing Bun processes: ${error}`);
  }

  const tempDir = Path.join(OS.tmpdir(), `bun-bundle-${crypto.randomUUID()}`);
  await FS.mkdir(tempDir, { recursive: true });

  const bundlePath = Path.join(tempDir, 'bundle.js');

  const args = ['build', Path.resolve(projectDir, entrypointPath), '--outfile', bundlePath, '--watch', '--sourcemap=inline', '--no-clear-screen'];
  const proc = spawn('bun', args, {
    cwd: projectDir,
    stdio: 'inherit',
    detached: false,
  });

  log.verbose(`Bun build watcher started with pid ${proc.pid}.`);

  const ready = Promise.withResolvers<void>();
  const bundleFile = Bun.file(bundlePath);
  await FS.mkdir(tempDir, { recursive: true });
  await bundleFile.write(''); // Make sure the file exists before we start watching it.
  let lastModified = 0;

  const watcher = watch(bundlePath, { recursive: true });
  // Use Intl to create a nice date formatter for logging:
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
      onClientScriptUpdated?.();
    }
  }, 500));

  await ready.promise;

  return function serveBundle (req: WebRequest) {
    req.serve(new Response(bundleFile));
  };
}
