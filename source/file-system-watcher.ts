import { MapSource, SetSource, Subscribable } from '@xf-common/dynamic';
import { SharedDemandAbortController } from '@xf-common/general/abort-signals';
import { dispose, tryDispose } from '@xf-common/general/disposables';
import * as FS from 'node:fs';
import * as OS from 'node:os';
import * as Path from 'node:path';

export class FileSystemWatcher {
  readonly #locations = new Map<string, WatchedLocation>();

  watch (path: string, abort: AbortSignal, listener: (event: FileSystemWatcher.Event, path: string) => void): FileSystemWatcher.Location {
    const loc = FileSystemWatcher._ensureLocation(this, path);
    const sub = loc.controller.subscribe(listener);
    abort.addEventListener('abort', () => dispose(sub));
    return loc;
  }

  /** @internal */
  static _ensureLocation (fsw: FileSystemWatcher, path: string): WatchedLocation {
    let loc = fsw.#locations.get(path);
    if (!loc) {
      loc = new WatchedLocation(fsw, fsw.#locations, path);
      fsw.#locations.set(path, loc);
    }
    return loc;
  }
}
export namespace FileSystemWatcher {
  export type Event = 'add' | 'delete' | 'change';
  export interface Location {
    readonly path: string;
    readonly exists: boolean;
    readonly isFile: boolean;
    readonly isDirectory: boolean;
    readonly dateCreated: number;
    readonly dateModified: number;
    readonly fileSize: number;
    readonly filePaths: SetSource<string>;
    readonly subdirPaths: SetSource<string>;
    readonly entries: MapSource<string, FS.StatsBase<any>>;
  }
}

class WatchedLocation implements Subscribable.DemandObserver.ListenerInterface<[]>, FileSystemWatcher.Location {
  constructor (
    private readonly fsw: FileSystemWatcher,
    private readonly locations: Map<string, WatchedLocation>,
    public readonly path: string,
  ) {
    this.#isHomeDir = path === OS.homedir();
    this.#parentPath = Path.dirname(this.path);
    this.#shouldWatchParent = !this.#isHomeDir && this.#parentPath !== '/';
  }
  readonly #controller = new Subscribable.Controller<[event: FileSystemWatcher.Event, path: string, stats: FS.Stats | undefined]>(this);
  readonly #isHomeDir: boolean;
  readonly #parentPath: string;
  readonly #shouldWatchParent: boolean;
  #watcher: FS.FSWatcher | undefined;
  #stats: FS.Stats | undefined;
  #watchingParent: AbortController | undefined;
  #attachedChildren: Map<string, WatchedLocation> | undefined;
  #childrenAbortController: SharedDemandAbortController | undefined;
  #filePaths: SetSource.Manual<string> | undefined;
  #subdirPaths: SetSource.Manual<string> | undefined;
  #entries: MapSource.Manual<string, FS.StatsBase<any>> | undefined;

  get controller () { return this.#controller; }
  get exists (): boolean { return this.#stats !== undefined; }
  get isFile () { return this.#stats?.isFile() === true; }
  get isDirectory () { return this.#stats?.isDirectory() === true; }
  get dateCreated (): number { return this.#stats?.birthtimeMs ?? 0; }
  get dateModified (): number { return this.#stats?.mtimeMs ?? 0; }
  get fileSize (): number { return this.#stats?.size ?? 0; }
  get filePaths (): SetSource.Manual<string> { return this.#filePaths ??= SetSource.create(); }
  get subdirPaths (): SetSource.Manual<string> { return this.#subdirPaths ??= SetSource.create(); }
  get entries (): MapSource.Manual<string, FS.StatsBase<any>> { return this.#entries ??= MapSource.create(); }

  attachChild (child: WatchedLocation, signal: AbortSignal): void {
    const children = this.#attachedChildren ??= new Map();
    let childrenAbortController = this.#childrenAbortController;
    if (!childrenAbortController) {
      const sub = this.#controller.subscribe((event, path) => {
        this.forwardEventToChildLocation(event, path, this.getStats(path));
      });
      childrenAbortController = this.#childrenAbortController = new SharedDemandAbortController();
      childrenAbortController.signal.addEventListener('abort', () => {
        this.#childrenAbortController = undefined;
        this.#attachedChildren = undefined;
        dispose(sub);
      });
    }
    childrenAbortController.attach(signal);
    children.set(child.path, child);
  }

  private getStats (path: string): FS.Stats | undefined {
    try {
      return FS.statSync(path);
    }
    catch (error) {
      if (error.code !== 'ENOENT') {
        console.error(`Error getting stats for ${path}:`, error);
        throw error; // rethrow if it's not a "file not found" error
      }
      return undefined; // return undefined if the file doesn't exist
    }
  }

  private forwardEventToChildLocation (event: FileSystemWatcher.Event, path: string, stats: FS.Stats | undefined): void {
    const loc = this.locations.get(path);
    if (loc) {
      console.debug(`Signalling child location:`, loc.path);
      loc.controller.signal(event, Path.join(this.path, path), stats);
    }
  }
  private initializeEntries (): void {
    if (this.isFile || !this.exists) return;
    const entries = FS.readdirSync(this.path, { withFileTypes: true });
    if (this.filePaths.size > 0 || this.subdirPaths.size > 0) {
      throw new Error(`Expected empty filePaths and subdirPaths for ${this.path}, but found non-empty arrays. This is a bug.`);
    }
    for (const entry of entries) {
      if (entry.isFile()) {
        this.filePaths.add(Path.join(this.path, entry.name));
      }
      else if (entry.isDirectory()) {
        this.subdirPaths.add(Path.join(this.path, entry.name));
      }
    }
  }
  private initializeWatcher (): void {
    if (this.isFile) return; // No need to watch files directly, the parent directory will notify us of changes.
    if (this.#watcher) {
      throw new Error(`Watcher already initialized ... this shouldn't have happened. This is a bug.`);
    }
    this.#watcher = (FS
      .watch(this.path, (event, filename) => this.onNativeWatcherEvent(event, filename))
      .addListener('error', (error: any) => {
        if (error.code !== 'ENOENT') {
          console.error(`Error watching ${this.path}:`, error);
          throw error;
        }
      })
    );
  }
  private attachToParentLocation (): void {
    if (!this.#shouldWatchParent) return;
    const { signal } = this.#watchingParent = new AbortController();
    const loc = FileSystemWatcher._ensureLocation(this.fsw, this.#parentPath);
    loc.attachChild(this, signal);
  }

  private onNativeWatcherEvent (nativeEvent: FS.WatchEventType, filename: string | null): void {
    console.debug(`onNativeWatcherEvent:`, nativeEvent, filename);
    // If no filename is provided, I really don't care about the event.
    if (!filename) return;
    const path = Path.join(this.path, filename);
    const stats = this.getStats(path);

    let event: FileSystemWatcher.Event;
    switch (nativeEvent) {
      case 'rename': {
        if (stats) {
          event = 'add';
          this.entries.set(filename, stats);
          if (stats.isDirectory()) {
            this.subdirPaths.add(path);
          }
          else {
            this.filePaths.add(path);
          }
        }
        else {
          event = 'delete';
          this.entries.delete(filename);
          this.filePaths.delete(path);
          this.subdirPaths.delete(path);
        }
        break;
      }
      case 'change': {
        event = 'change';
        break;
      }
    }
    console.debug(`Signalling event:`, event, `for path:`, path);
    this.#controller.signal(event, path, stats);
  }

  private onSubscriptionEvent (event: FileSystemWatcher.Event, path: string, stats: FS.Stats | undefined): void {
    this.#stats = stats;
  }

  #sub: Disposable | undefined;
  online (): void {
    this.#stats = this.getStats(this.path);
    this.initializeEntries();
    this.initializeWatcher();
    this.attachToParentLocation();
    this.#sub = this.#controller.subscribe((event, path, stats) => this.onSubscriptionEvent(event, path, stats));
  }

  offline (): void {
    this.#watcher?.close();
    this.#watchingParent?.abort();
    this.locations.delete(this.path);
    this.#filePaths?.clear();
    this.#subdirPaths?.clear();
    tryDispose(this.#sub);
  }
}
