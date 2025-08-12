import { MapSource } from '@xf-common/dynamic';
import { PathLens } from '@xf-common/facilities/path-lens';
import { dispose } from '@xf-common/general/disposables.ts';
import { IdGenerator } from '@xf-common/general/ids-and-caching.ts';
import { isString } from '@xf-common/general/type-checking';
import type { Messaging } from '@xf-common/network/messaging';
import type { ServerWebSocket } from 'bun';

export type ClientWebSocket<TServerContext> = ServerWebSocket<ClientWebSocket.Data<TServerContext>>;
export namespace ClientWebSocket { export type Data<TServerContext> = { client: RemoteClient.Controller<TServerContext> }; }

export interface RemoteClients {
  send (clientId: number, message: any): void;
  broadcast (message: any): void;
}

export class RemoteClientsGroup<TServerContext = unknown> implements RemoteClients {
  readonly #clients = MapSource.create<number, {
    client: RemoteClient<TServerContext>;
    activeInGroup: AbortController;
  }>();

  add (client: RemoteClient<TServerContext>): void {
    const activeInGroup = new AbortController();
    // console.debug(`Adding client #${client.id} to the group.`);
    this.#clients.set(client.id, { client, activeInGroup });
    client.onClose(() => this.remove(client), activeInGroup.signal);
  }

  remove (client: RemoteClient<TServerContext>): void {
    // console.debug(`Removing client #${client.id} from the group.`);
    const entry = this.#clients.get(client.id);
    if (entry) {
      this.#clients.delete(client.id);
      entry.activeInGroup.abort();
    }
  }

  send (clientId: number, message: any): void {
    const entry = this.#clients.get(clientId);
    if (entry) entry.client.send(message);
  }

  broadcast (message: any): void {
    for (const { client } of this.#clients.__map.values()) {
      console.debug(`Broadcasting to client #${client.id}`);
      client.send(message);
    }
  }
}

export class RemoteClientManager<TServerContext = unknown> implements RemoteClients {
  constructor (serverContext: TServerContext) {
    this.#controller = new RemoteClientsController(serverContext);
  }
  readonly #controller: RemoteClientsController<TServerContext>;

  allocate (ws: ClientWebSocket<TServerContext>): RemoteClient.Controller<TServerContext> {
    return this.#controller.allocateClient(ws);
  }

  send (clientId: number, message: any): void {
    this.#controller.send(clientId, message);
  }

  broadcast (message: any): void {
    this.#controller.broadcast(message);
  }
}

class RemoteClientsController<TServerContext> {
  constructor (serverContext: TServerContext) { this.#serverContext = serverContext; }
  readonly #serverContext: TServerContext;
  readonly #clients = MapSource.create<number, RemoteClient.Controller<TServerContext>>();

  get serverContext () { return this.#serverContext; }

  allocateClient (ws: ClientWebSocket<TServerContext>): RemoteClient.Controller<TServerContext> {
    const client = new RemoteClient.Controller(this, ws);
    this.#clients.set(client.id, client);
    return client;
  }

  releaseClient (client: RemoteClient.Controller<TServerContext>): void {
    this.#clients.delete(client.id);
    dispose(client);
  }

  send (clientId: number, message: any): void {
    const client = this.#clients.get(clientId);
    if (client) client.send(message);
  }

  broadcast (message: any): void {
    for (const client of this.#clients.__map.values()) {
      client.send(message);
    }
  }
}

export interface RemoteClient<TServerContext = unknown> extends Disposable {
  readonly id: number;
  readonly abortSignal: AbortSignal;

  configureMessaging<TMessageContext extends Messaging.InboundMessageContext>(config: RemoteClient.MessagingConfig<TMessageContext, TServerContext>): void;
  send (message: any): void;
  reject (): void;
  onClose (callback: () => void, abort?: AbortSignal): void;
}
export namespace RemoteClient {
  export type Controller<TServerContext> = InstanceType<typeof RemoteClient.Controller<TServerContext>>;
  export const Controller = class RemoteClientController<TServerContext> implements RemoteClient<TServerContext> {
    constructor (controller: RemoteClientsController<TServerContext>, ws: ClientWebSocket<TServerContext>) {
      this._controller = controller;
      this._ws = ws;
      this._abortController = new AbortController();
    }
    private readonly _controller: RemoteClientsController<TServerContext>;
    private readonly _ws: ClientWebSocket<TServerContext>;
    private readonly _id = IdGenerator.global();
    private readonly _abortController: AbortController;
    private _disposed = false;
    private _messaging?: MessagingConfig<Messaging.InboundMessageContext, TServerContext>;
    private _onClosed?: Set<() => void>;

    get id () { return this._id; }
    get abortSignal () { return this._abortController.signal; }

    webSocketClosed () {
      if (this._onClosed) {
        for (const callback of this._onClosed) {
          callback();
        }
      }
      this._controller.releaseClient(this);
    }

    webSocketMessage (data: string | Buffer) {
      const rawMessage = String(data);
      const message = JSON.parse(rawMessage);
      if (!isString(message?.type) || !('data' in message)) {
        console.warn(`Received malformed message from client #${this.id}:`, rawMessage);
        return;
      }
      const messaging = this._messaging;
      if (messaging) {
        const context = messaging.createContext(
          message,
          PathLens.from(':', message.type),
          message.data,
          this._controller.serverContext,
        );
        return messaging.router.handleMessage(context);
      }
    }

    configureMessaging<TMessageContext extends Messaging.InboundMessageContext>(config: MessagingConfig<TMessageContext, TServerContext>): void {
      this._messaging = config;
      if (config.onClosed) this.onClose(config.onClosed);
    }

    send (message: any) {
      this._ws.send(JSON.stringify(message));
    }

    reject () {
      dispose(this);
    }

    onClose (callback: () => void, abort?: AbortSignal): void {
      const _onClosed = this._onClosed ??= new Set();
      _onClosed.add(callback);
      if (abort) {
        abort.addEventListener('abort', () => {
          _onClosed.delete(callback);
        });
      }
    }

    [Symbol.dispose] () {
      if (this._disposed) return;
      this._disposed = true;
      this._abortController.abort();
      this._ws.close();
      this._controller.releaseClient(this);
    }
  };
  export interface MessagingConfig<TMessageContext extends Messaging.InboundMessageContext, TServerContext> {
    readonly abort?: AbortSignal;
    readonly router: Messaging.MessageHandler<TMessageContext>;
    readonly createContext: (message: Messaging.Message, messageType: PathLens, messageData: any, context: TServerContext) => TMessageContext;
    readonly onClosed?: () => void;
  }
}
