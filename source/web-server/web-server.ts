import { isFunction } from '@xf-common/general/type-checking.ts';
import { terminal } from '@xf-common/terminal/terminal';
import type { Server } from 'bun';
import { ProcessEnv } from '../process-env.ts';
import { RemoteClient, RemoteClientManager, type ClientWebSocket, type RemoteClients } from './remote-clients.ts';
import { Router, type RoutePathMapSpec } from './router.ts';

const log = terminal.logger(`Web Server`);

export namespace WebServer {
  export interface Config<TServerContext> {
    readonly port: number;
    readonly routes: RoutePathMapSpec | ((clients: RemoteClients) => RoutePathMapSpec | Promise<RoutePathMapSpec>);
    readonly context: TServerContext;
  }
}
export class WebServer<TServerContext> {
  static async initialize<TServerContext> (config: WebServer.Config<TServerContext>): Promise<WebServer<TServerContext>> {
    const clients = new RemoteClientManager<TServerContext>(config.context);
    const router = Router.parse(isFunction(config.routes) ? await config.routes(clients) : config.routes);
    const server: Bun.Server = Bun.serve<ClientWebSocket.Data<TServerContext>, any>({
      development: true,
      port: config.port,
      tls: {
        key: Bun.file(ProcessEnv.Certificates['localhost.key']),
        cert: Bun.file(ProcessEnv.Certificates['localhost.cert']),
      },
      fetch (req, server) {
        const url = new URL(req.url);
        const isUpgradeRequest = req.headers.get('upgrade');
        log.verbose(`Incoming Request: ${isUpgradeRequest ? 'WebSocket Upgrade' : `HTTP ${req.method}`} ${url.pathname}`);
        if (isUpgradeRequest) {
          const data = { client: null as RemoteClient<TServerContext> | null };
          if (server.upgrade(req, { data })) {
            return router.dispatch(req, data.client!);
          }
          return new Response('Bad Request', { status: 400 });
        }
        const { resolve, promise } = Promise.withResolvers<Response>();
        router.dispatch(req, null, resolve);
        return promise;
      },
      websocket: {
        open (ws: Bun.ServerWebSocket<ClientWebSocket.Data<TServerContext>>) { ws.data.client = clients.allocate(ws); },
        close (ws: Bun.ServerWebSocket<ClientWebSocket.Data<TServerContext>>) { ws.data.client.webSocketClosed(); },
        message (ws: Bun.ServerWebSocket<ClientWebSocket.Data<TServerContext>>, message: string | Buffer) { ws.data.client.webSocketMessage(message); },
        // drain (ws) { console.debug(`ws drain`); }
      },
    });

    const webServer: WebServer<TServerContext> = new WebServer(config, server, clients);

    log.info(`Server is running at ${server.url}`);

    return webServer;
  }

  constructor (config: WebServer.Config<TServerContext>, server: Server, clients: RemoteClientManager<TServerContext>) {
    this.#config = config;
    this.#server = server;
    this.#clients = clients;
  }
  readonly #config: WebServer.Config<TServerContext>;
  readonly #server: Server;
  readonly #clients: RemoteClientManager<TServerContext>;

  get clients () { return this.#clients; }
}
