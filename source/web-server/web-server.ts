import { isFunction } from '@xf-common/general/type-checking.ts';
import type { BunFile, Server } from 'bun';
import { ProcessEnv } from '../process-env.ts';
import { RemoteClient, RemoteClientManager, type ClientWebSocket, type RemoteClients } from './remote-clients.ts';
import { Router, type RoutePathMapSpec } from './router.ts';
import { terminal } from '@xf-common/terminal/terminal';

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
    const server = Bun.serve<ClientWebSocket.Data<TServerContext>, any>({
      port: config.port,
      // tls: {
      //   key: Bun.file(ProcessEnv.Certificates['localhost.key']),
      //   cert: Bun.file(ProcessEnv.Certificates['localhost.cert']),
      // },
      async fetch (req) {
        log.verbose(`Incoming HTTP request: ${req.method} ${req.url}`);
        const data = { client: null as RemoteClient<TServerContext> | null };

        if (server.upgrade(req, { data })) {
          log.verbose(`Incoming WebSocket connection: ${req.url}`);
          router.dispatch(req, data.client!);
        }
        else {
          return await new Promise<Response>((resolve) => {
            router.dispatch(req, null, resolve);
          });
        }
      },
      websocket: {
        open (ws) { ws.data.client = clients.allocate(ws); },
        close (ws) { ws.data.client.webSocketClosed(); },
        message (ws, message) { ws.data.client.webSocketMessage(message); },
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
