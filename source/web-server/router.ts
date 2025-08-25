import { isDefined, isFunction, isNotNothing, isUndefined } from '@xf-common/general/type-checking.ts';
import type { RemoteClient } from './remote-clients.ts';
import { WebRequest } from './web-request.ts';

export type RouteSpec = RouteRequestHandler | RoutePathMapSpec | RouteMethodMapSpec;
export interface RouteRequestHandler {
  (request: WebRequest, pathIndex: number): void | Promise<void>;
}
export type RoutePathMapSpec = {
  [key: `/${string}`]: RouteSpec;
};
export interface RouteMethodMapSpec {
  readonly WS?: RouteMethodSpec;
  readonly GET?: RouteMethodSpec;
  readonly POST?: RouteMethodSpec;
  readonly PUT?: RouteMethodSpec;
  readonly DELETE?: RouteMethodSpec;
  readonly PATCH?: RouteMethodSpec;
  readonly OPTIONS?: RouteMethodSpec;
  readonly HEAD?: RouteMethodSpec;
  readonly CONNECT?: RouteMethodSpec;
  readonly TRACE?: RouteMethodSpec;
  readonly '*'?: RouteMethodSpec;
}
export type RouteMethodType = keyof RouteMethodMapSpec;
export namespace RouteMethodType {
  export const WS = 'WS';
  export const GET = 'GET';
  export const POST = 'POST';
  export const PUT = 'PUT';
  export const DELETE = 'DELETE';
  export const PATCH = 'PATCH';
  export const OPTIONS = 'OPTIONS';
  export const HEAD = 'HEAD';
  export const CONNECT = 'CONNECT';
  export const TRACE = 'TRACE';
}
export type RouteMethodSpec = RouteRequestHandler | RouteContentTypeMapSpec;
export interface RouteContentTypeMapSpec extends SRecord.Of<RouteRequestHandler> {}

export class Router {
  static readonly parse = (spec: any) => {
    return new Router(Route.parse(spec));
  };

  constructor (mainRoute: Route) {
    this._main = mainRoute;
  }

  private readonly _main: Route;

  async dispatch (request: Request, client: RemoteClient | null, respond?: (response: Response) => void) {
    const pathstr = new URL(request.url, 'https://localhost').pathname;
    const path = pathstr.substring(1).split('?')[0].split('/').filter(p => p !== '');
    const wreq = new WebRequest(request, path, client, respond);
    try {
      await this._main.next(wreq, 0);
    }
    catch (e) {
      wreq.serveInternalError(`An error occurred while processing the request.`);
      console.error(e);
      return;
    }
    if (wreq.unhandled) {
      if (wreq.hasWebSocketClient) wreq.client.reject();
      else wreq.serveNotFound();
    }
  }
}

// Same as RouteMethodMapSpec, but with RouteRequestHandler replaced with RouteDispatcher
interface RouteMethodMap {
  readonly [RouteMethodType.WS]?: RouteDispatcher;
  readonly [RouteMethodType.GET]?: RouteDispatcher;
  readonly [RouteMethodType.POST]?: RouteDispatcher;
  readonly [RouteMethodType.PUT]?: RouteDispatcher;
  readonly [RouteMethodType.DELETE]?: RouteDispatcher;
  readonly [RouteMethodType.PATCH]?: RouteDispatcher;
  readonly [RouteMethodType.OPTIONS]?: RouteDispatcher;
  readonly [RouteMethodType.HEAD]?: RouteDispatcher;
  readonly [RouteMethodType.CONNECT]?: RouteDispatcher;
  readonly [RouteMethodType.TRACE]?: RouteDispatcher;
  readonly '*'?: RouteDispatcher;
}

interface RouteDispatcher {
  next (request: WebRequest, pathIndex: number): Promise<void>;
}

const isRouteMethodMapSpec = (spec: any): spec is RouteMethodMapSpec => {
  for (const key in spec) {
    return key === '*' || key in RouteMethodType;
  }
  return false;
};

class Route implements RouteDispatcher {
  static parse (spec: RouteSpec) {
    if (isFunction(spec)) {
      const exactHandler = new RouteHandler(spec);
      return new Route(exactHandler);
    }
    let exactHandler!: RouteDispatcher;
    let catchallHandler!: RouteDispatcher;

    if (isRouteMethodMapSpec(spec)) {
      const exactHandler = SuccessfulRoutePathMatch.parse(spec);
      return new Route(exactHandler);
    }

    const routes: SRecord.Of<RouteSpec> = {};
    for (let key in spec) {
      const subspec = spec[key as keyof RoutePathMapSpec];
      key = key.replace(/^(\/+)|(\/+$)/g, '');
      switch (key) {
        case '':
          exactHandler = SuccessfulRoutePathMatch.parse(subspec);
          break;
        case '*':
          catchallHandler = SuccessfulRoutePathMatch.parse(subspec);
          break;
        default:
          const slashIndex = key.indexOf('/');
          if (slashIndex !== -1) {
            const head = key.substring(0, slashIndex);
            const tail = key.substring(slashIndex);
            routes['/' + head] = { [tail]: subspec };
          }
          else {
            routes['/' + key] = subspec;
          }
          break;
      }
    }
    return isDefined(routes)
      ? new Route(exactHandler, catchallHandler, RouteMap.parse(routes))
      : new Route(exactHandler, catchallHandler);
  }

  constructor (exactHandler: RouteDispatcher, catchallHandler?: RouteDispatcher, routes?: RouteDispatcher) {
    this._exact = exactHandler ?? ROUTE_NOOP;
    this._catchall = catchallHandler ?? ROUTE_NOOP;
    this._routes = routes ?? ROUTE_NOOP;
  }

  private readonly _exact: RouteDispatcher;
  private readonly _catchall: RouteDispatcher;
  private readonly _routes: RouteDispatcher;

  async next (request: WebRequest, pathIndex: number) {
    if (request.isPathEnd(pathIndex)) {
      await this._exact.next(request, pathIndex);
    }
    else {
      await this._routes.next(request, pathIndex);
    }
    if (request.unhandled) {
      await this._catchall.next(request, pathIndex);
    }
  }
}

class SuccessfulRoutePathMatch implements RouteDispatcher {
  static parse (spec: RouteRequestHandler | RouteMethodMapSpec) {
    if (isFunction(spec)) return new RouteHandler(spec);
    const methods: RouteMethodMap = {};
    let catchall = ROUTE_NOOP;
    for (const key in spec) {
      if (key === '*') catchall = RouteMethod.parse(spec[key]!);
      methods[key] = RouteMethod.parse(spec[key]);
    }
    return new SuccessfulRoutePathMatch(methods, catchall);
  }

  constructor (methods: RouteMethodMap, catchall: RouteDispatcher) {
    this._methods = methods;
    this._catchall = catchall;
  }

  private readonly _methods: RouteMethodMap;
  private readonly _catchall: RouteDispatcher;

  async next (request: WebRequest, pathIndex: number) {
    let handler: RouteDispatcher;
    if (request.hasWebSocketClient) {
      request.setHandled();
      handler = this._methods[RouteMethodType.WS]!;
      if (isUndefined(handler)) {
        request.client.reject();
        return;
      }
    }
    else {
      handler = this._methods[request.method] ?? this._catchall;
    }
    await handler.next(request, pathIndex);
  }
}

class RouteMethod implements RouteDispatcher {
  static parse (spec: RouteMethodSpec) {
    if (isFunction(spec)) return new RouteHandler(spec);
    let contentTypes: SRecord.Of<RouteDispatcher> | undefined;
    let catchallHandler: RouteDispatcher | undefined;
    for (const key in spec) {
      if (key === '*') {
        catchallHandler = RouteHandler.parse(spec[key]);
      }
      else {
        if (isUndefined(contentTypes)) contentTypes = {};
        contentTypes[key] = RouteHandler.parse(spec[key]);
      }
    }
    return isUndefined(contentTypes)
      ? catchallHandler ?? ROUTE_NOOP
      : new RouteMethod(contentTypes, catchallHandler);
  }

  constructor (contentTypes: Record<string, RouteDispatcher>, catchallHandler: RouteDispatcher = ROUTE_NOOP) {
    this._contentTypes = contentTypes;
    this._catchallHandler = catchallHandler;
  }

  private readonly _contentTypes: Record<string, RouteDispatcher>;
  private readonly _catchallHandler: RouteDispatcher;

  async next (request: WebRequest, pathIndex: number) {
    const { accept } = request;
    for (let j = 0; j < accept.length; ++j) {
      const handler = this._contentTypes[accept[j]];
      if (isDefined(handler)) {
        await handler!.next(request, pathIndex);
        break;
      }
    }
    if (request.unhandled) {
      await this._catchallHandler.next(request, pathIndex);
    }
  }
}

namespace RouteMap {
  export interface Entry {
    readonly rx: RegExp;
    readonly route: Route;
  }
}
class RouteMap implements RouteDispatcher {
  static parse (spec: any) {
    const staticRoutes = {};
    const rxRoutes: RouteMap.Entry[] = [];
    for (const key in spec) {
      if (key[0] !== '/') continue;
      const route = Route.parse(spec[key]);
      if (key[key.length - 1] === '/') rxRoutes.push({ rx: new RegExp(key.substr(1, key.length - 2)), route });
      else staticRoutes[key.slice(1)] = route;
    }
    return new RouteMap(staticRoutes, rxRoutes);
  }

  constructor (staticRoutes: { [key: string]: Route }, regexpRoutes: readonly RouteMap.Entry[]) {
    this._staticRoutes = staticRoutes;
    this._regexpRoutes = regexpRoutes;
  }

  private readonly _staticRoutes: { [key: string]: Route };
  private readonly _regexpRoutes: readonly RouteMap.Entry[];

  async next (request: WebRequest, pathIndex: number) {
    const pathSegment = request.pathSegment(pathIndex);
    const route = this._staticRoutes[pathSegment];
    if (isNotNothing(route)) await route.next(request, pathIndex + 1);
    for (let rxi = 0; rxi < this._regexpRoutes.length; ++rxi) {
      const entry = this._regexpRoutes[rxi];
      if (entry.rx.test(pathSegment)) {
        await entry.route.next(request, pathIndex + 1);
        return;
      }
    }
  }
}

class RouteHandler implements RouteDispatcher {
  static parse (spec: any) {
    return new RouteHandler(spec);
  }

  private readonly _handler: RouteRequestHandler;
  constructor (handler: RouteRequestHandler) {
    this._handler = handler;
  }
  async next (request: WebRequest, pathIndex: number) {
    const handler = this._handler;
    await handler(request, pathIndex);
  }
}

const ROUTE_NOOP = new class RouteNoop implements RouteDispatcher {
  async next (request: WebRequest, i: number) {}
}();
