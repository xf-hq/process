import { isNotNull } from '@xf-common/general/type-checking.ts';
import { getEntriesFromCookie } from './cookies.ts';
import { RemoteClient } from './remote-clients.ts';

export class WebRequest {
  constructor (request: Request, path: string[], client: RemoteClient | null, respond?: (response: Response) => void) {
    this.#request = request;
    this.#path = path;
    this.#client = client;
    this.#respond = respond;
  }
  readonly #request: Request;
  readonly #path: readonly string[];
  readonly #client: RemoteClient | null;
  readonly #respond: ((response: Response) => void) | undefined;
  #headers: RequestHeaders;
  #body: RequestBody;
  #handled = false;

  get request () { return this.#request; }
  get path () { return this.#path; }
  get url () { return this.#request.url; }
  get handled () { return this.#handled; }
  get unhandled () { return !this.#handled; }
  get accept () { return this.headers.accept; }

  get method () { return this.#request.method; }
  get headers () { return this.#headers ??= new RequestHeaders(this.#request); }
  get body () { return this.#body ??= new RequestBody(this.#request); }
  get hasWebSocketClient () { return isNotNull(this.#client); }
  get client () { return this.#client!; }

  serve (response: Response) {
    this.setHandled();
    this.#respond!(response);
  }

  urlPathFrom (i: number) { return '/' + this.#path.slice(i).join('/'); }
  pathSegment (i: number) { return this.#path[i]; }
  isPathEnd (i: number) { return i === this.#path.length; }

  setHandled () {
    this.#handled = true;
  }

  #assertNotAlreadyHandled () {
    if (this.#handled) {
      throw new Error('ASSERTION FAILED: This request has already been handled.');
    }
  }

  redirectIfMissingTrailingSlash (): boolean {
    if (this.url.endsWith('/')) return false;
    this.redirect(this.url + '/');
    return true;
  }

  redirect (url: string) {
    this.#assertNotAlreadyHandled();
    this.#respond!(new Response(null, { status: 302, headers: { Location: url } }));
  }

  serveStatusOnly (statusCode: number, message: string) {
    this.#assertNotAlreadyHandled();
    this.#respond!(new Response(null, { status: statusCode, statusText: message }));
  }

  serveContent (contentType: string, body: string | Bun.BodyInit) {
    this.#assertNotAlreadyHandled();
    this.#respond!(new Response(body, { headers: { 'Content-Type': contentType } }));
  }

  serveAcknowledgement (message = 'OK') {
    this.serveStatusOnly(200, message);
  }

  serveNoContent (message = 'No Content') {
    this.serveStatusOnly(204, message);
  }

  serveNotFound (message = 'Not Found') {
    return this.serveStatusOnly(404, message);
  }

  serveBadRequest (message = 'Bad Request') {
    return this.serveStatusOnly(400, message);
  }

  serveInternalError (message = 'Internal Error') {
    return this.serveStatusOnly(500, message);
  }

  serveStream (stream: ReadableStream) { this.serveContent('application/octet-stream', stream); }
  serveText (text: string) { this.serveContent('text/plain', text); }
  serveNumber (number: number) { this.serveContent('text/plain', number.toString()); }
  serveJSON<T> (data: T) { this.serveContent('application/json', JSON.stringify(data)); }
  serveJS (js: string) { this.serveContent('application/javascript', js); }
  serveHTML (html: string) { this.serveContent('text/html', html); }
  serveCSS (css: string) { this.serveContent('text/css', css); }
  serveSVG (svg: string) { this.serveContent('image/svg+xml', svg); }
}

class RequestHeaders {
  constructor (request: Request) {
    this.#request = request;
  }

  readonly #request: Request;
  #accept: readonly string[] | undefined;
  #cookies: SRecord.Of<string> | undefined;

  get accept () { return this.#accept ??= this.#parseAccept(); }
  get cookies () { return this.#cookies ??= getEntriesFromCookie(this.#request.headers.get('cookie')); }

  get (name: string) { return this.#request.headers.get(name); }

  #parseAccept () {
    const [main] = (this.#request.headers.get('accept') ?? '').split(';');
    return main.split(',');
  }
}

class RequestBody {
  constructor (request: Request) {
    this.#request = request;
  }

  readonly #request: Request;

  async text () {
    return this.#request.text();
  }

  async json<T> (): Promise<T> {
    return this.#request.json() as T;
  }
}
