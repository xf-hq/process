import { isDefined } from '@xf-common/general/type-checking.ts';
import FS from 'fs';
import Path from 'path';
import type { RouteMethodMapSpec } from './router.ts';
import { WebRequest } from './web-request.ts';

export namespace StaticFiles {
  export function initializeRoute (absolutePublicPath: string, relativeSubPath?: string): RouteMethodMapSpec {
    if (isDefined(relativeSubPath)) absolutePublicPath = Path.join(absolutePublicPath, relativeSubPath);
    return {
      GET: async (request: WebRequest, pathIndex: number) => {
        await handleRequest(absolutePublicPath, request, pathIndex);
      },
    };
  }
  export async function handleRequest (absoluteBasePath: string, request: WebRequest, pathIndex: number) {
    const relativePath = request.urlPathFrom(pathIndex);
    const absolutePath = Path.join(absoluteBasePath, relativePath);
    if (!absolutePath.startsWith(absoluteBasePath)) return request.serveNotFound();
    await serve(absolutePath, request);
  }
  export async function serve (absolutePath: string, request: WebRequest) {
    if (!FS.existsSync(absolutePath)) {
      return request.serveNotFound();
    }

    // const mime = Mime.ext(Path.extname(absolutePath));
    // const responseHeaders: Record<string, string> = {};
    // if (isDefined(mime)) {
    //   responseHeaders['Content-Type'] = mime.contentType;
    // }
    let response: Response;
    const range = request.headers.get('range');
    if (range) {
      const responseHeaders: Record<string, string> = {};

      const { size } = await FS.promises.stat(absolutePath);
      const [sStart, sEnd] = range.replace(/bytes=/, '').split('-');
      let start = parseInt(sStart, 10);
      let end = sEnd ? parseInt(sEnd, 10) : size - 1;

      if (!isNaN(start) && isNaN(end)) {
        end = size - 1;
      }
      if (isNaN(start) && !isNaN(end)) {
        start = size - end;
        end = size - 1;
      }
      if (start >= size || end >= size) {
        responseHeaders['Content-Range'] = `bytes */${size}`;
        response = new Response(null, { status: 416, headers: responseHeaders });
      }
      else {
        responseHeaders['Content-Range'] = `bytes ${start}-${end}/${size}`;
        responseHeaders['Accept-Ranges'] = 'bytes';
        responseHeaders['Content-Length'] = String(end - start + 1);

        response = new Response(Bun.file(absolutePath).slice(start, end + 1), { status: 206, headers: responseHeaders });
      }
    }
    else {
      response = new Response(Bun.file(absolutePath));
    }
    request.serve(response);
  }
}
