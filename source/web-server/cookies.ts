import { isNotNothing, isNothing } from '@xf-common/general/type-checking.ts';

// Cookie parser/serializer credit: https://stackoverflow.com/a/62157790/98389
export function getEntriesFromCookie (cookieString: string | Nothing) {
  if (isNothing(cookieString)) return {};
  return Object.fromEntries(cookieString.split(';').map((pair) => {
    const indexOfEquals = pair.indexOf('=');
    let name: string;
    let value: string;
    if (indexOfEquals === -1) {
      name = '';
      value = pair.trim();
    }
    else {
      name = pair.substring(0, indexOfEquals).trim();
      value = pair.substring(indexOfEquals + 1).trim();
    }
    const firstQuote = value.indexOf('"');
    const lastQuote = value.lastIndexOf('"');
    if (firstQuote !== -1 && lastQuote !== -1) {
      value = value.substring(firstQuote + 1, lastQuote);
    }
    return [name, value];
  }));
}

export interface CookieOptions {
  name?: string;
  value?: string;
  expires?: Date;
  maxAge?: number;
  domain?: string;
  path?: string;
  secure?: boolean;
  httpOnly?: boolean;
  sameSite?: 'strict' | 'lax' | 'none';
}
export function createSetCookie (options: CookieOptions) {
  const cookieParts: string[] = [];
  if (options.name) cookieParts.push(`${options.name}=${options.value || ''}`);
  if (isNotNothing(options.expires)) cookieParts.push(`Expires=${options.expires.toUTCString()}`);
  if (isNotNothing(options.maxAge)) cookieParts.push(`Max-Age=${options.maxAge}`);
  if (isNotNothing(options.domain)) cookieParts.push(`Domain=${options.domain}`);
  if (isNotNothing(options.path)) cookieParts.push(`Path=${options.path}`);
  if (options.secure) cookieParts.push('Secure');
  if (options.httpOnly) cookieParts.push('HttpOnly');
  if (isNotNothing(options.sameSite)) cookieParts.push(`SameSite=${options.sameSite}`);
  return cookieParts.join('; ');
}
