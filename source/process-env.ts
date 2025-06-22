import * as FS from 'fs';
import * as FSP from 'fs/promises';
import * as JSONC from 'jsonc-parser';
import * as Path from 'path';

let packageJsonDir: string;
let packageJsonPath: string;
let repoRootDir: string;
let dbDir: string;
let secrets: ProcessEnv.Secrets;
let certificates: ProcessEnv.Certificates;

let dbdirInitialized = false;

export class ProcessEnv {
  private constructor () {}

  /**
   * The absolute path of the closest directory containing a package.json file, starting at the directory containing the
   * script that launched the process, and recursively searching upwards towards the root of the filesystem.
   */
  static get PackageJsonDir () { return packageJsonDir!; }
  /**
   * The absolute path of the closest package.json file, starting at the directory containing the script that
   * launched the process, and recursively searching upwards towards the root of the filesystem.
   */
  static get PackageJsonPath () { return packageJsonPath!; }
  /**
   * The absolute path of the closest directory containing a .git folder, starting at the directory containing the
   * script that launched the process, and recursively searching upwards towards the root of the filesystem.
   */
  static get RepoRootDir () { return repoRootDir!; }
  static get DbDir () {
    if (!dbdirInitialized) {
      dbdirInitialized = true;
      FS.mkdirSync(dbDir, { recursive: true });
    }
    return dbDir;
  }
  static get Secrets () { return secrets!; }
  static get Certificates () { return certificates!; }

  static dbPath (nameOnly: string) { return Path.resolve(dbDir, `${nameOnly}.db`); }

  static resolve (...paths: string[]) { return Path.resolve(repoRootDir, ...paths); }
}
export namespace ProcessEnv {
  export interface Options {
    /**
     * - `false` or not specified: No attempt will be made to load a secrets file.
     * - `true`: Load the closest `secrets.json` file, starting at the directory containing the script that launched the
     *   process, and recursively searching upwards towards the root of the filesystem.
     * - `string`: Absolute path to a JSON file containing secrets. If the file is not found, an error will be thrown.
     */
    secrets?: boolean | string;
    certificates?: boolean/*  | string */;
  }
  export interface Secrets extends Readonly<Record<string, any>> {}
  export interface Certificates extends Readonly<Record<string, any>> {
    readonly 'localhost.key': string;
    readonly 'localhost.cert': string;
  }

  export async function initialize (options: Options = {}): Promise<void> {
    const MAIN_SCRIPT_URL = Bun.main;
    const MAIN_SCRIPT_DIR = Path.dirname(MAIN_SCRIPT_URL);
    packageJsonPath = await findClosestFileSystemPathTowardsRoot(MAIN_SCRIPT_DIR, 'package.json');
    packageJsonDir = Path.dirname(packageJsonPath);
    repoRootDir = await findClosestDirContainingFileOrDirName(packageJsonDir, '.git');
    dbDir = Path.resolve(repoRootDir, '.db');
    if (options.secrets) {
      const secretsPath = await findClosestFileSystemPathTowardsRoot(packageJsonDir, 'secrets.json');
      secrets = loadJSONCFile<Secrets>(secretsPath);
    }
    if (options.certificates) {
      const keyPath = await findClosestFileSystemPathTowardsRoot(packageJsonDir, 'key.pem');
      const certPath = Path.join(Path.dirname(keyPath), 'cert.pem');
      certificates = {
        'localhost.key': keyPath,
        'localhost.cert': certPath,
      };
    }
  }

  function loadJSONCFile<T extends Record<string, any>> (filePath: string): T {
    const fileContents = FS.readFileSync(filePath, 'utf8');
    const errors: JSONC.ParseError[] = [];
    const output = JSONC.parse(fileContents, errors, { allowTrailingComma: true });
    if (errors.length > 0) {
      const error = errors[0];
      throw new Error(`Error parsing ${filePath} at offset ${error.offset}: ${JSONC.printParseErrorCode(error.error)}`);
    }
    return output;
  }

  export async function findClosestFileSystemPathTowardsRoot (filename: string): Promise<string>;
  export async function findClosestFileSystemPathTowardsRoot (dir: string, filename: string): Promise<string>;
  export async function findClosestFileSystemPathTowardsRoot (arg0: string, arg1?: string): Promise<string> {
    const [dir, filename] = arguments.length === 1 ? [Path.dirname(Bun.main), arg0] : [arg0, arg1!];
    try {
      const filePath = Path.join(dir, filename);
      await FSP.access(filePath, FSP.constants.R_OK);
      return filePath;
    }
    catch {
      const parentDir = Path.resolve(dir, '..');
      if (parentDir === dir) {
        throw new Error(`No file named ${filename} was found in ${dir} or any directory above it.`);
      }
      return findClosestFileSystemPathTowardsRoot(parentDir, filename);
    }
  }

  /**
   * Finds the nearest directory containing a file or directory with the specified name. The search begins at the
   * directory containing the script that launched the process, and recursively searching upwards towards the root of
   * the filesystem.
   */
  export async function findClosestDirContainingFileOrDirName (name: string): Promise<string>;
  /**
   * Finds the nearest directory containing a file or directory with the specified name, starting from the specified
   * directory. The search recursively goes upwards towards the root of the filesystem.
   */
  export async function findClosestDirContainingFileOrDirName (dir: string, name: string): Promise<string>;
  export async function findClosestDirContainingFileOrDirName (arg0: string, arg1?: string): Promise<string> {
    const [dir, name] = arguments.length === 1 ? [Path.dirname(Bun.main), arg0] : [arg0, arg1!];
    try {
      const subdirPath = Path.join(dir, name);
      await FSP.access(subdirPath, FSP.constants.R_OK);
      return dir;
    }
    catch {
      const parentDir = Path.resolve(dir, '..');
      if (parentDir === dir) {
        throw new Error(`No subdirectory named ${name} was found in ${dir} or any directory above it.`);
      }
      return findClosestDirContainingFileOrDirName(parentDir, name);
    }
  }
}
