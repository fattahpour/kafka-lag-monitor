import { MtlsConfig } from './types';

export interface TlsConnectionOptions {
  ca?: string;
  cert?: string;
  key?: string;
  passphrase?: string;
}

/**
 * Builds the value for kafkajs's `ssl` option.
 *
 * @param ssl The profile's `ssl` field (boolean or MtlsConfig).
 * @param readFile Reads a file's contents as utf-8 text. Production code passes
 *                  `(path) => fs.readFileSync(path, 'utf-8')`; injected here for testability.
 * @param passphrase Optional passphrase for an encrypted private key (mTLS only).
 */
export function buildSslOptions(
  ssl: boolean | MtlsConfig,
  readFile: (path: string) => string,
  passphrase?: string,
): boolean | TlsConnectionOptions {
  if (typeof ssl === 'boolean') {
    return ssl;
  }

  const options: TlsConnectionOptions = {
    cert: readCertFile(readFile, ssl.cert, 'cert'),
    key: readCertFile(readFile, ssl.key, 'key'),
  };
  if (ssl.ca) {
    options.ca = readCertFile(readFile, ssl.ca, 'ca');
  }
  if (passphrase) {
    options.passphrase = passphrase;
  }
  return options;
}

function readCertFile(readFile: (path: string) => string, path: string, field: 'ca' | 'cert' | 'key'): string {
  try {
    return readFile(path);
  } catch (err) {
    throw new Error(`Failed to read TLS "${field}" file "${path}": ${(err as Error).message}`);
  }
}
