import { createServer } from "net";
import { sha1 } from "object-hash";
import { Client } from "ssh2";

const PROXY_HOST = "127.0.0.1";
const PROXY_PORT_RANGE_START = 55655;

const openProxies: { [key: string]: [string, number] } = {};

function openProxyKey(
  pgHost: string,
  pgPort: number,
  sshHost: string,
  sshPort: number,
  sshUsername: string
) {
  // Hash the values to avoid keeping them in memory
  return sha1({
    pgHost,
    pgPort,
    sshHost,
    sshPort,
    sshUsername,
  });
}

export async function sshProxy(
  pgHost: string,
  pgPort: number,
  sshHost: string,
  sshPort: number,
  sshUsername: string,
  sshPrivateKey: string,
  onConnectionError: (error: string) => void
): Promise<[string, number]> {
  const proxyHost = PROXY_HOST;
  let proxyPort = PROXY_PORT_RANGE_START;

  const proxyKey = openProxyKey(pgHost, pgPort, sshHost, sshPort, sshUsername);
  if (openProxies[proxyKey]) {
    return Promise.resolve(openProxies[proxyKey]);
  }

  return new Promise((resolve, reject) => {
    let sockets = 0;
    let lastAccess = Date.now();

    const proxy = createServer((sock) => {
      sockets += 1;
      lastAccess = Date.now();

      const connection = new Client();

      connection.on("ready", () => {
        if (sock.remoteAddress == null) {
          console.error("Empty sock.remoteAddress");
          return;
        }
        if (sock.remotePort == null) {
          console.error("Empty sock.remotePort");
          return;
        }

        connection.forwardOut(
          sock.remoteAddress,
          sock.remotePort,
          pgHost,
          pgPort,
          (_err, stream) => {
            sock.pipe(stream);
            stream.pipe(sock);
          }
        );
      });

      connection.on("error", (err) => {
        console.error(`Proxy on port ${proxyPort} connection error: ${err}`);
        onConnectionError(`Could not connect to SSH proxy: ${err}`);
      });

      sock.on("end", () => {
        setTimeout(() => {
          connection.end();
        }, 10000);
        sockets -= 1;
        console.log(`Proxy socket end on port ${proxyPort}: ${sockets}`);

        if (sockets == 0) {
          const checkpointLastAccess = lastAccess;
          setTimeout(() => {
            if (sockets == 0 && lastAccess == checkpointLastAccess) {
              console.log(
                `Closing proxy on port ${proxyPort} due to inactivity`
              );
              delete openProxies[proxyKey];
              proxy.close();
            }
          }, 10000);
        }
      });

      connection.connect({
        host: sshHost,
        port: sshPort,
        username: sshUsername,
        privateKey: sshPrivateKey,
      });
    });

    proxy.on("error", (err) => {
      console.log(`Proxy on port ${proxyPort} error: ${err}`);
      if ((err as { code?: string }).code == "EADDRINUSE") {
        console.log(`Port in use ${proxyPort} when trying to create proxy`);
        if (proxyPort > PROXY_PORT_RANGE_START + 100) {
          console.log(
            `Tried proxy ports up to ${proxyPort}, could not establish one`
          );
          reject();
        }
        setTimeout(() => {
          proxy.close();
          proxyPort += 1;
          proxy.listen(proxyPort, proxyHost);
        }, 200);
      }
    });

    proxy.on("listening", () => {
      console.log(`Proxy on port ${proxyPort} listening on ${proxyPort}`);
      openProxies[proxyKey] = [proxyHost, proxyPort];
      resolve([proxyHost, proxyPort]);
    });

    proxy.on("close", () => {
      console.log(`Proxy on port ${proxyPort} closed`);
    });

    proxy.listen(proxyPort, proxyHost);
  });
}
