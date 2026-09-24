# Deploying the hub behind a reverse proxy

This is how the hub runs on a shared box: a container on its own small docker network, which the reverse proxy also joins, a CDN in front terminating TLS, and the proxy routing by host name. Agents connect to `wss://<domain>/agent`.

The network is deliberately not the proxy's shared one. There, every other app's container could reach `fleetmon:7070` directly and skip the proxy's auth — so a single SSRF anywhere on the box would expose the page. On a network of two, the hub's allowlist (the subnet) admits only the proxy.

## One-time

1. `cp .env.deploy.example .env.deploy` and fill it in.
2. DNS: a proxied record for the domain pointing at the box.
3. `task deploy:token` — creates the agent token on the server, owned by the container's user, never printed.
4. `task deploy` once, which creates the `fleetmon` network.
5. `data/` next to the compose file holds the history database (created by `task deploy:pull`, owned by the container's user). Back it up if a month of history matters to you; the hub rebuilds nothing from it.
6. Join the reverse proxy to it — live with `docker network connect fleetmon <proxy-container>`, and persistently by adding it to the proxy's compose file as an external network.
7. Add a server block to the reverse proxy (below) and reload it.

## Each release

Merging to main publishes `ghcr.io/nhatvu148/fleetmon:latest`. Then:

```bash
task deploy
```

## Reverse proxy block

The page, `/ws` and `/api/hosts` sit behind basic auth — they show process names. `/agent` does not: an agent cannot answer a basic-auth prompt, and it is gated by the hub's own bearer token instead.

```nginx
server {
    listen 80;
    server_name fleetmon.example.com;

    resolver 127.0.0.11 valid=30s;

    # Agents: bearer token, checked by the hub.
    location = /agent {
        set $upstream fleetmon:7070;
        proxy_pass http://$upstream;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        # An agent sends every second; the hub drops one silent for 15 s.
        proxy_read_timeout 60s;
    }

    # Everything else: the page, its live socket, the JSON API.
    location / {
        auth_basic "fleetmon";
        auth_basic_user_file /etc/nginx/htpasswd;

        set $upstream fleetmon:7070;
        proxy_pass http://$upstream;
        proxy_http_version 1.1;
        proxy_set_header Host $host;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header Upgrade $http_upgrade;
        proxy_set_header Connection "upgrade";
        # The page's socket is quiet between samples, never for long, but a
        # reader with no agents online would otherwise be cut at 60 s.
        proxy_read_timeout 86400;
    }
}
```

## Pointing an agent at it

```bash
fleetmon-agent --hub wss://fleetmon.example.com --token-file fleetmon.token
```

`task deploy:token:fetch OUT=<path>` copies the token from the server to a local file for distribution.
