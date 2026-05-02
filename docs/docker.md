# Docker notes

From the repository root:

```bash
docker build -t ghcr.io/chainlake/rpcstream:v0.2.0 .
```

## Local image workflow

```bash
docker build -t rpcstream:dev .
docker save rpcstream:dev -o rpcstream.tar
sudo k3s ctr images import rpcstream.tar
kubectl rollout restart deployment bsc-block-transaction -n ingestion
```

## Build shape

| Stage   | Purpose                                      |
| ------- | -------------------------------------------- |
| Builder | installs build tools and compiles deps       |
| Runtime | copies installed packages into a clean image |

The runtime image should contain the application and compiled Python
dependencies, but not `gcc` or other build tools.

## Dependency lockfile

After changing `pyproject.toml`, run `uv lock` to refresh `uv.lock`, then
`uv sync` to update the local environment.
