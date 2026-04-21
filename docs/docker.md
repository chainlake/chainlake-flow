from repo root:

```bash
docker build -f Dockerfile.dev -t ghcr.io/chainlake/rpcstream:v0.3 .
docker build -t ghcr.io/chainlake/rpcstream:v0.2.0 .

image: rpcstream:dev
imagePullPolicy: Never
```
### Generate lockfile
`uv pip compile pyproject.toml -o uv.lock`

Dockerfile build: 

|                | Single-stage      | Multi-stage      |
| -------------- | ----------------- | ---------------- |
| Complexity     | simple ✅          | more complex     |
| Image size     | larger ❌          | smaller ✅        |
| Security       | worse ❌           | better ✅         |
| Build speed    | faster first time | better long-term |
| Production use | 😐 ok             | 🚀 recommended   |


# 1. build
docker build -t rpcstream:dev .

# 2. export
docker save rpcstream:dev -o .rpcstream.tar

# 3. import
sudo k3s ctr images import .rpcstream.tar

# 4. restart pod
kubectl rollout restart deployment bsc-ingestion-block

👉 Two containers:

Stage 1 (builder)
```
installs gcc
compiles dependencies (e.g. confluent-kafka)
builds everything
```

Stage 2 (runtime)
```
clean base image
ONLY copies installed Python packages
```
👉 Final image contains:
```
app ✅
compiled deps ✅
NO gcc ❌
NO build tools ❌
```

MUST run uv sync (or uv lock) after changing pyproject.toml