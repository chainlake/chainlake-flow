from repo root:

docker build -f docker/Dockerfile -t ghcr.io/chainlake/rpcstream:uv .


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