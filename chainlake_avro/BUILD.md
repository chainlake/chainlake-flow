# chainlake_avro — build guide

PyO3 extension for GIL-free parallel Avro encoding.

## Prerequisites

```bash
# Install Rust toolchain (one-time)
curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh
source "$HOME/.cargo/env"

# Install maturin into the project venv
source .venv/bin/activate
pip install maturin
```

## Development build

```bash
source .venv/bin/activate
cd chainlake_avro
maturin develop --release   # builds .so in-place inside .venv
```

After this, `import chainlake_avro` works from anywhere in the venv and
`KafkaWriter._encode_batch_sync` will automatically use the Rust path.

## Dockerfile integration

Add to the relevant stage in `Dockerfile`:

```dockerfile
# --- Rust toolchain (only needed during build) ---
RUN curl --proto '=https' --tlsv1.2 -sSf https://sh.rustup.rs | sh -s -- -y --default-toolchain stable
ENV PATH="/root/.cargo/bin:${PATH}"

# Build and install the Avro extension
RUN pip install maturin && \
    cd /app/chainlake_avro && \
    maturin build --release -o /tmp/wheels && \
    pip install /tmp/wheels/*.whl
```

Or with `maturin develop`:
```dockerfile
RUN cd /app/chainlake_avro && maturin develop --release
```

## Verifying the extension is active

```python
import chainlake_avro
print(chainlake_avro.__file__)  # shows path to .so
```

Or look for `rust_encoder_available=True` in `kafka.schema_registry_warmup_complete` logs.

## Fallback behaviour

If the .so is not installed, `_RUST_ENCODER_AVAILABLE = False` in `protobuf.py`
and `KafkaWriter._encode_batch_sync` falls back to the original pure-Python path
transparently. No configuration change required.
