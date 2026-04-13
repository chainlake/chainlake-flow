from rpcstream.utils import hex_to_dec

# trace (trace_block)
def parse_trace_block(traces: list, block_number: int):
    rows = []

    for t in traces:
        action = t.get("action") or {}
        result = t.get("result") or {} # result being None is NORMAL behavior
        
        trace_address = t.get("traceAddress") or []

        # unified trace_id logic (same as debug_trace)
        tx_hash = t.get("transactionHash")
        trace_id = build_trace_id(tx_hash, trace_address)

        # parent_trace_id (for graph consistency)
        parent_trace_id = None
        if trace_address:
            parent_trace_id = build_trace_id(tx_hash, trace_address[:-1])
        
        rows.append({
            # identity
            "block_number": block_number,
            "transaction_hash": t.get("transactionHash"),

            # trace identity
            "trace_id": trace_id,
            "parent_trace_id": parent_trace_id,
            "trace_index": t.get("transactionPosition"),
            
            # addresses
            "from_address": action.get("from"),
            "to_address": action.get("to"),
            
            # value + data
            "value": hex_to_dec(action.get("value")),
            "input": action.get("input"),
            "output": result.get("output"),
            
            # execution
            "call_type": action.get("callType"),
            "trace_type": t.get("type"),
            "status": result.get("status"),
            "error": t.get("error"),
            
            # gas
            "gas": hex_to_dec(action.get("gas")),
            "gas_used": hex_to_dec(result.get("gasUsed")),
            
            # structure
            "depth": len(t.get("traceAddress", [])),
            "subtraces": t.get("subtraces"),
            
            # unified fields
            "reward_type": action.get("rewardType"),
            "trace_address": trace_address,
            
            # origin metadata ⭐
            "trace_source": "parity",
            "trace_method": "trace_block",
            
            
        })
    return rows


# trace (debug_traceBlockByNumber)
def parse_debug_trace_block(traces, block_number):
    rows = []

    for tx in traces:
        tx_hash = tx.get("txHash") or tx.get("transactionHash")

        result = tx.get("result")
        if not result:
            continue

        rows.extend(
            flatten_call(result, block_number, tx_hash)
        )

    return rows

def flatten_call(call, block_number, tx_hash, depth=0, parent_trace_id=None, trace_address=None):
    rows = []

    if trace_address is None:
        trace_address = []
    
    # deterministic trace_id
    trace_id = build_trace_id(tx_hash, trace_address)

    rows.append({
        "block_number": block_number,
        "transaction_hash": tx_hash,

        "trace_id": trace_id,
        "parent_trace_id": parent_trace_id,
        "trace_index": None,

        "from_address": call.get("from"),
        "to_address": call.get("to"),
        
        "value": hex_to_dec(call.get("value")),
        "input": call.get("input"),
        "output": call.get("output"),

        "call_type": call.get("type"),
        "trace_type": "call",
        "status": None,
        "error": call.get("error"),
        
        "gas": hex_to_dec(call.get("gas")),
        "gas_used": hex_to_dec(call.get("gasUsed")),

        "depth": depth,
        "subtraces": len(call.get("calls", [])),

        # unified fields
        "reward_type": None,
        "trace_address": trace_address.copy(),
            
        "trace_source": "geth",
        "trace_method": "debug_trace",
    })

    for i, subcall in enumerate(call.get("calls", []) or []):
        rows.extend(
            flatten_call(
                subcall,
                block_number,
                tx_hash,
                depth + 1,
                trace_id,
                trace_address + [i]
            )
        )

    return rows

# Trace parser dispatcher (clean entrypoint)
def parse_traces_auto(value, block_number, trace_type):
    if trace_type == "trace_block":
        return parse_trace_block(value, block_number)
    elif trace_type == "debug_trace":
        return parse_debug_trace_block(value, block_number)
    else:
        raise ValueError(f"unknown trace_type: {trace_type}")
    
# helper function
def build_trace_id(tx_hash, trace_address):
    if not trace_address:
        return f"{tx_hash}_root"
    return f"{tx_hash}_{'_'.join(map(str, trace_address))}"