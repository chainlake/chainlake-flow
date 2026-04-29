from rpcstream.adapters.evm.processor import TraceProcessor


def test_trace_processor_accepts_debug_trace_result_list():
    processor = TraceProcessor()

    raw_value = [
        {
            "txHash": "0xabc",
            "result": {
                "type": "CALL",
                "from": "0xfrom",
                "to": "0xto",
                "value": "0x0",
                "gas": "0x5208",
                "gasUsed": "0x5208",
                "input": "0x",
                "output": "0x",
                "calls": [
                    {
                        "type": "STATICCALL",
                        "from": "0xto",
                        "to": "0xsub",
                        "value": "0x0",
                        "gas": "0x1000",
                        "gasUsed": "0x100",
                        "input": "0x",
                        "output": "0x",
                    }
                ],
            },
        }
    ]

    result = processor.process(123, raw_value)

    assert "trace" in result
    assert len(result["trace"]) == 2
    assert result["trace"][0]["trace_id"] == "0xabc_root"
    assert result["trace"][1]["parent_trace_id"] == "0xabc_root"
