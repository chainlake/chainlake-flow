batch mode
streaming mode
checkpoint manager
retry queue
DLQ sink
Kafka EOS guarantee
reorg-aware block versioning

cli:
## bounded backfill
rpcstream --from 90000001 --to 90000100

## realtime from a specific cursor
rpcstream --from 90000001

## default realtime from checkpoint or latest
rpcstream
