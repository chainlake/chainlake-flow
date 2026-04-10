# install grpcurl

##  download
```bash
wget https://github.com/fullstorydev/grpcurl/releases/download/v1.9.3/grpcurl_1.9.3_linux_x86_64.tar.gz
```

## uncompress
```bash
tar -xzf grpcurl_1.9.3_linux_x86_64.tar.gz
```

## add to path
```bash
sudo mv grpcurl /usr/local/bin/
```

## remove source file
```bash
rm grpcurl_1.9.3_linux_x86_64.tar.gz LICENSE
```


## test grp connection using grpcurl
```bash
grpcurl \
  -proto sui/rpc/v2/ledger_service.proto \
  -H "x-token: " \
  -d '{}' \
  fullnode.mainnet.sui.io:443 \
  sui.rpc.v2.LedgerService/GetServiceInfo

# or
grpcurl \
  -d '{}' \
  -H "x-token: " \
  fullnode.mainnet.sui.io:443 \
  sui.rpc.v2.LedgerService/GetServiceInfo
```