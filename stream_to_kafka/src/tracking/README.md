| 情况                              | 处理                              |
| ------------------------------- | ------------------------------- |
| first value                     | accept                          |
| new == current                  | ignore                          |
| new > current + max_forward_gap | cap 到 current + max_forward_gap |
| current - new <= max_reorg_tol  | accept（reorg）                   |
| current - new > max_reorg_tol   | reject（脏 RPC）                   |
