##### Example 5: 0 node cluster, 1 peer successfully joins

- No nodes fully in the cluster
- Local state before (1): `{:pairs {}}`

- A wants to join
- B sends `prepare-join-cluster` (1)

- A plays the log

- A encounters (1)
  - Pre: `{:pair {}}`
  - Post: `{:pairs {}}`
  - A realizes it is the only peer in the cluster, promotes itself to fully joined status
  - flushes all outbox messages