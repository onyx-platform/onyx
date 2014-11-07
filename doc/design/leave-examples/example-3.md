##### Example 3: 3 node cluster, 1 joins with garbage collection                                                                                            

- Nodes A, B, and C fully in the cluster
- All peers are dead, none could report peer death
- D sends `peer-gc` (1)
- Local state before (1): `{:pairs {:a :b, :b :c, :c :a}}`

- D plays the log

- D encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a}}`
  - D notices that A, B, and C are all dead
  - D sends `leave-cluster` (2), `leave-cluster` (3), and `leave-cluster` (4)
  - D sends `prepare-join-cluster` (5)

- D encounters (2)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :c, :c :a}}`
  - Command for B's death. A transitively fills the hole

- D encounters (3)
  - Pre: `{:pairs {:a :c, :c :a}}`
  - Post: `{:pairs {}}`
  - Command for A's death. C becomes the lone cluster member

- D encounters (4)
  - Pre: `{:pairs {}}`
  - Post: `{:pairs {}}`
  - Command for C's death. The cluster is now empty

- D encounters (5)
  - Pre: `{:pairs {}}`
  - Post: `{:pairs {}}`
  - D is the only node in the cluster. Promotes itself to full status
  - D flushes its outbox