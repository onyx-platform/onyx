##### Example 3: 2 node cluster, 1 peer successfully joins, 1 aborts

- Nodes A and B fully in the cluster                                                                                                                        
- Local state before (1): `{:pairs {:a :b, :b :a}}`

- C and D want to join
- C sends `prepare-join-cluster` (1)
- D sends `prepare-join-cluster` (2)

- A, B, C, and D play the log

- C encounters (1)
  - Pre: `{:pairs {:a :b, :b :a}}`
  - Post: `{:pairs {:a :b, :b :a} :prepared {:c :b}}`
  - determines C will attach to B. Records this in the local state.
  - adds watch to B
  - sends `notify-join-cluster` (3)

- A, B, and D encounter (1)
  - Pre: `{:pairs {:a :b, :b :a}}`
  - Post: `{:pairs {:a :b, :b :a} :prepared {:c :b}}`
  - determines C will attach to B. Records this in the local state.
  - no reactions

- D encounters (2)
  - Pre: `{:pairs {:a :b, :b :a} :prepared {:c :b}}`
  - Post: `{:pairs {:a :b, :b :a} :prepared {:c :b}}`
  - determines D cannot attach to any nodes. Aborts.
  - possibly sends another `prepare-join-cluster` after waiting for a period
  - in this example, the peer simply aborts and never retries

- A, B, and C encounter (2)
  - Pre: `{:pairs {:a :b, :b :a} :prepared {:c :b}}`
  - Post: `{:pairs {:a :b, :b :a} :prepared {:c :b}}`
  - determines D cannot attach to any nodes.
  - no reactions

- A encounters (3)
  - Pre: `{:pairs {:a :b, :b :a} :prepared {:c :b}}`
  - Post: `{:pairs {:a :b, :b :a} :accepted {:c :b}}`
  - adds a watch to C
  - removes its watch from B
  - sends `accept-join-cluster` (4)

- B and C encounter (3)
  - Pre: `{:pairs {:a :b, :b :a} :prepared {:c :b}}`
  - Post: `{:pairs {:a :b, :b :a} :accepted {:c :b}}`
  - ignore

- C encounters (4)
  - Pre: `{:pairs {:a :b, :b :a} :prepared {:c :b}}`
  - Post: `{:pairs {:a :c, :b :a, :c :b}}`
  - flushes outbox
  - fully joined into cluster

- A and B encounter (4)
  - Pre: `{:pairs {:a :b, :b :a} :prepared {:c :b}}`
  - Post: `{:pairs {:a :c, :b :a, :c :b}}`
  - ignore
