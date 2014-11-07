##### Example 1: 3 node cluster, 1 peer successfully joins

- Nodes A, B, and C fully in the cluster                                                                                                                    
- Local state before (1): `{:pairs {:a :b, :b :c, :c :a}}`

- D wants to join
- D sends `prepare-join-cluster` (1)

- A, B, C, and D play the log

- D encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a}}`
  - determines D will attach to A. Records this in the local state.
  - adds watch to A
  - sends `notify-watchers` (2)

- A, B, C encounter (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a}}`
  - determines D will attach to A. Records this in the local state.
  - no reactions

- C encounters (2)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :accepted {:d :a}}`
  - adds a watch to D
  - removes its watch from A
  - sends `accept-join-cluster` (3)

- A, B, D encounter (2)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :accepted {:d :a}}`
  - ignore

- D encounters (3)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :accepted {:d :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :d :d :a}}`
  - flushes outbox
  - fully joined into cluster

- A, B, C encounter (3)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :accepted {:d :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :d :d :a}}`
  - ignore