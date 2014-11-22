##### Example 1: 3 node cluster, 1 peer successfully joins

- Nodes A, B, and C fully in the cluster                                                                                                                    
- Local state before (1): `{:pairs {:a :b, :b :c, :c :a}}`

- D wants to join
- D sends `prepare-join-cluster` (1)

- A, B, C, and D play the log

- A encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d}}`
  - determines A will attach to D. Records this in the local state.
  - adds watch to D
  - sends `notify-join-cluster` (2)

- B, C, D encounter (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d}}`
  - determines A will attach to D. Records this in the local state.
  - no reactions

- D encounters (2)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :accepted {:a :d}}`
  - adds a watch to B
  - sends `accept-join-cluster` (3)

- A, B, C encounter (2)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :accepted {:a :d}}`
  - ignore

- A encounters (3)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :accepted {:a :d}}`
  - Post: `{:pairs {:a :d, :d :b, :b :c, :c :a}}`
  - drops its watch from B

- B, C, D encounter (3)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :accepted {:a :d}}`
  - Post: `{:pairs {:a :d, :d :b, :b :c, :c :a}}`
  - D flushes its outbox
  - ignore