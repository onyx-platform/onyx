##### Example 3: 2 node cluster, 1 peer successfully joins, 1 aborts

- Nodes A and B fully in the cluster                                                                                                                        
- Local state before (1): `{:pairs {:a :b, :b :a}}`

- C and D want to join
- C sends `prepare-join-cluster` (1)
- D sends `prepare-join-cluster` (2)

- A, B, C, and D play the log

- B encounters (1)
  - Pre: `{:pairs {:a :b, :b :a}}`
  - Post: `{:pairs {:a :b, :b :a} :prepared {:b :c}}`
  - determines B will attach to C. Records this in the local state.
  - adds watch to C
  - sends `notify-join-cluster` (3)

- A, C, and D encounter (1)
  - Pre: `{:pairs {:a :b, :b :a}}`
  - Post: `{:pairs {:a :b, :b :a} :prepared {:b :c}}`
  - determines B will attach to C. Records this in the local state.
  - no reactions

- A, B, C, D encounter (2)
  - Pre: `{:pairs {:a :b, :b :a} :prepared {:b :c}}`
  - Post: `{:pairs {:a :b, :b :a} :prepared {:b :c}}`
  - determines D cannot attach to any nodes. Aborts.
  - D sends `abort-join-cluster` to the log, optionally tries to join again later
  - no reactions

- C encounters (3)
  - Pre: `{:pairs {:a :b, :b :a} :prepared {:b :c}}`
  - Post: `{:pairs {:a :b, :b :a} :accepted {:b :c}}`
  - adds a watch to A
  - sends `accept-join-cluster` (4)

- A and B encounter (3)
  - Pre: `{:pairs {:a :b, :b :a} :prepared {:b :c}}`
  - Post: `{:pairs {:a :b, :b :a} :accepted {:b :c}}`
  - ignore

- B encounters (4)
  - Pre: `{:pairs {:a :b, :b :a} :prepared {:b :c}}`
  - Post: `{:pairs {:a :c, :b :c, :c :a}}`
  - B drops its watch form A

- A and C encounter (4)
  - Pre: `{:pairs {:a :b, :b :a} :prepared {:b :c}}`
  - Post: `{:pairs {:a :c, :b :c, :c :a}}`
  - C flushes its outbox
  - ignore
