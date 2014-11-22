##### Example 2: 3 node cluster, 2 peers successfully join

- Nodes A, B, and C fully in the cluster                                                                                                                    
- Local state before (1): `{:pairs {:a :b, :b :c, :c :a}}`

- D and E want to join
- D sends `prepare-join-cluster` (1)
- E sends `prepare-join-cluster` (2)

- A, B, C, D, and E play the log

- A encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d}}`
  - determines A will attach to D. Records this in the local state.
  - adds watch to D
  - sends `notify-join-cluster` (3)

 - B, C, D, and E encounter (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d}}`
  - determines A will attach to D. Records this in the local state.
  - no reactions

- B encounters (2)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d, :b :e}}`
  - determines B will attach to E. Records this in the local state.
  - adds watch to E
  - sends `notify-join-cluster` (4)

- A, C, D, and E encounter (2)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d, :b :e}}`
  - determines B will attach to E. Records this in the local state.
  - no reactions

- D encounters (3)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d, :b :e}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:b :e} :accepted {:a :d}}`
  - adds a watch to B
  - sends `accept-join-cluster` (5)

- A, B, C, E encounter (3)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:a :d, :b :e}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:b :e} :accepted {:a :d}}`
  - ignore

- E encounters (4)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:b :e} :accepted {:a :d}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :accepted {:a :d, :b :e}}`
  - adds a watch to C
  - sends `accept-join-cluster` (6)

- A, B, C, D encounter (4)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:b :e} :accepted {:a :d}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :accepted {:a :d, :b :e}}`
  - ignore

- A encounters (5)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :accepted {:a :d, :b :e}}`
  - Post: `{:pairs {:a :d, :d :b, :b :c, :c :a} :accepted {:b :e}}`
  - A drops its watch from B

- B, C, D, E encounter (5)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :accepted {:a :d, :b :e}}`
  - Post: `{:pairs {:a :d, :d :b, :b :c, :c :a} :accepted {:b :e}}`
  - D flushes outbox
  - ignore

- B encounters (6)
  - Pre: `{:pairs {:a :d, :d :b, :b :c, :c :a} :accepted {:b :e}}`
  - Post: `{:pairs {:a :d, :d :b, :b :e, :e :c, :c :a}}`
  - B drops its watch from C

- A, C, D, E encounter (6)
  - Pre: `{:pairs {:a :d, :d :b, :b :c, :c :a} :accepted {:b :e}}`
  - Post: `{:pairs {:a :d, :d :b, :b :e, :e :c, :c :a}}`
  - E flushes its outbox
  - ignore