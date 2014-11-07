##### Example 2: 3 node cluster, 2 peers successfully join

- Nodes A, B, and C fully in the cluster                                                                                                                    
- Local state before (1): `{:pairs {:a :b, :b :c, :c :a}}`

- D and E want to join
- D sends `prepare-join-cluster` (1)
- E sends `prepare-join-cluster` (2)

- A, B, C, D, and E play the log

- D encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a}}`
  - determines D will attach to A. Records this in the local state.
  - adds watch to A
  - sends `notify-watchers` (3)

- A, B, C, and E encounter (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a}}`
  - determines D will attach to A. Records this in the local state.
  - no reactions

- E encounters (2)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a, :e :b}}`
  - determines E will attach to B. Records this in the local state.
  - adds watch to B
  - sends `notify-watchers` (4)

- A, B, C, and D encounter (2)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a, :e :b}}`
  - determines E will attach to B. Records this in the local state.
  - no reactions
- C encounters (3)                                                                                                                                          
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a, :e :b}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepated {:e :b} :accepted {:d :a}}`
  - adds a watch to D
  - removes its watch from A
  - sends `accept-join-cluster` (5)

- A, B, D, E encounter (3)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a, :e :b}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepated {:e :b} :accepted {:d :a}}`
  - ignore

- A encounters (4)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepated {:e :b} :accepted {:d :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :accepted {:d :a, :e :b}}`
  - adds a watch to E
  - removes its watch from B
  - sends `accept-join-cluster` (6)

- B, C, D, E encounter (4)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepated {:e :b} :accepted {:d :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :accepted {:d :a, :e :b}}`
  - ignore

- D encounters (5)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :accepted {:d :a, :e :b}}`
  - Post: `{:pairs {:a :b, :b :c, :c :d, :d :a} :accepted {:e :b}}`
  - flushes outbox
  - fully joined into cluster

- A, B, C, E encounter (5)
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :accepted {:d :a, :e :b}}`
  - Post: `{:pairs {:a :b, :b :c, :c :d, :d :a} :accepted {:e :b}}`
  - ignore

- E encounters (6)
  - Pre: `{:pairs {:a :b, :b :c, :c :d, :d :a} :accepted {:e :b}}`
  - Post: `{:pairs {:e :b, :a :e, :b :c, :c :d, :d :a}}`
  - flushes outbox
  - fully joined into cluster

- A, B, C, D encounter (6)
  - Pre: `{:pairs {:a :b, :b :c, :c :d, :d :a} :accepted {:e :b}}`
  - Post: `{:pairs {:e :b, :a :e, :b :c, :c :d, :d :a}}`
  - ignore