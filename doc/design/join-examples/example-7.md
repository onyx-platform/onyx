##### Example 7: 3 node cluster, 1 peer dies while joining                                                                                                  

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
  - D dies. No peer is watching, so this isn't reported

- A, B, C encounter (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a}}`
  - ignore

- A, B encounter (2):
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :accepted {:d :a}`
  - ignore

- C encounters (2):
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :prepared {:d :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a} :accepted {:d :a}`
  - tries to add a watch to D, notices its dead
  - sends `abort-join-cluster` (3)

A, B, C encounter (3):
  - Pre: `{:pairs {:a :b, :b :c, :c :a} :accepted {:d :a}`
  - Post: `{:pairs {:a :b, :b :c, :c :a}`
  - rollback accepted peer