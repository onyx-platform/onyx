##### Example 4: 3 node cluster, 1 peer lags during GC                                                                                                      

- Nodes A, B, and C fully in the cluster
- D sends `peer-gc` (1)
- Local state before (1): `{:pairs {:a :b, :b :c, :c :a}}`

- A, B, C, and D play the log
- B dies, A is exceptionally slow to react and does not yet send a command to the log

- D encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a}}`
  - D notices that B is dead
  - D sends `leave-cluster` (2)
  - *A wakes up and sends `leave-cluster` (3) for B*
  - D sends `prepare-join-cluster` (4)

- C encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a}}`
  - ignores, D initiated GC

- C and D encounter (2):
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :c, :c :a}}`
  - determines A will close the gap and watch C

- A encounters (1):
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :b, :b :c, :c :a}}`
  - ignores, D initiated GC

- A encounters (2):
  - Pre: `{:pairs {:a :b, :b :c, :c :a}}`
  - Post: `{:pairs {:a :c, :c :a}}`
  - determines A will close the gap and watch C
  - adds watch to C

- A, C, and D encounter (3):
  - Pre: `{:pairs {:a :c, :c :a}}`
  - Post: `{:pairs {:a :c, :c :a}}`
  - idempontently ignores message

- (4) continues as normal