##### Example 1: 4 node cluster, 1 peer crashes

- Nodes A, B, C, and D fully in the cluster
- Local state before (1): `{:pairs {:a :b, :b :c, :c :d, :d :a}}`

- A, B, C, and D play the log
- B dies, A sends `leave-cluster` (1)

- A encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :c, :c :d, :d :a}}`
  - determines A will transitively close the gap and watch C
  - adds watch to C

- C and D encounter (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :c, :c :d, :d :a}}`
  - determines A will transitively close the gap and watch C
  - ignores
