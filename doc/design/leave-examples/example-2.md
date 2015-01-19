##### Example 2: 4 node cluster, 2 peers instantaneously crash

- Nodes A, B, C, and D fully in the cluster
- Local state before (1): `{:pairs {:a :b, :b :c, :c :d, :d :a}}`

- A, B, C, and D play the log
- B and C at the exact same time, A sends `leave-cluster` (1)

- A encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :c, :c :d, :d :a}}`
  - determines A will transitively close the gap and watch C
  - notices C is also dead
  - sends `leave-cluster` (2)

- D encounters (1)
  - Pre: `{:pairs {:a :b, :b :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :c, :c :d, :d :a}}`
  - determines A will transitively close the gap and watch C
  - ignores

- A encounters (2)
  - Pre: `{:pairs {:a :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :d, :d :a}}`
  - determines A will transitively close the gap and watch D
  - adds watch to D

- D encounters (2)
  - Pre: `{:pairs {:a :c, :c :d, :d :a}}`
  - Post: `{:pairs {:a :d, :d :a}}`
  - determines A will transitively close the gap and watch D
  - ignore