##### Example 4: 1 node cluster, 1 peer successfully joins

- Node A fully in the cluster                                                                                                                               
- Local state before (1): `{:pairs {}}`

- B wants to join
- B sends `prepare-join-cluster` (1)

- And B play the log

- B encounters (1)
  - Pre: `{:pair {}}`
  - Post: `{:pairs {} :prepared {:b :a}}`
  - determines B will attach to A
  - adds watch to A
  - sends `notify-join-cluster` (2)

- A encounters (1)
  - Pre: `{:pair {}}`
  - Post: `{:pairs {} :prepared {:b :a}}`
  - determines B will attach to A
  - no reactive commands

- A encounters (2)
  - Pre: `{:pairs {} :prepared {:b :a}}`
  - Post: `{:pairs {} :accepted {:b :a}}`
  - adds a watch to B
  - sends `accept-join-cluster` (3)

- B encounters (2)
  - Pre: `{:pairs {} :prepared {:b :a}}`
  - Post: `{:pairs {} :accepted {:b :a}}`
  - ignores

- B encounters (3)
  - Pre: `{:pairs {} :prepared {:b :a}}`
  - Post: `{:pairs {:a :b, :b :a}}`
  - flushes outbox
  - fully joined into cluster

- A encounters (3)
  - Pre: `{:pairs {} :prepared {:b :a}}`
  - Post: `{:pairs {:a :b, :b :a}}`
  - ignores
