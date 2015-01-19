##### Example 3: 7 node cluster, 2 jobs, Round Robin job scheduler, Greedy task schedulers

- Nodes A, B, C, D, E, F, G fully in the cluster
- Local state before (1): `{}`

- Client sends `submit-job` (j1, tasks t1, t2, t3) with a Greedy task scheduler (1)
- Client sends `submit-job` (j2, tasks t4, t5, t6) with a Greedy task scheduler (2)
- t1..t6 request no maximum number of peers

- A, B, C, D, E, F, G play the log

- A, B, C, D, E, F, G encounter (1)
  - Pre: `{}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 []}}}`
  - adds job information to local replica
  - A sends `volunteer-for-task` (3)
  - B sends `volunteer-for-task` (4)
  - C sends `volunteer-for-task` (5)
  - D sends `volunteer-for-task` (6)
  - E sends `volunteer-for-task` (7)
  - F sends `volunteer-for-task` (8)
  - G sends `volunteer-for-task` (9)

- A, B, C, D, E, F, G encounter (2)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [] :t3 []}}}`
  - Post: `{:jobs [:j1 :j2]
            :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 []} :j2 {:t4 [] :t5 [] :t6 []}}`
  - adds job information to local replica
  - A sends `volunteer-for-task` (10)
  - B sends `volunteer-for-task` (11)
  - C sends `volunteer-for-task` (12)
  - D sends `volunteer-for-task` (13)
  - E sends `volunteer-for-task` (14)
  - F sends `volunteer-for-task` (15)
  - G sends `volunteer-for-task` (16)

- A encounters (3)
  - Pre: `{:jobs [:j1 :j2]
           :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
           :allocations {:j1 {:t1 [] :t2 [] :t3 []} :j2 {:t4 [] :t5 [] :t6 []}}`
  - Post: `{:jobs [:j1 :j2]
            :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
            :allocations {:j1 {:t1 [:A] :t2 [] :t3 []} :j2 {:t4 [] :t5 [] :t6 []}}`
  - determines A will execute t1
  - executes t1, no reactions

- B, C, D, E, F, G encounter (3)
  - Pre: `{:jobs [:j1 :j2]
           :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
           :allocations {:j1 {:t1 [] :t2 [] :t3 []} :j2 {:t4 [] :t5 [] :t6 []}}`
  - Post: `{:jobs [:j1 :j2]
            :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
            :allocations {:j1 {:t1 [:A] :t2 [] :t3 []} :j2 {:t4 [] :t5 [] :t6 []}}`
  - determines A will execute t1
  - ignores

- B encounters (4)
  - Pre: `{:jobs [:j1 :j2]
           :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
           :allocations {:j1 {:t1 [:A] :t2 [] :t3 []} :j2 {:t4 [] :t5 [] :t6 []}}`
  - Post: `{:jobs [:j1 :j2]
            :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
            :allocations {:j1 {:t1 [:A] :t2 [] :t3 []} :j2 {:t4 [:B] :t5 [] :t6 []}}`
  - determines B will execute t4
  - executes t4, no reactions

- A, C, D, E, F, G encounter (4)
  - Pre: `{:jobs [:j1 :j2]
           :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
           :allocations {:j1 {:t1 [:A] :t2 [] :t3 []} :j2 {:t4 [] :t5 [] :t6 []}}`
  - Post: `{:jobs [:j1 :j2]
            :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
            :allocations {:j1 {:t1 [:A] :t2 [] :t3 []} :j2 {:t4 [:B] :t5 [] :t6 []}}`
  - determines B will execute t4
  - ignores

- C encounters (5)
  - Pre: `{:jobs [:j1 :j2]
           :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
           :allocations {:j1 {:t1 [:A] :t2 [] :t3 []} :j2 {:t4 [] :t5 [] :t6 []}}`
  - Post: `{:jobs [:j1 :j2]
            :tasks {:j1 [:t1 :t2 :t3] :j2 [:t4 :t5 :t6]}
            :allocations {:j1 {:t1 [:A :C] :t2 [] :t3 []} :j2 {:t4 [:B] :t5 [] :t6 []}}`
  - determines C will execute t1
  - executes t1, no reactions

- ... And so on ...