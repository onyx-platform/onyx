##### Example 2: 3 node cluster, 1 job, Greedy job scheduler, Round Robin task scheduler

- Nodes A, B, and C fully in the cluster
- Local state before (1): `{}`

- Client sends `submit-job` (j1, tasks t1, t2) with a Round Robin task scheduler (1)
- t1 and t2 request no maximum number of peers

- A, B, and C play the log

- A, B and C encounter (1)
  - Pre: `{}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [] :t2 []}}}`
  - adds job information to local replica
  - A sends `volunteer-for-task` (2)
  - B sends `volunteer-for-task` (3)
  - C sends `volunteer-for-task` (4)

- A encounters (2)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [] :t2 []}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [:A] :t2 []}}}`
  - determines A will execute t1
  - executes t1, no reactions

- B, C encounters (2)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [] :t2 []}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [:A] :t2 []}}}`
  - determines A will execute t1
  - ignores

- B encounters (3)
  - Pre: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [:A] :t2 []}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [:A] :t2 [:B]}}}`
  - determines B will execute t2
  - executes t2, no reactions

- A, C encounter (3)
  - Pre: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [:A] :t2 []}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [:A] :t2 [:B]}}}`
  - determines B will execute t2
  - ignores

- C encounters (4)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [:A] :t2 [:B]}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [:A :C] :t2 [:B]}}}`
  - determines C will execute t1
  - executes t1, no reactions

- A, B encounters (4)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [:A] :t2 [:B]}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [:A :C] :t2 [:B]}}}`
  - determines C will execute t1
  - ignores

- C completes t1, sends `complete-task` (5)

- A, C encounter (5)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [:A :C] :t2 [:B]}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [] :t2 [:B]}}
            :completions {:j1 [:t1]}}`
  - marks task as complete in local replica
  - A sends `volunteer-for-task` (6)
  - C sends `volunteer-for-task` (7)

- A encounters (6)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [] :t2 [:B]}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [] :t2 [:B :A]}}
            :completions {:j1 [:t1]}}`
  - determines A will execute t2
  - executes t2, no reactions

- C encounters (6)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [] :t2 [:B]}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [] :t2 [:B :A]}}
            :completions {:j1 [:t1]}}`
  - determines A will execute t2
  - ignores

- C encounters (7)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [] :t2 [:B :A]}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [] :t2 [:B :A :C]}}
            :completions {:j1 [:t1]}}`
  - determines C will execute t2
  - executes t2, no reactions

- A encounters (7)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [] :t2 [:B :A]}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [] :t2 [:B :A :C]}}
            :completions {:j1 [:t1]}}`
  - determines C will execute t2
  - ignores

- B encounters (5)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [:A :C] :t2 [:B]}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [] :t2 [:B]}}
            :completions {:j1 [:t1]}}`
  - marks task as complete in local replica
  - ignores, currently executing a task

- B encounters (6)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [] :t2 [:B]}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [] :t2 [:B :A]}}
            :completions {:j1 [:t1]}}`
  - determines A will execute t2
  - ignores

- B encounters (7)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [] :t2 [:B :A]}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [] :t2 [:B :A :C]}}
            :completions {:j1 [:t1]}}`
  - determines C will execute t2
  - ignores

- B completes t2, sends `complete-task` (8)

- A, B, C encounter (8)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2]}
           :allocations {:j1 {:t1 [] :t2 [:B :A :C]}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2]}
            :allocations {:j1 {:t1 [] :t2 []}}
            :completions {:j1 [:t1 :t2]}}`
  - marks task as complete in local replica
  - A, B and C notice there are no more jobs to execute tasks for, no reactions