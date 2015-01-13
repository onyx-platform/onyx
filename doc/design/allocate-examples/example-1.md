##### Example 1: 3 node cluster, 1 job, Greedy job scheduler, Greedy task scheduler

- Nodes A, B, and C fully in the cluster
- Local state before (1): `{}`

- Client sends `submit-job` (j1, tasks t1, t2, t3) with a Greedy task scheduler (1)
- t1, t2, and t3 request no maximum number of peers

- A, B, and C play the log

- A encounters (1)
  - Pre: `{}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 []}}}`
  - adds job information to local replica
  - sends `volunteer-for-task` (2)

- B encounters (1)
  - Pre: `{}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 []}}}`
  - adds job information to local replica
  - sends `volunteer-for-task` (3)

- C encounters (1)
  - Pre: `{}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 []}}}`
  - adds job information to local replica
  - sends `volunteer-for-task` (4)

- B, C encounter (2)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [] :t3 []}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [:A] :t2 [] :t3 []}}}`
  - determines A will execute t1
  - ignores

- A encounters (2)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [] :t3 []}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [:A] :t2 [] :t3 []}}}`
  - determines A will execute t1
  - executes t1, no reactions
  
- A, C encounter (3)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [:A] :t2 [] :t3 []}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [:A :B] :t2 [] :t3 []}}}`
  - determines B will execute t1
  - ignores

- B encounters (3)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [:A] :t2 [] :t3 []}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [:A :B] :t2 [] :t3 []}}}`
  - determines B will execute t1
  - executes t1, no reacitons

- A, B encounter (4)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [:A :B] :t2 [] :t3 []}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [:A :B :C] :t2 [] :t3 []}}}`
  - determines C will execute t1
  - ignores

- C encounters (4)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [:A :B] :t2 [] :t3 []}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [:A :B :C] :t2 [] :t3 []}}}`
  - determines C will execute t1
  - executes t1, no reactions

- C completes t1, sends `complete-task` (5)

- A, B, C encounter (5)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [:A :B :C] :t2 [] :t3 []}}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
            :completions {:j1 [:t1]}}`
  - marks task as complete in local replica
  - A sends `volunteer-for-task` (6)
  - B sends `volunteer-for-task` (7)
  - C sends `volunteer-for-task` (8)

- A, B, C encounter (6)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [:A] :t3 []}}
            :completions {:j1 [:t1]}}`
  - determines A will execute t2
  - A executes t2, no reactions
  - B, C ignore

- A, B, C encounter (7)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [:A] :t3 []}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [:A :B] :t3 []}}
            :completions {:j1 [:t1]}}`
  - determines B will execute t2
  - B executes t2, no reactions
  - A, C ignore

- A, B, C encounter (8)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [:A :B] :t3 []}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [:A :B :C] :t3 []}}
            :completions {:j1 [:t1]}}`
  - determines C will execute t2
  - C executes t2, no reactions
  - A, B ignore

- A completes t2, sends `complete-task` (9)
- B and C lag behind a little for the rest of the example, to show demonstrate a slightly more interesting case in t3

- A encounters (9)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [:A :B :C] :t3 []}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
            :completions {:j1 [:t1 :t2]}}`
  - marks task as complete in local replica
  - A sends `volunteer-for-task` (10)

- A encounters (10)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
           :completions {:j1 [:t1 :t2]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 [:A]}}
            :completions {:j1 [:t1 :t2]}}`
  - determines A will execute t3
  - A executes t3, no reactions

- A completes t3, sends `complete-task` (11)

- A encounters (11)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
           :completions {:j1 [:t1 :t2]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
            :completions {:j1 [:t1 :t2 :t3]}}`
  - marks task as complete in local replica
  - A notices there are no more jobs to execute tasks for, no reactions

- B, C encounter (9)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [:A :B :C] :t3 []}}
           :completions {:j1 [:t1]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
            :completions {:j1 [:t1 :t2]}}`
  - marks task as complete in local replica
  - B sends `volunteer-for-task` (12)
  - C sends `volunteer-for-task` (13)

- B, C encounter (10)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
           :completions {:j1 [:t1 :t2]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 [:A]}}
            :completions {:j1 [:t1 :t2]}}`
  - determines A will execute t3
  - ignores

- B, C encounter (11)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
           :completions {:j1 [:t1 :t2]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
            :completions {:j1 [:t1 :t2 :t3]}}`
  - marks task as complete in local replica
  - B and C notice there are no more jobs to execute tasks for, no reactions

- A, B, and C encounter (12) and (13) (same scenario for both messages)
  - Pre: `{:jobs [:j1]
           :tasks {:j1 [:t1 :t2 :t3]}
           :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
           :completions {:j1 [:t1 :t2 :t3]}}`
  - Post: `{:jobs [:j1]
            :tasks {:j1 [:t1 :t2 :t3]}
            :allocations {:j1 {:t1 [] :t2 [] :t3 []}}
            :completions {:j1 [:t1 :t2 :t3]}}`
  - A, B and C notice there are no more jobs to execute tasks for, no reactions