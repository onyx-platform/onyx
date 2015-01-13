##### Example 4: 7 node cluster, Round Robin job scheduler, Greedy task schedulers, 2 job cluster shift

- Nodes A, B, C, D, E, F, G fully in the cluster
- Client sends `submit-job` (j1, tasks t1, t2, t3) with a Greedy task scheduler (1)
- t1, t2, t3 request no maximum number of peers

- Tasks get allocated to shorten the example
- Local state before (1): `{:jobs [:j1] :tasks {:j1 [:t1 :t2 :t3]} :allocations {:j1 {:t1 [:A :B :C :D :E :F :G] :t2 [] :t3 []}}}`

- A, B, C, D, E, F, G play the log

- Client sends `submit-job` (j2, tasks t4, t5, t6) with a Greedy task scheduler (2)

- A, B, C, D, E, F, G encounter (2)
  - Pre: `{:jobs [:j1] :tasks {:j1 [:t1 :t2 :t3]} :allocations {:j1 {:t1 [:A :B :C :D :E :F :G] :t2 [] :t3 []}}}`
  - Post: `{:jobs [:j1] :tasks {:j1 [:t1 :t2 :t3]} :allocations {:j1 {:t1 [:A :B :C :D :E :F :G] :t2 [] :t3 []}}}`
  - adds job information to local replica
  - let J = 2 (the number of jobs)
  - let P = 7 (the number of peers)
  - every job gets at least 7/2 = 3 peers
  - let R = remainder of 7/2 = 1/J = 1/2
  - let N = numerator of 1/2 = 1
  - the first N (1) jobs will get (P / J) + 1  = 4 peers
  - let K = the number of peers executing this job = 7
  - this job will now only receive 4 peers
  - since this is a Greedy task scheduler, we drop the last 7 - 4 = 3 peers and reassign them
  - A, B, C, D ignore the rest of this message
  - E, F, G send `volunteer-for-task` (3) (4) (5)

-A, B, C, D, E, F, G encounter (3) (4) and (5)
  - Pre: `{:jobs [:j1] :tasks {:j1 [:t1 :t2 :t3]} :allocations {:j1 {:t1 [:A :B :C :D :E :F :G] :t2 [] :t3 []}}}`
  - Post: `{:jobs [:j1] :tasks {:j1 [:t1 :t2 :t3]} :allocations {:j1 {:t1 [:A :B :C :D] :t2 [] :t3 []} :j2 {:t4 [:E :F :G] :t5 [] :t6 []}}}`
  - (condenses messages 3, 4, and 5 for concision)
  - E, F, and G execute t4
  - no reactions

- ... and so on ...
