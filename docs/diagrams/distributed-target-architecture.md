# Distributed Queue Target Architecture

## System topology

```mermaid
flowchart TB
    Client[Producer / Consumer] --> Gateway[Queue Gateway]
    Gateway --> Metadata[(PostgreSQL metadata)]
    Gateway --> LeaderA

    subgraph Control[Control plane]
        Metadata --> Placement[Replica placement controller]
        Placement --> Nodes[Node reconciliation]
    end

    subgraph Group[Partition replica group — RF 3]
        LeaderA[Node A\nPartition leader]
        FollowerB[Node B\nVoting follower]
        FollowerC[Node C\nVoting follower]
        LeaderA <-->|AppendEntries / vote| FollowerB
        LeaderA <-->|AppendEntries / vote| FollowerC
        FollowerB <-.->|Election messages| FollowerC
    end

    Metadata -. desired membership / discovery .-> Group
```

PostgreSQL describes desired topology and reports observations. The replica
group owns election, log ordering, and majority commit.

## One node hosting many tenant partitions

```mermaid
flowchart LR
    subgraph NodeA[Queue Node A]
        A1[Tenant 1 / Orders / P0\nLeader]
        A2[Tenant 1 / Payments / P1\nFollower]
        A3[Tenant 2 / Events / P2\nLeader]
        A4[Tenant 3 / Jobs / P0\nFollower]
    end

    A1 --> D1[(Independent WAL + snapshot)]
    A2 --> D2[(Independent WAL + snapshot)]
    A3 --> D3[(Independent WAL + snapshot)]
    A4 --> D4[(Independent WAL + snapshot)]
```

Each partition replica has an independent runtime, lifecycle gate, durable log,
snapshot, role, term, and progress. Node failure is decomposed into independent
partition elections or follower replacements.

## Replica state progression

```mermaid
flowchart LR
    Receive[Receive entry] --> Append[Append + force]
    Append --> LastLog[lastLogIndex advances]
    LastLog --> Commit{leaderCommit covers entry?}
    Commit -- No --> Wait[Remain durable but unapplied]
    Commit -- Yes --> Persist[Persist commitIndex]
    Persist --> Apply[Apply in index order]
    Apply --> LastApplied[lastApplied advances]
```

Invariant:

```text
lastApplied <= commitIndex <= lastLogIndex
```

## Node failure recovery

```mermaid
flowchart TD
    Fail[Node A unavailable] --> Enumerate[Evaluate each hosted replica]
    Enumerate --> Role{Role on this partition?}
    Role -- Follower --> Majority{Leader still has majority?}
    Majority -- Yes --> Continue[Continue committed operations]
    Continue --> Replace[Bootstrap replacement learner]
    Majority -- No --> Unavailable[Reject mutations]
    Role -- Leader --> Quorum{Remaining voters form majority?}
    Quorum -- No --> Unavailable
    Quorum -- Yes --> Election[Node-coordinated election]
    Election --> NewLeader[Highest eligible log wins quorum]
    NewLeader --> Route[Report leader and update routing]
    Route --> Replace
```
