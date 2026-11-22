# Scheduler namespaces

HTCondor's `ClusterId.ProcId` identifies a job in a schedd's queue, not
across all schedds. JobLens distinguishes the configured or supplied pool
routing label and the complete schedd `Name`. The full name is opaque: `@`
is normal and punctuation such as `#` may occur. A routing label is a
conservative attribution boundary; collector addresses or lists with
different spellings can refer to the same logical pool. JobLens does not
perform DNS or scheduler queries to infer aliases.
The legacy manual RPC uses its local collector/Slurm cluster configuration
when the request supplies no corresponding label. That fallback does not prove
where a remote job originated; callers registering remote jobs should supply
their explicit routing or cluster label.
The Trigger's `/joblens/job` forwards optional top-level `clusterName` and
`clusterTag` unchanged. The Core injects them into the typed `sub_attr`
facts (facts win) and rejects a contradiction with `namespace_conflict`;
the Trigger does not reconcile conflicting values.

Slurm identities use `ClusterName`, job ID, and any supplied step. Historical
incarnations, requeue, job-ID reuse and federation identity are separate
questions and are not resolved by this namespace policy.

Matching compares separately stored structured fields. Which typed
`sub_attr` fields form a scheduler's namespace is adapter-owned knowledge
(`ISchedulerAdapter`'s namespace interface); the common `clusterName`/
`clusterTag` fields are a rendered projection of those facts — the
`job_info.*` contract the query side filters on — never an independent
second source. Request-level `clusterName`/`clusterTag` aliases are
injected into the typed facts at the RPC boundary (facts win; a
contradiction is an explicit `namespace_conflict` rejection). A
disagreement between known components is a conflict even when another
component is unknown. Missing legacy facts are unknown rather than proof
of equality: compatible candidates may be used when unique, but multiple
candidates are ambiguous. Schema 4's existing combined namespace remains
unchanged. Its delimiter encoding can alias different structured tuples;
an identified alias is rejected instead of merging records. No assumption
is made that synthetic alias labels are valid HTCondor `COLLECTOR_HOST`
settings.

**Discovery limitation:** the existing execute-side Condor discovery path
derives schedd information from `GlobalJobId`. HTCondor documents that value
as opaque and explicitly advises against parsing it; `#` in names, escaping,
or future format changes can make that legacy parsing wrong. This change
preserves discovery for compatibility and does not establish a guaranteed
independent execute-ad source of the schedd name. A separately validated
provenance mechanism is future work.

Official references:

- [HTCondor ClusterId and GlobalJobId](https://htcondor.readthedocs.io/en/24.x/classad-attributes/job-classad-attributes.html#GlobalJobId)
- [HTCondor condor_q pool and schedd selection](https://htcondor.readthedocs.io/en/24.x/man-pages/condor_q.html)
- [HTCondor SCHEDD_NAME and MASTER_NAME](https://htcondor.readthedocs.io/en/24.x/admin-manual/configuration-macros.html#SCHEDD_NAME)
- [HTCondor COLLECTOR_HOST](https://htcondor.readthedocs.io/en/24.x/admin-manual/configuration-macros.html#COLLECTOR_HOST)
- [Slurm ClusterName](https://slurm.schedmd.com/slurm.conf.html#OPT_ClusterName), [sacct job IDs and duplicate records](https://slurm.schedmd.com/sacct.html#OPT_duplicates), and [federation job IDs](https://slurm.schedmd.com/federation.html#jobids)
