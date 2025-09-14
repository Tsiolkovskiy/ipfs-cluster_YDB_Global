# IPFS Cluster ZFS Integration - Level 1 Certification Exam

## Exam Information

**Certification Level:** Associate  
**Duration:** 2 hours  
**Passing Score:** 70%  
**Question Types:** Multiple choice, True/False, Short answer  
**Total Questions:** 50

## Instructions

1. Read each question carefully
2. Select the best answer for multiple choice questions
3. Provide clear, concise answers for short answer questions
4. You may use the official documentation during the exam
5. No collaboration with other candidates is allowed

---

## Section 1: Fundamentals (25% - 12 questions)

### Question 1
What is the primary benefit of content addressing in IPFS?

a) Faster download speeds  
b) Guaranteed data integrity through cryptographic hashing  
c) Reduced storage costs  
d) Better compression ratios

**Answer:** b) Guaranteed data integrity through cryptographic hashing

### Question 2
Which consensus algorithm is recommended for large-scale IPFS Cluster deployments?

a) Raft  
b) PBFT  
c) CRDT  
d) Paxos

**Answer:** c) CRDT

### Question 3
What does CID stand for in IPFS?

a) Cluster Identification  
b) Content Identifier  
c) Cryptographic ID  
d) Cluster Index Data

**Answer:** b) Content Identifier

### Question 4
True or False: ZFS compression is applied at the block level.

**Answer:** True

### Question 5
What is the maximum theoretical storage capacity of ZFS?

a) 256 TB  
b) 1 PB  
c) 256 ZB  
d) Unlimited

**Answer:** c) 256 ZB

### Question 6
Which ZFS feature provides automatic corruption detection and repair?

a) Snapshots  
b) Checksumming  
c) Compression  
d) Deduplication

**Answer:** b) Checksumming

### Question 7
What is the role of the IPFS Cluster coordinator?

a) Store IPFS data  
b) Manage pin allocation across cluster peers  
c) Provide web interface  
d) Handle user authentication

**Answer:** b) Manage pin allocation across cluster peers

### Question 8
True or False: IPFS Cluster can operate without IPFS nodes.

**Answer:** False

### Question 9
What is the primary purpose of sharding in the ZFS integration?

a) Improve security  
b) Distribute load across multiple datasets  
c) Reduce memory usage  
d) Increase compression ratios

**Answer:** b) Distribute load across multiple datasets

### Question 10
Which port does the IPFS Cluster REST API use by default?

a) 5001  
b) 8080  
c) 9094  
d) 9096

**Answer:** c) 9094

### Question 11
Short Answer: Explain the difference between IPFS and IPFS Cluster in 2-3 sentences.

**Sample Answer:** IPFS is a distributed file system that stores and shares files using content addressing. IPFS Cluster is a coordination layer that manages which content should be pinned (stored) on which IPFS nodes across a cluster, providing replication and load balancing capabilities.

### Question 12
What does ARC stand for in ZFS?

a) Automatic Replication Cache  
b) Adaptive Replacement Cache  
c) Advanced Read Cache  
d) Asynchronous Recovery Cache

**Answer:** b) Adaptive Replacement Cache

---

## Section 2: Installation and Configuration (30% - 15 questions)

### Question 13
What is the recommended ashift value for modern SSDs in ZFS?

a) 9  
b) 12  
c) 15  
d) 16

**Answer:** b) 12

### Question 14
Which command creates a ZFS pool with compression enabled?

a) `zpool create -O compression=on pool-name /dev/sda`  
b) `zfs create -o compression=lz4 pool-name /dev/sda`  
c) `zpool create -O compression=lz4 pool-name /dev/sda`  
d) `zfs set compression=on pool-name`

**Answer:** c) `zpool create -O compression=lz4 pool-name /dev/sda`

### Question 15
True or False: The cluster secret must be the same across all IPFS Cluster peers.

**Answer:** True

### Question 16
What is the default record size for ZFS datasets?

a) 64K  
b) 128K  
c) 256K  
d) 1M

**Answer:** b) 128K

### Question 17
Which configuration file contains the main IPFS Cluster settings?

a) cluster.json  
b) service.json  
c) config.json  
d) ipfs-cluster.conf

**Answer:** b) service.json

### Question 18
What command initializes an IPFS Cluster configuration?

a) `ipfs-cluster init`  
b) `ipfs-cluster-service init`  
c) `ipfs-cluster-ctl init`  
d) `ipfs cluster init`

**Answer:** b) `ipfs-cluster-service init`

### Question 19
True or False: ZFS datasets can have different compression algorithms within the same pool.

**Answer:** True

### Question 20
Which parameter controls the maximum number of concurrent pin operations?

a) max_pins  
b) concurrent_pins  
c) pin_workers  
d) parallel_pins

**Answer:** b) concurrent_pins

### Question 21
What is the purpose of the `atime` property in ZFS?

a) Set automatic backup time  
b) Control access time updates  
c) Configure archive time  
d) Set allocation time

**Answer:** b) Control access time updates

### Question 22
Short Answer: List the three storage tiers typically configured in the ZFS integration and their intended use cases.

**Sample Answer:**
- Hot tier (NVMe): Metadata and frequently accessed data
- Warm tier (SSD): Active data with moderate access patterns  
- Cold tier (HDD): Archive data and infrequently accessed content

### Question 23
Which IPFS configuration parameter should be adjusted for cluster use?

a) Addresses.API  
b) Datastore.StorageMax  
c) Swarm.ConnMgr.HighWater  
d) All of the above

**Answer:** d) All of the above

### Question 24
What does the `replication_factor_min` setting control?

a) Minimum number of cluster peers  
b) Minimum number of copies for each pin  
c) Minimum storage space per peer  
d) Minimum bandwidth per peer

**Answer:** b) Minimum number of copies for each pin

### Question 25
True or False: ZFS deduplication operates at the file level.

**Answer:** False (it operates at the block level)

### Question 26
Which command checks the status of ZFS pools?

a) `zfs status`  
b) `zpool status`  
c) `zfs list`  
d) `zpool list`

**Answer:** b) `zpool status`

### Question 27
What is the recommended approach for handling the cluster secret?

a) Use the same secret for all deployments  
b) Generate a unique secret for each cluster  
c) Use a default secret provided by IPFS Cluster  
d) Secrets are not necessary for security

**Answer:** b) Generate a unique secret for each cluster

---

## Section 3: Operations and Monitoring (25% - 13 questions)

### Question 28
Which command shows the current pin status across the cluster?

a) `ipfs-cluster-ctl pins`  
b) `ipfs-cluster-ctl status`  
c) `ipfs-cluster-ctl list`  
d) `ipfs-cluster-ctl show`

**Answer:** b) `ipfs-cluster-ctl status`

### Question 29
What does a ZFS scrub operation do?

a) Deletes old snapshots  
b) Checks data integrity and repairs corruption  
c) Compresses existing data  
d) Defragments the pool

**Answer:** b) Checks data integrity and repairs corruption

### Question 30
True or False: IPFS Cluster automatically rebalances pins when new peers join.

**Answer:** True

### Question 31
Which metric indicates poor ZFS cache performance?

a) High compression ratio  
b) Low ARC hit ratio  
c) High deduplication ratio  
d) Low fragmentation level

**Answer:** b) Low ARC hit ratio

### Question 32
What command creates a ZFS snapshot?

a) `zfs create snapshot dataset@name`  
b) `zfs snapshot dataset@name`  
c) `zpool snapshot dataset@name`  
d) `zfs backup dataset@name`

**Answer:** b) `zfs snapshot dataset@name`

### Question 33
How do you check the health of IPFS Cluster peers?

a) `ipfs-cluster-ctl health`  
b) `ipfs-cluster-ctl peers ls`  
c) `ipfs-cluster-ctl status`  
d) `ipfs-cluster-ctl ping`

**Answer:** b) `ipfs-cluster-ctl peers ls`

### Question 34
True or False: ZFS compression ratios above 2.0x are common for typical IPFS content.

**Answer:** True

### Question 35
What should you do if a ZFS pool shows DEGRADED status?

a) Ignore it, it will self-heal  
b) Restart the system  
c) Replace the failed disk  
d) Delete and recreate the pool

**Answer:** c) Replace the failed disk

### Question 36
Which log file contains IPFS Cluster service messages?

a) /var/log/ipfs-cluster.log  
b) journalctl -u ipfs-cluster  
c) /var/log/cluster.log  
d) /var/log/messages

**Answer:** b) journalctl -u ipfs-cluster

### Question 37
Short Answer: What are three key metrics you should monitor in a production IPFS Cluster ZFS deployment?

**Sample Answer:**
- ZFS pool health and capacity usage
- IPFS Cluster peer connectivity and pin success rates  
- System resources (CPU, memory, disk I/O)

### Question 38
What does high fragmentation in a ZFS pool indicate?

a) Good performance  
b) Need for more storage  
c) Degraded performance, may need defragmentation  
d) Successful compression

**Answer:** c) Degraded performance, may need defragmentation

### Question 39
True or False: IPFS Cluster can automatically recover from split-brain scenarios.

**Answer:** True (with CRDT consensus)

### Question 40
Which command shows ZFS I/O statistics?

a) `zfs iostat`  
b) `zpool iostat`  
c) `zfs stats`  
d) `zpool stats`

**Answer:** b) `zpool iostat`

---

## Section 4: Troubleshooting (20% - 10 questions)

### Question 41
An IPFS Cluster peer shows as "down" in the peer list. What should you check first?

a) Disk space  
b) Network connectivity  
c) ZFS pool status  
d) Memory usage

**Answer:** b) Network connectivity

### Question 42
What could cause high memory usage in a ZFS system?

a) Large ARC cache  
b) Too many snapshots  
c) High compression ratios  
d) Deduplication tables

**Answer:** a) Large ARC cache

### Question 43
True or False: Pin operations failing with timeout errors usually indicate network issues.

**Answer:** False (usually indicates storage or performance issues)

### Question 44
If ZFS reports checksum errors, what should you do?

a) Ignore them  
b) Run a scrub operation  
c) Restart the system  
d) Disable checksumming

**Answer:** b) Run a scrub operation

### Question 45
What is the most likely cause of slow pin operations?

a) Network latency  
b) Insufficient CPU  
c) Storage I/O bottleneck  
d) Memory shortage

**Answer:** c) Storage I/O bottleneck

### Question 46
Short Answer: Describe the steps to diagnose why an IPFS Cluster service won't start.

**Sample Answer:**
1. Check service logs with `journalctl -u ipfs-cluster`
2. Verify IPFS service is running first
3. Check configuration file syntax
4. Verify network ports are available
5. Check file permissions and ownership

### Question 47
What does the error "pool is full" indicate in ZFS?

a) Memory is exhausted  
b) Storage capacity is at 100%  
c) Too many files in the pool  
d) Network buffer is full

**Answer:** b) Storage capacity is at 100%

### Question 48
True or False: Restarting IPFS Cluster will cause data loss.

**Answer:** False

### Question 49
If a pin shows "ERROR" status, what information helps with troubleshooting?

a) Pin creation time  
b) File size  
c) Error message in logs  
d) Peer ID

**Answer:** c) Error message in logs

### Question 50
What command helps identify which peer is having issues in a cluster?

a) `ipfs-cluster-ctl status --verbose`  
b) `ipfs-cluster-ctl peers ls`  
c) `ipfs-cluster-ctl health check`  
d) `ipfs-cluster-ctl debug`

**Answer:** b) `ipfs-cluster-ctl peers ls`

---

## Practical Assessment

In addition to the written exam, candidates must complete a practical assessment demonstrating:

### Task 1: Basic Setup (20 points)
- Create ZFS pools with appropriate configuration
- Install and configure IPFS Cluster with ZFS integration
- Verify services are running correctly

### Task 2: Operations (15 points)
- Pin content through the cluster
- Check pin status and peer health
- Create and manage ZFS snapshots

### Task 3: Troubleshooting (15 points)
- Diagnose and resolve a simulated service failure
- Interpret log messages and system metrics
- Demonstrate proper troubleshooting methodology

## Scoring

**Written Exam (70%):**
- Section 1: 25 points
- Section 2: 30 points  
- Section 3: 25 points
- Section 4: 20 points

**Practical Assessment (30%):**
- Task 1: 20 points
- Task 2: 15 points
- Task 3: 15 points

**Total: 150 points**  
**Passing Score: 105 points (70%)**

## Study Resources

- [Module 1: Fundamentals](../module-1/)
- [Module 2: Installation and Configuration](../module-2/)
- [Module 3: Operations and Monitoring](../module-3/)
- [Lab 1: Basic Setup](../labs/lab-1-basic-setup.md)
- [Official IPFS Cluster Documentation](https://cluster.ipfs.io/)
- [ZFS Administration Guide](https://openzfs.github.io/openzfs-docs/)

## Retake Policy

- Candidates who score 60-69% may retake the exam after 30 days
- Candidates who score below 60% must complete additional training before retaking
- Maximum of 3 attempts per 12-month period

## Certification Validity

- Level 1 certification is valid for 2 years
- Renewal requires completing continuing education or passing a renewal exam
- Certification may be upgraded to Level 2 without expiration of Level 1