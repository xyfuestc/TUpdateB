# T-UpdateB: A data update scheme for data storage

Created by: Yifei Xiao
Created time: December 4, 2022 10:17 AM

We present **T-UpdateB**, a scheme that is based on [T-Update](https://ieeexplore.ieee.org/abstract/document/7524347/), employing gradual merging and XOR. Extensive local test-bed experiments show that T-UpdateB can improve the throughput by 17%-214% while maintaining extremely low cross-rack traffic.

# Preparations

## 1.install go

You should ensure the goLang running environment(go ≥ `1.11`, make sure you have `go mod`). Assume in Ubuntu 18.04,

```bash
#install go
sudo add-apt-repository ppa:longsleep/golang-backports

sudo apt-get update

sudo apt-get install golang-go

#check go version：
go version  #eg: go version go1.11.5 linux/amd64

```

## 2.deployment

At least you should have 3 nodes : 1)a node for ms(central controller), 2)a node for data node, and 3) a node for parity node.

For example, we have 13 nodes: 1) a node for ms, 2) 8 for data nodes and 4 for parity nodes. Meanwhile, these 12 storage nodes are in 3 racks (2 data racks and 1 parity rack)

### 1) put source code to nodes

put the source code to the 13 nodes, for example, you can put it in home dir (cd ~).

### 2) generate blocks

use `dd` command  to generate a big file, which simulate the local data blocks or parity blocks.

```bash
dd if=/dev/zero of=test bs=1M count=10240  #10GB
```

### 3) configuration

in config/config.go

```go
// RS(N,K) and W
const K int = 8
const M int = 4
const W int = 8
const N  = K + M

...

// we have 9 policies
var Policies = []string{"Base", "CAU", "CAU_DB", "PDN_P", "TUpdate", "TUpdateB", "TUpdateBA" , "MultiD", "MultiDB"}

...

// make sure the 13 nodes have correct IPs
const BaseIP string = "192.168.1."
var MSIP = BaseIP + "108"
var NodeIPs =[]string{
	BaseIP+"110", BaseIP+"111", BaseIP+"112", BaseIP+"113",     //rack0
	BaseIP+"120", BaseIP+"121", BaseIP+"122", BaseIP+"123",     //rack1
	BaseIP+"140", BaseIP+"141", BaseIP+"142", BaseIP+"143",     //rack2
}
// use dd command to generate a big file (e.g., 10GB) named test, 
// simulate many blocks in local
const DataFilePath string = "../../test"  
```

## Running

## 1. running data nodes

in data node, you can run it like this:

```bash
cd datanode 
go run datanode.go
```

## 2.running parity nodes

```bash
cd paritynode 
go run paritynode.go
```

## 3.running ms node

```bash
cd ms 
go run ms.go
```

in ms, you can change running parameters in terms of :

```go
var NumOfMB = flag.Float64("b", 4, "block size：int，default：4MB")
var policyID = flag.Int("p", 0, "policy ID：0-Base, 1-CAU, 2-CAU_DB...(see config.go)  default：0")
var closeNodes = flag.Int("c", 0, "whether close data nodes and parity nodes or not when the running is over？1-close；0-no，default：0")
var traceName = flag.String("f", "rsrch_2", "the trace name，default：rsrch_2")
```

for example, you can use parameters to run like this:

```go
go run ms.go -b 16 -p 5 -c 1 -f hm_0
```

the above cmd means using trace file `hm_0` to run, setting block size as 16MB and the policy as T-UpdateB, and close all nodes when the running is over.

PS: If you have any problems, you can send email to me(xyf1989@uestc.edu.cn)