# InstaInfer-SoCC24

This repo contains a demo implementation of our SoCC 2024 paper, [Pre-Warming is Not Enough: Accelerating Serverless Inference With Opportunistic Pre-Loading](https://intellisys.haow.us/assets/pdf/yifan-socc24.pdf)

---

> Serverless computing has rapidly prospered as a new cloud computing diagram with agile scalability, pay-as-you-go pricing, and ease-to-use features for Machine Learning (ML) inference tasks. Users package their ML code into lightweight serverless functions and execute them using containers. Unfortunately, a notorious problem, called cold-starts, hinders serverless computing from providing low-latency function executions. To mitigate cold-starts, pre-warming, which keeps containers warm predictively, has been widely accepted by academia and industry. However, pre-warming fails to eliminate the unique latency incurred by loading ML artifacts. We observed that for ML inference functions, the loading of libraries and models takes significantly more time than container warming. Consequently, pre-warming alone is not enough to mitigate the ML inference function's cold-starts.
>

> This paper introduces InstaInfer, an opportunistic pre-loading technique to achieve instant inference by eliminating the latency associated with loading ML artifacts, thereby achieving minimal time cost in function execution. InstaInfer fully utilizes the memory of warmed containers to pre-load the function's libraries and model, striking a balance between maximum acceleration and resource wastage. We design InstaInfer to be transparent to providers and compatible with existing pre-warming solutions. Experiments on OpenWhisk with real-world workloads show that InstaInfer reduces up to 93% loading latency and achieves up to 8 × speedup compared to state-of-the-art pre-warming solutions.
>

---

InstaInfer is built atop [Apache OpenWhisk](https://github.com/apache/openwhisk). We describe how to build and deploy InstaInfer from scratch for this demo.

As InstaInfer is compatible with different pre-warming solutions, this repo shows InstaInfer + Pagurus, the pre-warming method of [Help Rather Than Recycle](https://www.usenix.org/conference/atc22/presentation/li-zijun-help).

## Build From Scratch

### Hardware Prerequisite

- Operating systems and versions: Ubuntu 22.04
- Resource requirement
  - CPU: >= 8 cores
  - Memory: >= 15 GB
  - Disk: >= 40 GB
  - Network: no requirement since it's a single-node deployment

### Deployment and Run Demo

This demo hosts all InstaInfer’s components on a single node.

**Instruction**

1. Download the github repo.

```
git clone https://github.com/IntelliSys-Lab/InstaInfer-SoCC24.git
```

2. Set up OpenWhisk Environment.

```
cd ~/InstaInfer-SoCC24/tools/ubuntu-setup
sudo ./all.sh
```

3. Deploy InstaInfer. This could take quite a while due to building Docker images from scratch. The recommended shell is Bash.

```
cd ~/InstaInfer-SoCC24
sudo chmod +x setup.sh
sudo ./setup.sh
```

4. Run InstaInfer’s demo. The demo experiment runs a 5-minute workload from Azure Trace.

```bash
cd ~/InstaInfer-SoCC24/demo
python3 Excute_from_trace_5min.py
```

## Experimental Results and OpenWhisk Logs

After executing `Excute_from_trace_5min.py`, you may use the [wsk-cli](https://github.com/IntelliSys-Lab/RainbowCake-ASPLOS24/blob/master/demo/wsk) to check the results of function executions:

```
wsk -i activation list

```

Detailed experimental results are collected as `output.log` file in  `InstaInfer-SoCC24/demo`. The result includes function end-to-end and startup latency, invocation startup types, timelines, and whether pre-loaded. Note that `~/InstaInfer-SoCC24/demo/output.log` is not present in the initial repo. It will only be generated after running an experiment. OpenWhisk system logs can be found under `/var/tmp/wsklogs`.

## Workloads

We provide the codebase of [20 serverless applications](https://github.com/IntelliSys-Lab/RainbowCake-ASPLOS24/tree/master/applications) used in our evaluation. However, due to hardware and time limitations, we only provide a simple [demo invocation trace](https://github.com/IntelliSys-Lab/RainbowCake-ASPLOS24/tree/master/demo/azurefunctions-dataset2019) for the demo experiment.

## Distributed InstaInfer

The steps of deploying a distributed InstaInfer are basically the same as deploying a distributed OpenWhisk cluster. For deploying a distributed InstaInfer, please refer to the README of [Apache OpenWhisk](https://github.com/apache/openwhisk) and [Ansible](https://github.com/apache/openwhisk/tree/master/ansible).

