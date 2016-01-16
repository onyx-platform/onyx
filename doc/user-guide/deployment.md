---
layout: user_guide_page
---

## Deployment

Onyx has no built-in mechanism for deployment. Rather, we let you deploy at your comfort. We'll describe some approaches to doing this.

### Deployment Style

Unlike Hadoop and Storm, Onyx does not have a built-in deployment feature. To deploy your application, you need to uberjar your program and place it on every node in your cluster. Start up the uberjar, passing it your shared Onyx ID and ZooKeeper address. Once it connects, you've successfully deployed!

We've chosen not to build in a deployment strategy because there are so many flexible approaches to handling deployment. S3, Docker, Mesos, Swarm, and Kubernetes are a few good choices. We'll describe some of these strategies below.

#### Shared File System

Perhaps the most primitive deployment that you can use is a shared file system. Write a small script to SCP your uberjar to each of the nodes in your cluster and start the jar. You might use S3 for this, and a utility that allows you to manipulate a few SSH sessions in parallel. This is great for getting started on 3 nodes or so. You'll want to use something a bit more elaborate as you go to production and get bigger, though.

#### Docker

We recommend packaging your uberjar into a Docker container. This is useful for locking in a specific Java version. Make sure you expose the ports that peers will be communicating on (this is configurable, see the peer configuration chapter). Once you have that down, you can upload your Docker image to DockerHub and use a simple script to pull down your Docker image and begin its execution. This is another step in the right direction, but read on to remove the scripting part of this task for real production systems.

#### Mesos and Marathon

Mesos and Marathon are a pair of applications that work together to manage your entire cluster of machines. I recommend deploying your Docker image onto Marathon, which will allow you to scale at runtime and avoid any scripting. Marathon ensures that if your Docker container goes down, it will be restarted. This is one of our favorite solutions for production scale, cluster-wide management at the moment.

#### Kubernetes

We are less familiar with Kubernetes as it's a bit younger than Mesos and Marathon, but Kubernetes will deliver roughly the same functionality that Mesos and Marathon will.

#### Production Check List

A production check list is included in the [Environment documentation](doc/user-guide/environment.md). 
