## Hardware

In this chapter, we'll outline the hardware that should be used in an Onyx production environment for maximum performance.

<!-- START doctoc generated TOC please keep comment here to allow auto update -->
<!-- DON'T EDIT THIS SECTION, INSTEAD RE-RUN doctoc TO UPDATE -->

- [Hardware](#hardware)
  - [Solid State Drives](#solid-state-drives)
  - [10 Gigabit Ethernet Connection](#10-gigabit-ethernet-connection)

<!-- END doctoc generated TOC please keep comment here to allow auto update -->

### Solid State Drives

Each of the nodes running a HornetQ server should be backed with an SSD. If possible, place the HornetQ journal on a dedicated drive for maximum performance. You don't need to break the bank for these drives, they're not where you want to place your money. If running on AWS, ensure that you have enough IOPS provisioned to keep up with the load. You can investigate this with the `iostat` command.

### 10 Gigabit Ethernet Connection

This is the most important piece to get right in your cluster. Onyx performs best in a single data center with a "flat" network design. Under a 10 gigabit ethernet connection, there is no privileged access to a single hard drive - local or remote. This is due to disks being comparatively slower to the network connection. Having a fast network connection removes the penalty that's traditionally incurred from fetching data from a remote drive. Onyx is able to achieve high throughput using a queue based architecture when this bit is in place. For a thorough treatment of this topic, see [Disk-Locality in Datacenter Computing Considered Irrelevant](http://static.usenix.org/event/hotos11/tech/final_files/Ananthanarayanan.pdf).
