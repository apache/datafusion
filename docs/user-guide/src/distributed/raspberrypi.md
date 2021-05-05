# Running Ballista on Raspberry Pi

The Raspberry Pi single-board computer provides a fun and relatively inexpensive way to get started with distributed
computing.

These instructions have been tested using an Ubuntu Linux desktop as the host, and a 
[Raspberry Pi 4 Model B](https://www.raspberrypi.org/products/raspberry-pi-4-model-b/) with 4 GB RAM as the target.

## Preparing the Raspberry Pi

We recommend installing the 64-bit version of [Ubuntu for Raspberry Pi](https://ubuntu.com/raspberry-pi).

The Rust implementation of Arrow does not work correctly on 32-bit ARM architectures 
([issue](https://github.com/apache/arrow-rs/issues/109)).

## Cross Compiling DataFusion for the Raspberry Pi

Although it is technically possible to build DataFusion directly on a Raspberry Pi, it really isn't very practical. 
It is much faster to use [cross](https://github.com/rust-embedded/cross) to cross-compile from a more powerful 
desktop computer.

Docker must be installed and the Docker daemon must be running before cross-compiling with cross. See the 
[cross](https://github.com/rust-embedded/cross) project for more detailed instructions.

Run the following command to install cross.

```bash
cargo install cross
```

From the root of the DataFusion project, run the following command to cross-compile for ARM 64 architecture.

```bash
cross build --release --target aarch64-unknown-linux-gnu
```

It is even possible to cross-test from your desktop computer:

```bash
cross test --target aarch64-unknown-linux-gnu
```

## Deploying the binaries to Raspberry Pi

You should now be able to copy the executable to the Raspberry Pi using scp on Linux. You will need to change the IP 
address in these commands to be the IP address for your Raspberry Pi. The easiest way to find this is to connect a 
keyboard and monitor to the Pi and run `ifconfig`. 

```bash
scp ./target/aarch64-unknown-linux-gnu/release/ballista-scheduler ubuntu@10.0.0.186:
scp ./target/aarch64-unknown-linux-gnu/release/ballista-executor ubuntu@10.0.0.186:
```

Finally, ssh into the Pi and make the binaries executable:

```bash
ssh ubuntu@10.0.0.186
chmod +x ballista-scheduler ballista-executor
```

It is now possible to run the Ballista scheduler and executor natively on the Pi.