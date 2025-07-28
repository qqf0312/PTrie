# Building Instructions

We use **CMake** as the building system.

## Step 1: Configure the Build

First, open a terminal and navigate to the root directory of the project:

```bash
cd /path/to/your/project
```

Then configure the build plan:

```bash
cmake -S . -B build
```

## Step 2: Build the Project

Run the following command to compile:

```bash
cmake --build build -j
```

This will compile and link the main library and several executables.

## Step 3: Run the Program

The generated executable is located at:

```
./build/mini-p2p
```

Before running the program, create a file named `config.ini` inside the `build/` directory.

### Example `config.ini`:

```ini
[server]
port = 8086           ; Port number the server listens on
host = 127.0.0.1      ; Bind address

[general]
nodes_number = 4      ; Number of participating nodes in the system
fault_tolerance = 2   ; Number of faulty nodes the system can tolerate
encoding_level = 2    ; Level of hierarchical encoding used
block_num = 1         ; Number of blocks to process
tx_num = 1000         ; Number of transactions per block.
skew = 0.1            ; Zipfian skew factor for transaction distribution
```

> You can modify the values as needed. The program will read `config.ini` from the current working directory.