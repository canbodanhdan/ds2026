from mpi4py import MPI
import os
import sys

# Constants for MPI Tags
# We use tags to separate metadata messages from raw data messages
TAG_METADATA = 1
TAG_DATA = 2
CHUNK_SIZE = 4096  # 4KB chunks

def run_sender(comm, dest_rank, filename):
    """
    Logic for the Sender (Rank 0)
    Reads a file and sends it to the destination rank.
    """
    if not os.path.isfile(filename):
        print(f"[Sender] Error: File '{filename}' not found.")
        # Send None to notify receiver to abort
        comm.send(None, dest=dest_rank, tag=TAG_METADATA)
        return

    filesize = os.path.getsize(filename)
    print(f"[Sender] Sending '{filename}' ({filesize} bytes) to Rank {dest_rank}...")

    # 1. Send Metadata
    # mpi4py's lowercase 'send' method uses pickle, so we can send Python dicts directly.
    metadata = {'filename': os.path.basename(filename), 'filesize': filesize}
    comm.send(metadata, dest=dest_rank, tag=TAG_METADATA)

    # 2. Send File Content
    sent_bytes = 0
    with open(filename, 'rb') as f:
        while True:
            chunk = f.read(CHUNK_SIZE)
            if not chunk:
                break
            comm.send(chunk, dest=dest_rank, tag=TAG_DATA)
            sent_bytes += len(chunk)

    # 3. Send EOF Signal (Empty bytes) to indicate end of transmission
    comm.send(b'', dest=dest_rank, tag=TAG_DATA)
    print(f"[Sender] Transfer complete. Sent {sent_bytes} bytes.")


def run_receiver(comm, source_rank):
    """
    Logic for the Receiver (Rank 1)
    Listens for a file from the source rank and writes it to disk.
    """
    print(f"[Receiver] Waiting for file from Rank {source_rank}...")

    # 1. Receive Metadata
    metadata = comm.recv(source=source_rank, tag=TAG_METADATA)
    
    if metadata is None:
        print("[Receiver] Sender aborted (File not found).")
        return

    # Prefix the filename so we don't overwrite the original if running in the same folder
    filename = "mpi_recv_" + metadata['filename']
    filesize = metadata['filesize']
    print(f"[Receiver] Incoming file: '{filename}' expecting {filesize} bytes.")

    # 2. Receive Data Loop
    received_bytes = 0
    with open(filename, 'wb') as f:
        while True:
            # We receive chunks until we get an empty byte string
            chunk = comm.recv(source=source_rank, tag=TAG_DATA)
            
            if not chunk: # Empty bytes means EOF
                break
                
            f.write(chunk)
            received_bytes += len(chunk)

    print(f"[Receiver] Saved to '{filename}'. Total bytes: {received_bytes}")


if __name__ == "__main__":
    # Initialize MPI environment
    comm = MPI.COMM_WORLD
    rank = comm.Get_rank()
    size = comm.Get_size()

    # This script requires exactly 2 processes to demonstrate 1-to-1 transfer
    if size < 2:
        if rank == 0:
            print("Error: This program requires at least 2 MPI processes.")
            print("Run with: mpiexec -n 2 python mpi_transfer.py <file_to_send>")
        sys.exit(1)

    # Rank 0 acts as the Sender
    if rank == 0:
        # Check command line args for filename
        if len(sys.argv) < 2:
            print("Usage: mpiexec -n 2 python mpi_transfer.py <file_to_send>")
            # Send abort signal to receiver if we can't run
            comm.send(None, dest=1, tag=TAG_METADATA)
            sys.exit(1)
            
        file_to_send = sys.argv[1]
        run_sender(comm, dest_rank=1, filename=file_to_send)
        
    # Rank 1 acts as the Receiver
    elif rank == 1:
        run_receiver(comm, source_rank=0)
    
    # Any other ranks stay idle
    else:
        print(f"[Rank {rank}] Idle. Only Ranks 0 and 1 are used in this demo.")