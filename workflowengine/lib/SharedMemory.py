import posix_ipc, mmap

def create_shm(name, size):
    try:
        memory = posix_ipc.SharedMemory(name, posix_ipc.O_CREX, 0600, size)
    except posix_ipc.ExistentialError:
        posix_ipc.unlink_shared_memory(name)
        memory = posix_ipc.SharedMemory(name, posix_ipc.O_CREX, 0600, size)
    mapfile = mmap.mmap(memory.fd, memory.size)
    memory.close_fd()
    return mapfile

def open_shm(name):
    memory = posix_ipc.SharedMemory(name)
    mapfile = mmap.mmap(memory.fd, memory.size)
    memory.close_fd()
    return mapfile

def close_shm(name, mapfile, unlink=False):
    mapfile.close()
    if unlink:
        posix_ipc.unlink_shared_memory(name)

def write_shm(mapfile, string):
    mapfile.seek(0)
    mapfile.write(string + "\n")
