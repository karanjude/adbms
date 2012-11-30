import sys

f = open(sys.argv[1])

kv = {}

def to_mb(x):
    x = float(x) / float(1024)
    x = float(x) / float(1024)
    return x

for l in f:
    l = l.strip()
    l = l.replace("|","")
    l = l.split()
    kv[l[0]] = int(l[1])
    print l[0] , to_mb(l[1])

f.close()

max_connections = kv['max_connections']
total_per_thread_buffers = kv['read_buffer_size'] + kv['read_rnd_buffer_size'] + kv['sort_buffer_size']
total_per_thread_buffers += kv['thread_stack'] + kv['join_buffer_size']
server_buffers = kv['key_buffer_size'] + kv['query_cache_size'] + kv['innodb_log_buffer_size'] + kv['innodb_additional_mem_pool_size']
server_buffers += kv['innodb_buffer_pool_size']
total_memory_for_threads = to_mb(total_per_thread_buffers) * 100
total_memory = to_mb(server_buffers) + total_memory_for_threads

print total_memory , total_memory_for_threads, to_mb(server_buffers)
