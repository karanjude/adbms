mysqladmin5 variables | grep key_buffer_size > var.txt
mysqladmin5 variables | grep innodb_buffer_pool_size >> var.txt
mysqladmin5 variables | grep innodb_additional_mem_pool_size >> var.txt
mysqladmin5 variables | grep innodb_log_buffer_size >> var.txt
mysqladmin5 variables | grep query_cache_size >> var.txt
mysqladmin5 variables | grep read_buffer_size >> var.txt
mysqladmin5 variables | grep read_rnd_buffer_size >> var.txt
mysqladmin5 variables | grep  '| sort_buffer_size*' >> var.txt
mysqladmin5 variables | grep thread_stack >> var.txt
mysqladmin5 variables | grep join_buffer_size >> var.txt
mysqladmin5 variables | grep max_connections >> var.txt