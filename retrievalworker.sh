# PROCESS_MODULES=./graphsync_worker,./http_worker,./bitswap_worker
export PROCESS_MODULES=./http_worker
export PROCESS_ERROR_INTERVAL=5s
export TASK_WORKER_POLL_INTERVAL=30s
export TASK_WORKER_TIMEOUT_BUFFER=10s
export QUEUE_MONGO_URI=mongodb+srv://user:pass@host/?retryWrites=true&w=majority
export QUEUE_MONGO_DATABASE=test
export RESULT_MONGO_URI=mongodb+srv://user:pass@host/?retryWrites=true&w=majority
export RESULT_MONGO_DATABASE=test
export ACCEPTED_CONTINENTS=
export ACCEPTED_COUNTRIES=
export CONCURRENCY_GRAPHSYNC_WORKER=10
export CONCURRENCY_BITSWAP_WORKER=10
export CONCURRENCY_HTTP_WORKER=10
export GOLOG_LOG_LEVEL=panic,convert=info,env=debug,bitswap_client=info,graphsync_client=info,http_client=info,process-manager=info,task-worker=info,bitswap_worker=info
export GOLOG_LOG_FMT=json

./retrieval_worker
