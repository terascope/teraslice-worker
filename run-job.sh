#!/bin/bash

start_ex() {
    echo "* starting execution controller"
    local job="$1"

    node command.js \
        --assignment 'execution_controller' \
        --job "$job" &

    return $!
}

start_worker() {
    local job="$1"

    node command.js \
        --assignment 'worker' \
        --job "$job" &

    return $!
}

main() {
   local jobFile="$1" 
   local job
   local workers
   local exPid

   job="$(jq -c -M '.' "$jobFile")"

   workers="$(jq '.workers' "$jobFile")"
   workers="${workers:-1}"

   exPid="$(start_ex "$job")"

   echo "* sleeping for 10 seconds"
   sleep 10

   for worker in $(seq 0 "$workers"); do
      echo "* starting worker $((worker+1))"
      start_worker "$job"
   done

   wait "$exPid"

   kill "$(jobs -p)"

   wait "$(jobs -p)"
}

main "$@"