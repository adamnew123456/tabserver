# The callback is used to notify our caller about protocol events and to request
# protocol decitions. This command can called in a few ways:
#
#  callback_cmd error connection_id message
#    The client reported an error when trying to execute or prepare whatever query
#    was requested. This may also happen if the client sends unsolicated data.
#
#  callback_cmd metadata connection_id row
#    A row of metadata has been returned from the query. The metadata row is a list
#    of pairs, each containing a label and a type name.
#
#  callback_cmd data connection_id row
#    A row of data has been returned from the query.
#
#  callback_cmd page connection_id
#    The client has hit the end of the current page and the user must decide whether
#    to continue reading or not. At some point tabquery_resolve_page must be called
#    with the connection_id and a 1 or a 0.
#
#  callback_cmd page_confirm connection_id
#    Reports that tabquery_resolve_page was called, or that the timeout expired.
#
#  callback_cmd done connection_id
#    The query has successfully completed executing.
#
#  callback_cmd new_client connection_id descriptor
#    A new client has connected to the server.
#
#  callback_cmd drop_client connection_id
#    The client has disconnected from the server.
set callback_cmd {}

set connection_id 0

# A connection info consists of the channel itself along with a state and some
# other data. Possible states are:
#
# - AWAIT_HELLO: Waiting for the HELLO messages on a fresh connection.
#
# - IDLE: The connection is active, but no commands are currently executing on
#   it.
#
# - AWAIT_EXECUTE: The query has been sent, waiting for a METADATA line, an
#   ERROR line or an AFFECTED line
#
# - AWAIT_PREPARE: The query has been sent, waiting for either a METADATA line
#   or an ERROR line
#
# - {AWAIT_ROW columns}: The metadata is complete and we're waiting for
#   either a ROW, PAGE, END or ERROR line.
#
# - {PROMPT_PAGE columns timeout}: We received a PAGE and are waiting for our
#   caller to decide whether to continue reading or not. The answer must be
#   provided before the timeout (a Unix timestamp) has elapsed. If the timeout
#   elapses then an abort is automatically sent.
#
# - AWAIT_END: We sent an ABORT in response to a PAGE reply, and are waiting for
#   the END response.
#
array set connection_infos {}

proc tabquery_bind {port callback} {
    global callback_cmd
    set callback_cmd $callback

    socket -server tabquery_accept $port
    after 1000 tabquery_clean_timeouts
}

proc tabquery_accept {client addr port} {
    global connection_id connection_infos

    set id $connection_id
    incr connection_id

    set connection_infos($id) [list $client AWAIT_HELLO]

    fconfigure $client -eofchar "\n"
    chan configure $client -blocking 1 -buffering line
    chan event $client readable "tabquery_read $id"
}

proc tabquery_read {id} {
    global callback_cmd connection_infos

    set client_info $connection_infos($id)
    set client [lindex $client_info 0]
    set state [lindex $client_info 1]

    switch $state {
        AWAIT_HELLO {
            set command [tabquery_recv_raw $client]
            switch $command {
                HELLO {
                    set descriptor [tabquery_recv_data $client]
                    eval $callback_cmd new_client $id [list $descriptor]
                    set connection_infos($id) [list $client IDLE]
                }

                default {
                    # The connection isn't reported until after the HELLO
                    # message, so our caller won't know what connection this is
                    # if we report it
                    tabquery_kill_connection $id 0
                }
            }
        }

        IDLE {
            eval $callback_cmd error $id [list "Unsolicited data during IDLE state"]
            tabquery_kill_connection $id 1
        }

        AWAIT_EXECUTE {
            set reply [tabquery_recv_raw $client]
            switch $reply {
                METADATA {
                    set metadata_count [tabquery_recv_raw $client]
                    if {$metadata_count == ""} {
                        eval $callback_cmd error $id [list "Connection closed"]
                        tabquery_kill_connection $id 1
                        return
                    }

                    set metadata_row {}
                    for {set i 0} {$i < $metadata_count} {incr i} {
                        set col_label [tabquery_recv_data $client]
                        set col_type [tabquery_recv_data $client]
                        lappend metadata_row [list $col_label $col_type]
                    }

                    eval $callback_cmd metadata $id [list $metadata_row]
                    set connection_infos($id) [list $client AWAIT_ROW $metadata_count]
                }

                AFFECTED {
                    set affected_count [tabquery_recv_raw $client]
                    if {$affected_count == ""} {
                        eval $callback_cmd error $id [list "Connection closed"]
                        tabquery_kill_connection $id 1
                        return
                    }

                    eval $callback_cmd metadata $id [list {{AFFECTED_ROWS *}}]
                    eval $callback_cmd data $id [list $affected_count]
                    eval $callback_cmd done $id
                    set connection_infos($id) [list $client IDLE]
                }

                ERROR {
                    set message [tabquery_recv_data $client]
                    eval $callback_cmd error $id [list $message]
                    set connection_infos($id) [list $client IDLE]
                }

                default {
                    eval $callback_cmd error $id [list "Protocol violation: '$reply' not allowed in AWAIT_EXECUTE state"]
                    tabquery_kill_connection $id 1
                }
            }
        }

        AWAIT_PREPARE {
            set reply [tabquery_recv_raw $client]
            switch $reply {
                METADATA {
                    set metadata_count [tabquery_recv_raw $client]
                    if {$metadata_count == ""} {
                        eval $callback_cmd error $id [list "Connection closed"]
                        tabquery_kill_connection $id 1
                        return
                    }

                    set metadata_row {}
                    for {set i 0} {$i < $metadata_count} {incr i} {
                        set col_label [tabquery_recv_data $client]
                        set col_type [tabquery_recv_data $client]
                        lappend metadata_row [list $col_label $col_type]
                    }

                    eval $callback_cmd metadata $id [list $metadata_row]
                    eval $callback_cmd done $id
                    set connection_infos($id) [list $client IDLE]
                }

                ERROR {
                    set message [tabquery_recv_data $client]
                    eval $callback_cmd error $id [list $message]
                    set connection_infos($id) [list $client IDLE]
                }

                default {
                    eval $callback_cmd error $id [list "Protocol violation: '$reply' not allowed in AWAIT_PREPARE state"]
                    tabquery_kill_connection $id 1
                }
            }
        }

        AWAIT_ROW {
            set reply [tabquery_recv_raw $client]
            set column_count [lindex $client_info 2]

            switch $reply {
                ROW {
                    set row {}
                    for {set i 0} {$i < $column_count} {incr i} {
                        lappend row [tabquery_recv_data $client]
                    }

                    eval $callback_cmd data $id [list $row]
                }

                PAGE {
                    set timeout [clock seconds]
                    incr timeout 10
                    set connection_infos($id) [list $client PROMPT_PAGE $column_count $timeout]

                    eval $callback_cmd page $id
                }

                ERROR {
                    set message [tabquery_recv_data $client]
                    eval $callback_cmd error $id [list $message]
                    set connection_infos($id) [list $client IDLE]
                }

                END {
                    eval $callback_cmd done $id
                    set connection_infos($id) [list $client IDLE]
                }

                default {
                    eval $callback_cmd error $id [list "Protocol violation: '$reply' not allowed in AWAIT_ROW state"]
                    tabquery_kill_connection $id 1
                }
            }
        }

        AWAIT_END {
            set reply [tabquery_recv_raw $client]

            switch $reply {
                END {
                    eval $callback_cmd done $id
                    set connection_infos($id) [list $client IDLE]
                }

                default {
                    eval $callback_cmd error $id [list "Protocol violation: '$reply' not allowed in AWAIT_END state"]
                    tabquery_kill_connection $id 1
                }
            }
        }
    }
}

proc tabquery_clean_timeouts {} {
    global connection_infos
    set now [clock seconds]

    foreach {id client_info} [array get connection_infos] {
        set state [lindex $client_info 1]

        if {$state != "PROMPT_PAGE"} {
            continue
        }

        set timeout [lindex $client_info 3]
        if {$now >= $timeout} {
            tabquery_resolve_page $id 0
        }
    }

    after 1000 tabquery_clean_timeouts
}

proc tabquery_resolve_page {id keep_reading} {
    global callback_cmd connection_infos
    set client_info $connection_infos($id)
    set state [lindex $client_info 1]

    if {$state != "PROMPT_PAGE"} {
        return
    }

    set client [lindex $client_info 0]
    if $keep_reading {
        set columns [lindex $client_info 2]
        tabquery_send_raw $client MORE
        set connection_infos($id) [list $client AWAIT_ROW $columns]
    } else {
        tabquery_send_raw $client ABORT
        set connection_infos($id) [list $client AWAIT_END]
    }

    eval $callback_cmd page_confirm $id
}

proc tabquery_send_raw {client data} {
    puts $client $data
    flush $client
}

proc tabquery_send_data {client data} {
    tabquery_send_raw $client [binary encode base64 [encoding convertto utf-8 $data]]
}

proc tabquery_recv_raw {client} {
    gets $client data
    return $data
}

proc tabquery_recv_data {client} {
    return [encoding convertfrom utf-8 [binary decode base64 [tabquery_recv_raw $client]]]
}

proc tabquery_prepare {id query} {
    global connection_infos
    set client_info $connection_infos($id)
    set client [lindex $client_info 0]
    set state [lindex $client_info 1]

    if {$state != "IDLE"} {
        error "Cannot prepare a query while connection is in non-idle '$state' state"
    }

    tabquery_send_raw $client PREPARE
    tabquery_send_data $client $query
    set connection_infos($id) [list $client AWAIT_PREPARE]
}

proc tabquery_execute {id query} {
    global connection_infos
    set client_info $connection_infos($id)
    set client [lindex $client_info 0]
    set state [lindex $client_info 1]

    if {$state != "IDLE"} {
        error "Cannot execute a query while connection is in non-idle '$state' state"
    }

    tabquery_send_raw $client EXECUTE
    tabquery_send_data $client $query
    set connection_infos($id) [list $client AWAIT_EXECUTE]
}

proc tabquery_kill_connection {id notify} {
    global callback_cmd connection_infos

    if {![info exists connection_infos($id)]} {
        return
    }

    set client_info $connection_infos($id)
    set client [lindex $client_info 0]
    close $client

    if $notify {
        eval $callback_cmd drop_client $id
    }
}
