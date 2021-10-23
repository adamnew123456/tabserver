source core.tcl

proc max {a b} {
    if {$a > $b} {
        return $a
    } else {
        return $b
    }
}

proc prompt {message} {
    puts -nonewline "$message> "
    flush stdout
}

proc print_rows {rows} {
    if {[llength $rows] == 0} {
        return
    }

    set col_sizes {}
    set col_count [llength [lindex $rows 0]]
    while {$col_count >= 0} {
        lappend col_sizes 0
        incr col_count -1
    }

    foreach row $rows {
        set col_count [llength $row]
        while {$col_count > 0} {
            incr col_count -1
            set col_data [lindex $row $col_count]
            lset col_sizes $col_count [max [lindex $col_sizes $col_count] [string length $col_data]]
        }
    }

    foreach row $rows {
        set i 0
        set col_count [llength $row]
        while {$i < $col_count} {
            set col_data [lindex $row $i]
            set justify [lindex $col_sizes $i]
            incr justify -[string length $col_data]

            puts -nonewline " | [string repeat { } $justify] $col_data"
            incr i
        }

        puts "|"
    }
}

proc print_table {metadata rows} {
    set col_names {}
    set col_types {}

    foreach {name type} $metadata {
        lappend col_names $name
        lappend col_types $type
    }

    print_rows [concat [list $col_names $col_types] $rows]
    puts "~~ Rows: [llength $rows] ~~"
}

proc print_metadata {metadata} {
    set col_names {}
    set col_types {}

    foreach {name type} $metadata {
        lappend col_names $name
        lappend col_types $type
    }

    print_rows [list $col_names $col_types]
}

if {$argc == 0 || $argc > 2} {
    puts "$argv0 SERVER \[PORT\]"
    exit 1
}

if {$argc == 2} {
    set server [lindex $argv 0]
    set port [lindex $argv 1]
} else {
    set server [lindex $argv 0]
    set port 3096
}

set conn [tabquery_connect $server $port]

set query {}
set prepare 0
set done 0

while {!$done} {
    if {$prepare} {
        prompt "prepare"
    } else {
        prompt "sql"
    }

    gets stdin command

    set execute 0
    switch $command {
        /exit {
            set done 1
        }

        /quit {
            set done 1
        }

        /help {
            puts "Meta-commands: /help /quit /exit /prepare /block /abort"
        }

        /prepare {
            set prepare 1
        }

        /block {
            set query {}
            puts "Entering block mode. Must end with /abort or /exec"

            set blockdone 0
            while {!$blockdone} {
                prompt "block"
                gets stdin command

                switch $command {
                    /abort {
                        set blockdone 1
                    }

                    /exec {
                        set blockdone 1
                        set execute 1
                    }

                    default {
                        set query "$query\n$command"
                    }
                }
            }
        }

        /abort {
            set prepare 0
        }

        default {
            set query $command
            set execute 1
        }
    }

    if $execute {
        if $prepare {
            set failed [catch {
                set metadata [tabquery_prepare $conn $query]
                print_metadata $metadata
            } err]

            if $failed {
                puts "$err"
            }

            set prepare 0
        } else {
            set failed [catch {
                set output [tabquery_execute $conn $query]
                set metadata [lindex $output 0]
                set rows [lrange $output 1 end]

                print_table $metadata $rows
            } err]

            if $failed {
                puts "$err"
            }
        }
    }
}

tabquery_close $conn
