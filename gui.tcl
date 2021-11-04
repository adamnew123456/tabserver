package require Tk

source core.tcl

global SERVER
global PORT

proc max {a b} {
    if {$a > $b} {
        return $a
    } else {
        return $b
    }
}

proc connect {} {
    global SERVER
    global PORT
    global CONN
    set CONN [tabquery_connect $SERVER $PORT]
}

set OUTPUT_COUNTER 0

proc text_label {name text tag args} {
    set lines 0
    set maxlength 0
    foreach line [split $text "\n"] {
        set len [string length $line]
        if {$len > $maxlength} {
            set maxlength $len
        }
        incr lines
    }

    tk::text $name {*}$args
    $name insert 1.0 $text $tag
    $name configure -state disabled -width $maxlength -height $lines
}

proc add_query_label {query} {
    global OUTPUT_COUNTER
    set result_label ".canvas.history.frame.$OUTPUT_COUNTER"
    incr OUTPUT_COUNTER

    text_label $result_label $query message
    $result_label tag configure message -justify left -wrap word
    pack $result_label -fill x
}

proc add_error_label {result} {
    global OUTPUT_COUNTER
    set error_label ".canvas.history.frame.$OUTPUT_COUNTER"
    incr OUTPUT_COUNTER

    text_label $error_label $result message
    $error_label tag configure message -justify left -wrap word -foreground red
    pack $error_label -fill x
}

proc build_table {result} {
    global OUTPUT_COUNTER
    set result_grid ".canvas.history.frame.$OUTPUT_COUNTER"
    incr OUTPUT_COUNTER

    set buffer {}
    if {[llength $result] == 0} {
        text_label $result_grid "<empty>" data
        pack $result_grid -anchor w
        return
    }

    set col_sizes {}
    set col_count [llength [lindex $result 0]]
    while {$col_count >= 0} {
        lappend col_sizes 0
        incr col_count -1
    }

    foreach row $result {
        set col_count [llength $row]
        while {$col_count > 0} {
            incr col_count -1
            set col_data [lindex $row $col_count]
            lset col_sizes $col_count [max [lindex $col_sizes $col_count] [string length $col_data]]
        }
    }

    foreach row $result {
        set i 0
        set col_count [llength $row]
        while {$i < $col_count} {
            set col_data [lindex $row $i]
            set justify [lindex $col_sizes $i]
            incr justify -[string length $col_data]

            set buffer "$buffer | $col_data [string repeat { } $justify] "
            incr i
        }

        set buffer "$buffer|\n"
    }


    text_label $result_grid $buffer data
    pack $result_grid -anchor w
}

proc query_action {action} {
    global CONN
    set sql [string trim [.sql get 1.0 end]]
    set was_error [catch {$action $CONN $sql} result]
    add_query_label $sql

    if {$was_error} {
        add_error_label $result
    } else {
        build_table $result
    }

    # Only run this after the canvas rescales itself for the new widgets
    after idle {
        .canvas.history yview moveto 1
    }
}

proc build_ui {} {
    global OUTPUT_COUNTER

    ttk::frame .canvas
    tk::canvas .canvas.history -xscrollcommand {.canvas.history_scrlx set} -yscrollcommand {.canvas.history_scrly set}
    tk::frame .canvas.history.frame
    tk::text .sql -height 6
    tk::scrollbar .canvas.history_scrlx -orient horizontal -command {.canvas.history xview}
    tk::scrollbar .canvas.history_scrly -orient vertical -command {.canvas.history yview}
    tk::frame .buttons

    tk::button .buttons.execute -text Execute -command {
        query_action tabquery_execute
    }

    tk::button .buttons.prepare -text Prepare -command {
        query_action tabquery_prepare
    }

    tk::button .buttons.clear -text Clear -command {
        global OUTPUT_COUNTER
        while {$OUTPUT_COUNTER > 0} {
            incr OUTPUT_COUNTER -1
            destroy .canvas.history.frame.$OUTPUT_COUNTER
        }

        # Let the canvas rescale itself for the size of the frame
        after idle {
            .canvas.history configure -scrollregion {0 0 0 0}
            .canvas.history xview moveto 0
            .canvas.history yview moveto 0
        }
    }

    tk::button .buttons.reconnect -text Reconnect -command {
        global CONN
        add_query_label "<Reconnect requested>"
        tabquery_close $CONN
        connect
    }

    bind .canvas.history.frame "<Configure>" {
        .canvas.history configure -scrollregion [list %x %y %w %h]
    }

    bind .canvas.history "<Configure>" {
        .canvas.history itemconfigure .canvas.history.frame -width %w
    }

    .canvas.history create window 0 0 -anchor nw -window .canvas.history.frame

    pack .canvas.history_scrly -side right -fill y
    pack .canvas.history -fill both -expand 1
    pack .canvas.history_scrlx -side bottom -fill x
    pack .canvas -fill both -expand 1
    pack .buttons.reconnect -side right -expand 1 -fill x
    pack .buttons.clear -side right -expand 1 -fill x
    pack .buttons.prepare -side right -expand 1 -fill x
    pack .buttons.execute -side right -expand 1 -fill x
    pack .buttons -fill x -side bottom
    pack .sql -side bottom -fill x
}

if {$argc == 0 || $argc > 2} {
    puts "$argv0 SERVER \[PORT\]"
    exit 1
}

if {$argc == 2} {
    set SERVER [lindex $argv 0]
    set PORT [lindex $argv 1]
} else {
    set SERVER [lindex $argv 0]
    set PORT 3306
}

connect

build_ui
