package require Tk

source core.tcl

global SERVER
global PORT

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

    set rownum 0
    tk::frame $result_grid -relief groove -borderwidth 2
    foreach row $result {
        set colnum 0
        foreach col $row {
            set cell "$result_grid.${rownum}_${colnum}"
            text_label $cell $col data

            if {$rownum < 2} {
                $cell tag configure data -justify center -wrap char -underline true
            } else {
                $cell tag configure data -justify right -wrap char
            }

            grid $cell -row $rownum -column $colnum
            incr colnum
        }
        incr rownum
    }


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

    .canvas.history yview moveto 1
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

        .canvas.history yview moveto 0
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
