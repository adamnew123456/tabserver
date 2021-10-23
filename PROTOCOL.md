# tabserver Protocol

- Line-based for ease of use with Tcl (line here meaning linefeed)
- Single-threaded so queries can't be easily cancelled
- Supports prepared statement to report just the schema that a query would report
- Supports a max return count so the viewer doesn't have to render everything
- Non-command text is assumed to be encoded as UTF-8 and base64 encoded

## Lifecycle

1. The server (which manages the database connection) accepts a connection on
   port 3096.

2. The client sends a command.

3. The server sends a response.

4. GOTO 2 until the connection is closed.

## Client Commands

- `EXECUTE` The line after the `EXECUTE` must be a SQL query. This query is
  executed and returns:
  - An `ERROR` response
  - A `METADATA` response, then one or more `ROW` responses, and finally an
    `END` response or an `ERROR` response.
  - An `AFFECTED` response
    
- `PREPARE` The line after the `PREPARE` must be a SQL query. The query is
  prepared and its metadata fetched, but it is not executed. This returns:
  - An `ERROR` response
  - A `METADATA` response

- Any other command will trigger an `ERROR` response

After the initial command, but before the trailing query, the server must send
an `OK` response. This is to avoid the client and server disagreeing about where
the next command starts if one of the commands is invalid.

For example, if the client tried to send an illegal command with some arguments
and there was no acknowledgment:

```
client> PREPARE
client> SELECT ...
server> METADATA
server> ...
client> BAD
server> ERROR
server> Illegal command
client> -1
server> ERROR
server> Illegal command
```

With the acknowledgment this is avoided because the client will not send
arguments after the initial `ERROR` response:

```
client> PREPARE
server> OK
client> SELECT ...
server> METADATA
server> ...
client> BAD
server> ERROR
server> Illegal command
```
  
## Server Responses

- `OK` Indicates that the command is recognized and the client may continue.

- `ERROR` The line after the `ERROR` is a description of the error message

- `METADATA` The line after the `METADATA` is a base 10 number that gives the
  number of columns (it is not base64 encoded). Then two lines per column
  follows giving the column's label and type.
  
- `ROW` always follows an existing `METADATA` command, so the number of columns
  to expect is known in advance. One line per column follows the `ROW` command
  and contains the data rendered as a string.
  
- `END` indicates that no more rows follow.

- `AFFECTED` The line after the `AFFECTED` is a base 10 number that gives the
  number of rows affected by the previous command. Should only be returned for
  non-SELECT queries.

## Example session

Here strings are not base64 encoded for ease of reading:

```
client> EXECUTE
client> SELECT COUNT(*) FROM t
server> METADATA
server> 1
server> COUNT
server> INT
server> ROW
server> 10
server> END
client> PREPARE
client> SELECT * FROM t
server> METADATA
server> 3
server> id
server> INT
server> name
server> VARCHAR
server> balance
server> DECIMAL
client> INSERT INTO t(balance) VALUES (12.34)
server> AFFECTED
server> 1
```
