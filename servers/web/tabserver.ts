import { BrokerConnection } from './broker';

/**
 * The type of message most recently sent to the TabServer client.
 */
export const TabServerCommands = {
    /**  A request to execute a query */
    Execute: 0,

    /**  A request to prepare a query */
    Prepare: 1,
} as const;

export type TabServerCommand = typeof TabServerCommands[keyof typeof TabServerCommands];

/**
 * The part of the TabServer message that the parser is expecting next.
 */
const TabServerParserStates = {
    /** Sent a query or prepare request, can receive any response. */
    AwaitReply: 0,

    /** Received an "ERROR" response, awaiting error message data. */
    AwaitErrorMessage: 1,

    /** Received a "METADATA" response, awaiting the column count. */
    AwaitMetadataCount: 2,

    /** Awaiting a column name for "METADATA". */
    AwaitMetadataName: 3,

    /** Awaiting a column type for "METADATA". */
    AwaitMetadataType: 4,

    /** Awaiting either a "ROW", a "PAGE", an "END", or an "ERROR" */
    AwaitRow: 5,

    /** Awaiting columns from a "ROW" */
    InRow: 6,

    /** Received an "AFFECTED", awaiting the count. */
    AwaitAffectedCount: 7,
} as const;

type TabServerParserState = typeof TabServerParserStates[keyof typeof TabServerParserStates];

export interface TabServerConnectionEvents {
    /** Called when an "ERROR" response has been parsed. */
    onError(message: string): void;

    /** Called when an "AFFECTED" response has been parsed. */
    onAffected(rows: number): void;

    // Metadata sequence:
    //   onStartMetadata()
    //     onMetadataColumn("a", "INT")
    //     onMetadataColumn("b", "VARCHAR")
    //     onMetadataColumn("c", "FLOAT")
    //   onEndMetadata()

    onStartMetadata(): void;
    onMetadataColumn(name: string, type: string): void;
    onEndMetadata(): void;

    // Row sequence:
    //   if has rows:
    //     onNextPage(callback);
    //       if callback(true)
    //         onStartRow()
    //           onColumnValue("1")
    //           onColumnValue("foo")
    //           onColumnValue("3.141")
    //         onEndRow()
    //         onStartRow()
    //           onColumnValue("2")
    //           onColumnValue("bar")
    //           onColumnValue("2.714")
    //         onEndRow()
    //         ...
    //   if no more rows or callback(false):
    //     onQueryEnd()

    onStartRow(): void;
    onColumnValue(value: string): void;
    onFinishRow(): void;

    onEndQuery(): void;
    onNextPage(sendResponse: (fetchNextPage: boolean) => void): void;
}

export class TabServerConnection {
    private buffer: string;
    private state: TabServerParserState;
    private lastCommand: TabServerCommand | null;
    private encoder: TextEncoder;
    private decoder: TextDecoder;

    private expectedRowsForCommand: number;
    private columnCount: number;
    private lastLine: string;

    constructor(
        private readonly connection: BrokerConnection,
        private readonly clientId: number,
        private readonly sink: TabServerConnectionEvents,
    ) {
        this.buffer = '';
        this.state = TabServerParserStates.AwaitReply;
        this.expectedRowsForCommand = 0;
        this.lastCommand = null;
        this.encoder = new TextEncoder();
        this.decoder = new TextDecoder();
    }

    /**
     * Sends a command to execute a query.
     *
     * Throws an Error if the parser is still processing results from the
     * previous query. Only one query may be in flight on a given client. */
    sendExecuteCommand(query: string) {
        if (this.lastCommand != null) {
            throw new Error('Cannot execute query while processing last action');
        }
        this.lastCommand = TabServerCommands.Execute;
        let command = `EXECUTE\n${this.utf8ToBase64(query)}\n`;
        this.connection.sendToClient(this.clientId, command);
    }

    /**
     * Sends a command to prepare a query.
     *
     * Throws an Error if the parser is still processing results from the
     * previous query. Only one query may be in flight on a given client. */
    sendPrepareCommand(query: string) {
        if (this.lastCommand != null) {
            throw new Error('Cannot execute query while processing last action');
        }
        this.lastCommand = TabServerCommands.Prepare;
        let command = `PREPARE\n${this.utf8ToBase64(query)}\n`;
        this.connection.sendToClient(this.clientId, command);
    }

    /**
     * Processes a chunk of TabServer data returned by the client. */
    feed(content: string): void {
        let lineEnding = content.indexOf('\n');
        if (lineEnding == -1) {
            this.buffer += content;
            return;
        }

        let line = this.buffer + content.substring(0, lineEnding);
        this.processLine(line);

        let lineStart = lineEnding + 1;
        lineEnding = content.indexOf('\n', lineStart);
        while (lineEnding != -1) {
            this.processLine(content.substring(lineStart, lineEnding));
            lineStart = lineEnding + 1;
            lineEnding = content.indexOf('\n', lineStart);
        }

        this.buffer = content.substring(lineStart);
    }

    private processLine(line: string): void {
        if (this.lastCommand == null) {
            throw new Error('Cannot process TabServer protocol without last command type');
        }

        switch (this.state) {
            case TabServerParserStates.AwaitReply: {
                if (line == 'ERROR') {
                    this.state = TabServerParserStates.AwaitErrorMessage;
                } else if (line == 'METADATA') {
                    this.state = TabServerParserStates.AwaitMetadataCount;
                } else if (line == 'AFFECTED') {
                    this.state = TabServerParserStates.AwaitAffectedCount;
                } else {
                    throw new Error(`In state ${this.state}: Unexpected ${line}`);
                }
                break;
            }

            case TabServerParserStates.AwaitErrorMessage: {
                this.sink.onError(this.utf8FromBase64(line));
                this.reset();
                break;
            }

            case TabServerParserStates.AwaitMetadataCount: {
                this.columnCount = Number(line);
                this.expectedRowsForCommand = this.columnCount;
                this.sink.onStartMetadata();
                if (this.expectedRowsForCommand == 0) {
                    this.nextStateAfterMetadata();
                } else {
                    this.state = TabServerParserStates.AwaitMetadataName;
                }
                break;
            }

            case TabServerParserStates.AwaitMetadataName: {
                this.lastLine = this.utf8FromBase64(line);
                this.state = TabServerParserStates.AwaitMetadataType;
                break;
            }

            case TabServerParserStates.AwaitMetadataType: {
                this.sink.onMetadataColumn(this.lastLine, this.utf8FromBase64(line));
                this.expectedRowsForCommand--;
                if (this.expectedRowsForCommand == 0) {
                    this.nextStateAfterMetadata();
                } else {
                    this.state = TabServerParserStates.AwaitMetadataName;
                }
                break;
            }

            case TabServerParserStates.AwaitRow: {
                if (line == 'ERROR') {
                    this.state = TabServerParserStates.AwaitErrorMessage;
                } else if (line == 'PAGE') {
                    this.sink.onNextPage(this.sinkPageResponseCallback.bind(this));
                } else if (line == 'END') {
                    this.sink.onEndQuery();
                    this.reset();
                } else if (line == 'ROW') {
                    this.sink.onStartRow();
                    this.state = TabServerParserStates.InRow;
                    this.expectedRowsForCommand = this.columnCount;
                } else {
                    throw new Error(`In state ${this.state}: Unexpected ${line}`);
                }
                break;
            }

            case TabServerParserStates.InRow: {
                let data = this.utf8FromBase64(line);
                this.sink.onColumnValue(data);

                this.expectedRowsForCommand--;
                if (this.expectedRowsForCommand == 0) {
                    this.sink.onFinishRow();
                    this.state = TabServerParserStates.AwaitRow;
                }
                break;
            }

            case TabServerParserStates.AwaitAffectedCount: {
                this.sink.onAffected(Number(line));
                this.reset();
                break;
            }
        }
    }

    private sinkPageResponseCallback(fetchNextPage: boolean) {
        if (fetchNextPage) {
            this.connection.sendToClient(this.clientId, "MORE\n1\n");
            // Don't need to adjust the state here, we were waiting for a row
            // before and we're still waiting for it
        } else {
            this.connection.sendToClient(this.clientId, 'ABORT\n');
        }
    }

    private nextStateAfterMetadata() {
        this.sink.onEndMetadata();
        if (this.lastCommand == TabServerCommands.Execute) {
            this.state = TabServerParserStates.AwaitRow;
        } else {
            this.sink.onEndQuery();
            this.reset();
        }
    }

    private reset() {
        this.state = TabServerParserStates.AwaitReply;
        this.lastCommand = null;
    }

    // Adapted from:
    // https://developer.mozilla.org/en-US/docs/Glossary/Base64#the_unicode_problem

    private utf8FromBase64(base64: string) {
        const binString = atob(base64);
        let bytes = Uint8Array.from(binString, (m) => m.codePointAt(0));
        return this.decoder.decode(bytes);
    }

    private utf8ToBase64(text: string): string {
        let bytes = this.encoder.encode(text);
        const binString = Array.from(bytes, (x) => String.fromCodePoint(x)).join('');
        return btoa(binString);
    }
}
