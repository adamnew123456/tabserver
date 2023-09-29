/**
 * The type of command that a BrokerCommand encodes.
 *
 * The values of this enum correspond to the opcodes used in the broker's binary
 * protocol. */
export const CommandTypes = {
    Hello: 0,
    Goodbye: 1,
    Send: 2,
} as const;

export type CommandType = typeof CommandTypes[keyof typeof CommandTypes];

/**
 * Interface implemented by all command objects. */
export interface BrokerCommand {
    type: CommandType;

    /** How many bytes this message occupies when encoded. */
    encodedSize(): number;

    /**
     * Encodes this message into a buffer using the data view.
     *
     * The buffer backing the view must be writable and at least as large as the
     * value returned by encodedSize.*/
    encodeInto(view: DataView): void;
}

/**
 * Command sent by the broker, indicating that */
export class HelloCommand implements BrokerCommand {
    type: CommandType = CommandTypes.Hello;
    constructor(
        public readonly id: number,
        public readonly name: string
    ) {}

    encodedSize(): number {
        return 7 + this.name.length;
    }

    encodeInto(view: DataView) {
        view.setUint8(0, CommandTypes.Hello);
        view.setInt32(1, this.id, true);
        view.setUint16(5, this.name.length, true);

        let offset = 7;
        for (let i = 0; i < this.name.length; i++) {
            view.setUint8(offset++, this.name.charCodeAt(i));
        }
    }
}

export class GoodbyeCommand implements BrokerCommand {
    type: CommandType = CommandTypes.Goodbye;
    constructor(
        public readonly id: number
    ) {}

    encodedSize(): number {
        return 5;
    }

    encodeInto(view: DataView) {
        view.setUint8(0, CommandTypes.Goodbye);
        view.setInt32(1, this.id, true);
    }
}

export class SendCommand implements BrokerCommand {
    type: CommandType = CommandTypes.Send;
    constructor(
        public readonly id: number,
        public readonly content: string
    ) {}

    encodedSize(): number {
        return 7 + this.content.length;
    }

    encodeInto(view: DataView) {
        view.setUint8(0, CommandTypes.Send);
        view.setInt32(1, this.id, true);
        view.setUint16(5, this.content.length, true);

        let offset = 7;
        for (let i = 0; i < this.content.length; i++) {
            view.setUint8(offset++, this.content.charCodeAt(i));
        }
    }
}

/**
 * Parses the command encoded into the given data view.
 *
 * Throws an Error if the command opcode is not recognized, or a RangeError if
 * the buffer is too small. */
function decodeCommand(view: DataView): BrokerCommand {
    let opcode = view.getUint8(0);
    switch (opcode) {
        case CommandTypes.Hello: {
            let id = view.getInt32(1, true);
            let nameLength = view.getUint16(5, true);
            let chars: number[] = [];
            for (let i = 0; i < nameLength; i++) {
                chars.push(view.getUint8(i + 7));
            }

            let nameEncoded = String.fromCodePoint.apply(null, chars);
            const nameBytes = atob(nameEncoded);
            let bytes = Uint8Array.from(nameBytes, (m) => m.codePointAt(0));
            let name = new TextDecoder().decode(bytes);

            return new HelloCommand(id, name);
        }

        case CommandTypes.Goodbye: {
            let id = view.getInt32(1, true);
            return new GoodbyeCommand(id);
        }

        case CommandTypes.Send: {
            let id = view.getInt32(1, true);
            let commandLength = view.getUint16(5, true);
            let chars: number[] = [];
            for (let i = 0; i < commandLength; i++) {
                chars.push(view.getUint8(i + 7));
            }
            let command = String.fromCodePoint.apply(null, chars);
            return new SendCommand(id, command);
        }

        default:
            throw new Error(`Cannot decode command: unknown opcode ${opcode}`);
    }
}

/**
 * The events that a BrokerConnection fires when it receives different types of
 * messages, or the connection state changes. */
export interface BrokerEvents {
    /** A new client has connected to the broker. */
    onHello(command: HelloCommand): void;

    /** A client has disconnected to the broker. */
    onGoodbye(command: GoodbyeCommand): void;

    /** A TabServer message has been sent by a client.
     *
     * The message may not be a complete message in terms of the TabServer
     * protocol, or may contain parts from more than one TabServer message. */
    onSend(command: SendCommand): void;

    /**
     * The connection to the broker has closed along with any connected clients.
     *
     * If there are any clients still active, onGoodbye events will not be fired
     * for them. */
    onTerminate(): void;
}

/**
 * Wraps a WebSocket connection to the broker, processing messages as the broker
 * sends them and allows sending data to invidiual clients connected to the
 * broker. */
export class BrokerConnection {
    private connection: WebSocket

    constructor(
        uri: string,
        private readonly sink: BrokerEvents
    ) {
        this.connection = new WebSocket(uri);
        this.connection.binaryType = 'arraybuffer';
        this.connection.addEventListener('message', (e) => { this.onMessage(e); });
        this.connection.addEventListener('close', (e) => { this.onClose(); });
        this.connection.addEventListener('error', (e) => { this.onClose(); });
    }

    /**
     * Sends a message to the client.
     *
     * The content must have already been encoded properly for the TabServer
     * protocol. It is assumed to contain base64 text only. */
    public sendToClient(id: number, content: string) {
        let message = new SendCommand(id, content);
        let buffer = new ArrayBuffer(message.encodedSize());
        message.encodeInto(new DataView(buffer));
        this.connection.send(buffer);
    }

    /**
     * Parses the message received over the WebSocket and dispatches it to the
     * appropriate broker event handler.*/
    private onMessage(event: MessageEvent) {
        let command = decodeCommand(new DataView(event.data));
        switch (command.type) {
            case CommandTypes.Hello:
                this.sink.onHello(command as HelloCommand);
                break;
            case CommandTypes.Goodbye:
                this.sink.onGoodbye(command as GoodbyeCommand);
                break;
            case CommandTypes.Send:
                this.sink.onSend(command as SendCommand);
                break;
        }
    }

    private onClose() {
        this.sink.onTerminate();
    }
}
