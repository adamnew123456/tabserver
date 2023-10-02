import * as broker from './broker';
import * as tabserver from './tabserver';
import hljs from 'highlight';

/**
 * A query and response pair that's recorded in the notebook for a particular client. */
class HistoryEntry {
    /**
     * Creates an entry from a query that returned an AFFECTED count. */
    public static fromAffected(query: string, affected: number): HistoryEntry {
        return new HistoryEntry(query, affected, null, null);
    }

    /**
     * Creates an entry from a query that returned a table of data. */
    public static fromCells(query: string, cells: string[][]): HistoryEntry {
        return new HistoryEntry(query, null, cells, null);
    }

    /**
     * Creates an entry from a query that failed. */
    public static fromError(query: string, error: string): HistoryEntry {
        return new HistoryEntry(query, null, null, error);
    }

    private constructor(
        public readonly query: string,
        public readonly affected: number | null,
        public readonly cells: string[][] | null,
        public readonly error: string | null
    ) {}

    /**
     * Generates HTML elements displaying this entry and adds them to the given node. */
    render(): {query: HTMLElement; result: HTMLElement} {
        let queryNode = this.buildSQLNode(this.query);
        hljs.highlightElement(queryNode.firstChild as HTMLElement);

        let resultNode: HTMLElement;
        if (this.affected != null) {
            resultNode = this.buildSQLNode(`-- Affected: ${this.affected}`);
            hljs.highlightElement(resultNode.firstChild as HTMLElement);
        } else if (this.cells != null) {
            resultNode = this.buildTable(this.cells);
        } else {
            resultNode = this.buildErrorNode(this.error);
        }

        return { query: queryNode, result: resultNode };
    }

    private buildSQLNode(query: string): HTMLElement {
        let queryElement = document.createElement('div');
        let preElement = document.createElement('pre');
        let codeElement = document.createElement('code');

        queryElement.classList.add('notebook-box', 'query');
        codeElement.classList.add('language-sql');
        codeElement.textContent = query;
        preElement.appendChild(codeElement);
        queryElement.appendChild(preElement);
        return queryElement;
    }

    private buildErrorNode(error: string): HTMLElement {
        let errorElement = document.createElement('div');
        let textElement = document.createElement('pre');

        errorElement.classList.add('notebook-box', 'error');
        textElement.textContent = error;
        errorElement.appendChild(textElement);
        return errorElement;
    }

    private buildTable(cells: string[][]): HTMLElement {
        let tableContainer = document.createElement('div');
        let tableElement = document.createElement('table');

        for (let row = 0; row < cells.length; row++) {
            let isHeader = row == 0 || row == 1;
            let tableRow = document.createElement('tr');
            let cellRow = cells[row];

            for (let col = 0; col < cellRow.length; col++) {
                let tableCell: HTMLElement;
                if (isHeader) {
                    tableCell = document.createElement('th');
                    tableCell.setAttribute('scope', 'col');
                } else {
                    tableCell = document.createElement('td');
                }
                tableCell.textContent = cellRow[col];
                tableRow.appendChild(tableCell);
            }

            tableElement.appendChild(tableRow);
        }

        tableContainer.classList.add('notebook-box', 'result');
        tableContainer.appendChild(tableElement);
        return tableContainer;
    }
}

/**
 * Accepts query events and builds up a table of row data. */
class TableBuilder {
    private columnNames: string[];
    private columnTypes: string[];
    private rows: string[][];
    private currentRow: string[];

    constructor(
        private readonly maxRows: number
    ) {}

    onStartMetadata() {
        this.columnNames = [];
        this.columnTypes = [];
    }

    onMetadataColumn(name: string, type: string) {
        this.columnNames.push(name);
        this.columnTypes.push(type);
    }

    onEndMetadata() {
        this.rows = [];
    }

    onStartRow() {
        this.currentRow = [];
    }

    onColumnValue(value: string) {
        this.currentRow.push(value);
    }

    onFinishRow() {
        this.rows.push(this.currentRow);
    }

    onNextPage(sendResponse: (fetchNextPage: boolean) => void) {
        sendResponse(this.rows.length < this.maxRows);
    }

    buildCells(): string[][] {
        return [this.columnNames, this.columnTypes].concat(this.rows);
    }

    currentRowCount(): number {
        return this.rows.length;
    }
}

/**
 * Dispatches commands from a client to update the UI.
 *
 * Note that this instance specific to each client, so we don't need to pass
 * around client identifiers everywhere. The implementation will work with the
 * object that *really* controls the UI elements to see if the UI can actually
 * be updated. Maybe it can't be because some other client is currently visible.
 * */
interface ClientUIHandle {
    /**
     * Sets the message on the status line. */
    setStatus(message: string): void;

    /**
     * Appends a message to the history window. */
    addHistory(entry: HistoryEntry): void;
}

class Client implements tabserver.TabServerConnectionEvents {
    /** Our link to the main UI. */
    ui: ClientUIHandle;

    /** All the queries executed against this client and their responses. */
    history: HistoryEntry[];

    /** The connection to the client, or null if it has been closed. */
    connection: tabserver.TabServerConnection | null;

    /** Data for the history entry that's currently being built */
    tableBuilder: TableBuilder | null;

    /** The currently executing query. Only set while the query is actually pending. */
    currentQuery: string | null;

    /** The maximum number of rows to return from the current query. */
    maxRows: number;

    constructor() {
        this.history = [];
        this.tableBuilder = null;
        this.currentQuery = null;
    }

    bindConnection(connection: tabserver.TabServerConnection) {
        this.connection = connection;
    }

    bindUIHandle(ui: ClientUIHandle) {
        this.ui = ui;
    }

    executeQuery(query: string, maxRows: number) {
        if (!this.checkIdle()) return;
        this.currentQuery = query;
        this.maxRows = maxRows;
        this.ui.setStatus('Executing...');
        this.connection.sendExecuteCommand(query);
    }

    prepareQuery(query: string) {
        if (!this.checkIdle()) return;
        this.currentQuery = query;
        this.ui.setStatus('Preparing...');
        this.connection.sendPrepareCommand(query);
    }

    receiveData(data: string) {
        this.connection.feed(data);
    }

    disconnect() {
        this.connection = null;
    }

    getHistory(): HistoryEntry[] {
        return this.history;
    }

    onError(message: string) {
        this.addToHistory(HistoryEntry.fromError(this.currentQuery, message));
        this.tableBuilder = null;
        this.ui.setStatus('Query failed');
    }

    onAffected(rows: number) {
        this.addToHistory(HistoryEntry.fromAffected(this.currentQuery, rows));
        this.ui.setStatus('Query returned affected rows');
    }

    onStartMetadata() {
        this.tableBuilder = new TableBuilder(this.maxRows);
        this.tableBuilder.onStartMetadata();
        this.ui.setStatus('Reading first page...');
    }

    onMetadataColumn(name: string, type: string) {
        this.tableBuilder.onMetadataColumn(name, type);
    }

    onEndMetadata() {
        this.tableBuilder.onEndMetadata();
    }

    onStartRow() {
        this.tableBuilder.onStartRow();
    }

    onColumnValue(value: string) {
        this.tableBuilder.onColumnValue(value);
    }

    onFinishRow() {
        this.tableBuilder.onFinishRow();
    }

    onEndQuery() {
        let cells = this.tableBuilder.buildCells();
        this.addToHistory(HistoryEntry.fromCells(this.currentQuery, cells));
        this.tableBuilder = null;
        this.ui.setStatus('Query complete');
    }

    onNextPage(sendResponse: (fetchNextPage: boolean) => void) {
        this.tableBuilder.onNextPage(sendResponse);
        this.ui.setStatus(`Received ${this.tableBuilder.currentRowCount()} rows...`);
    }

    private addToHistory(entry: HistoryEntry) {
        this.history.push(entry);
        this.ui.addHistory(entry);
        this.currentQuery = null;
    }

    private checkIdle(): boolean {
        if (this.connection == null) {
            this.ui.setStatus('Connection is dead');
            return false;
        }

        if (this.currentQuery != null) {
            this.ui.setStatus('Another query is executing, please wait...');
            return false;
        }

        return true;
    }
}

/**
 * Dispatches messages from the UI and the broker to individual clients. */
class ClientManager implements broker.BrokerEvents {
    private connection: broker.BrokerConnection;
    private ui: UIManager;
    private currentClient: number | null;
    private clients: Map<number, ClientManagerHandle>;

    constructor(uri: string) {
        this.connection = new broker.BrokerConnection(uri, this);
        this.ui = new UIManager(this);
        this.currentClient = null;
        this.clients = new Map<number, ClientManagerHandle>();

        this.ui.setStatus('Awaiting connection');
    }

    // UI request handlers
    prepareQuery(query: string) {
        if (!this.checkCurrentClientOpen()) return;
        this.clients.get(this.currentClient).prepareQuery(query);
    }

    executeQuery(query: string, maxRows: number) {
        if (!this.checkCurrentClientOpen()) return;
        this.clients.get(this.currentClient).executeQuery(query, maxRows);
    }

    switchToClient(target: number) {
        if (target != this.currentClient) {
            if (this.currentClient != null) {
                this.ui.switchFromClient(this.clients.get(this.currentClient));
            }

            this.currentClient = target;
            this.ui.switchToClient(this.clients.get(this.currentClient));
        }
    }

    private checkCurrentClientOpen(): boolean {
        if (this.currentClient == null) {
            this.ui.setStatus('No client available');
            return false;
        }

        if (!this.clients.get(this.currentClient).open) {
            this.ui.setStatus('Client is not active');
            return false;
        }

        return true;
    }

    // Client request handlers
    setStatus(client: number, message: string) {
        if (client == this.currentClient) {
            this.ui.setStatus(message);
        }
    }

    addHistory(client: number, entry: HistoryEntry) {
        if (client == this.currentClient) {
            this.ui.addToHistory(entry);
        }
    }

    // Broker protocol events
    onHello(command: broker.HelloCommand) {
        let client = new Client();
        let clientConnection = new tabserver.TabServerConnection(this.connection, command.id, client);
        let clientHandle = new ClientManagerHandle(command.id, this, client);
        client.bindConnection(clientConnection);
        client.bindUIHandle(clientHandle);
        this.clients.set(command.id, clientHandle);
        this.ui.addClient(command.id, command.name);
    }

    onGoodbye(command: broker.GoodbyeCommand) {
        this.disconnectClient(command.id);
    }

    onSend(command: broker.SendCommand) {
        this.clients.get(command.id).receiveData(command.content);
    }

    onTerminate() {
        this.clients.forEach((_, id) => this.disconnectClient(id));
        alert('Connection to broker lost');
    }

    private disconnectClient(id: number) {
        let client = this.clients.get(id);
        if (this.currentClient == id) {
            this.ui.switchFromClient(client);
            this.ui.switchToClient(client);
        }

        client.disconnect();
        this.ui.disableClient(id);
    }
}

/**
 * Performs UI actions and handles events on behalf of the client maanger. */
class UIManager {
    connectButton: HTMLButtonElement;
    clientList: HTMLElement;
    clientEntries: Map<number, HTMLElement>;
    notebookHistory: HTMLElement;
    notebookControls: HTMLElement;
    notebookStatus: HTMLElement;
    query: HTMLTextAreaElement;
    maxRows: HTMLInputElement;

    constructor(
        private readonly owner: ClientManager
    ) {
        this.clientList = this.elem('client-list');
        this.notebookHistory = this.elem('notebook-history');
        this.notebookControls = this.elem('notebook-controls');
        this.notebookStatus = this.elem('notebook-status');
        this.query = this.elem('query') as HTMLTextAreaElement;
        this.maxRows = this.elem('max-rows') as HTMLInputElement;
        this.clientEntries = new Map<number, HTMLElement>();

        this.elem('execute-query').addEventListener('click', (_) => this.onExecuteClicked());
        this.elem('prepare-query').addEventListener('click', (_) => this.onPrepareClicked());
    }

    addClient(id: number, label: string) {
        let clientEntry = document.createElement('div');
        clientEntry.innerText = label;
        clientEntry.classList.add('client-entry');
        clientEntry.addEventListener('click', () => this.onClientClicked(id));
        this.clientList.appendChild(clientEntry);
        this.clientEntries.set(id, clientEntry);
    }

    disableClient(id: number) {
        let clientEntry = this.clientEntries.get(id);
        clientEntry.classList.add('dead');
    }

    addToHistory(entry: HistoryEntry) {
        let newNodes = entry.render();
        newNodes.query.addEventListener('click', () => this.onQueryClicked(entry.query));

        this.notebookHistory.appendChild(newNodes.query);
        this.notebookHistory.appendChild(newNodes.result);
    }

    setStatus(status: string) {
        this.notebookStatus.innerText = status;
    }

    switchFromClient(client: ClientManagerHandle) {
        client.lastScroll = this.notebookHistory.scrollTop;
        client.lastQuery = this.query.value;
        this.clientEntries.get(client.id).classList.remove('selected');

    }

    switchToClient(client: ClientManagerHandle) {
        this.clientEntries.get(client.id).classList.add('selected');
        if (client.open) {
            this.notebookControls.classList.remove('hidden');
        } else {
            this.notebookControls.classList.add('hidden');
        }

        this.setStatus(client.lastStatus);
        this.query.value = client.lastQuery;

        // Avoid rendering partial history while we're rebuilding it
        this.notebookHistory.classList.add('hidden');
        this.clearChildren(this.notebookHistory);
        client.getHistory().forEach(e => this.addToHistory(e));
        this.notebookHistory.classList.remove('hidden');
    }

    private onPrepareClicked() {
        this.owner.prepareQuery(this.query.value);
    }

    private onExecuteClicked() {
        this.owner.executeQuery(this.query.value, this.getMaxRows());
    }

    private onQueryClicked(query: string) {
        this.query.value = query;
    }

    private onClientClicked(id: number) {
        this.owner.switchToClient(id);
    }

    private elem(id: string): HTMLElement {
        return document.getElementById(id);
    }

    private clearChildren(element: Element) {
        while (element.firstChild) {
            element.removeChild(element.firstChild);
        }
    }

    private getMaxRows(): number {
        let value = this.maxRows.valueAsNumber;
        if (isNaN(value) || value <= 0) {
            value = 1;
        }
        return value;
    }
}

/**
 * Bridge between the Client and the ClientManager.
 *
 * The Client uses this to run UI commands on the manager, while the manager
 * uses this to execute queries via the client and determine the UI state when a
 * previously-hidden client is selected by the user. */
class ClientManagerHandle implements ClientUIHandle {
    lastStatus: string;
    lastScroll: number;
    lastQuery: string;
    open: boolean;

    constructor(
        public readonly id: number,
        private readonly owner: ClientManager,
        private readonly client: Client
    ) {
        this.lastStatus = 'OK';
        this.lastQuery = '';
        this.lastScroll = 0;
        this.open = true;
    }

    // Manager -> client interface
    executeQuery(query: string, maxRows: number) {
        this.client.executeQuery(query, maxRows);
    }

    prepareQuery(query: string) {
        this.client.prepareQuery(query);
    }

    receiveData(data: string) {
        this.client.receiveData(data);
    }

    getHistory(): HistoryEntry[] {
        return this.client.getHistory();
    }

    disconnect() {
        this.open = false;
        this.client.disconnect();
    }

    // Client -> manager interface
    setStatus(message: string): void {
        this.lastStatus = message;
        this.owner.setStatus(this.id, message);
    }

    addHistory(entry: HistoryEntry): void {
        this.owner.addHistory(this.id, entry);
    }
}

function init() {
    let uri = prompt("Enter URL of broker", "ws://localhost:1234");
    try {
        new ClientManager(uri);
    } catch (err) {
        alert(`Could not open connection: ${err}`)
    }
}

init();
