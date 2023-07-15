import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.sql.*;
import java.util.Base64;
import java.util.NoSuchElementException;
import java.util.Scanner;

public class TabServer {
  private String _host;
  private int _port;
  private Connection _cnx;

  private Scanner _peerRecv;
  private BufferedWriter _peerSend;

  public static void main(String[] args) throws Exception {
    if (args.length != 4) {
      System.out.println("TabServer DRIVER CONNECTION-STRING SERVER PORT");
      return;
    }

    String driverName = args[0];
    String connectionString = args[1];
    String server = args[2];
    int port = Integer.parseInt(args[3]);

    Class.forName(driverName);
    Connection cnx = DriverManager.getConnection(connectionString);
    new TabServer(server, port, cnx).start();
  }

  public TabServer(String host, int port, Connection cnx) {
    _host = host;
    _port = port;
    _cnx = cnx;
  }

  private String readData() throws IOException {
    String encodedBlob = readRaw();
    byte[] blob = Base64.getDecoder().decode(encodedBlob);
    return new String(blob, StandardCharsets.UTF_8);
  }

  private String readRaw() throws IOException {
    _peerSend.flush();
    return _peerRecv.nextLine();
  }

  private void sendData(String data) throws IOException {
    byte[] blob = data.getBytes(StandardCharsets.UTF_8);
    String encodedBlob = Base64.getEncoder().encodeToString(blob);
    sendRaw(encodedBlob);
  }

  private void sendRaw(String data) throws IOException {
    _peerSend.write(data);
    _peerSend.write('\n');
  }

  private int sendMetadata(ResultSetMetaData metadata) throws IOException, SQLException {
    String[] metadataLines = null;
    int columnCount = 0;

    // Metadata errors can only be reported up-front, so build the response first
    columnCount = metadata.getColumnCount();
    metadataLines = new String[2 * columnCount];
    for (int i = 0; i < columnCount; i++) {
      metadataLines[2*i] = metadata.getColumnLabel(i + 1);
      metadataLines[2*i + 1] = metadata.getColumnTypeName(i + 1);
    }

    sendRaw("METADATA");
    sendRaw("" + columnCount);
    for (String line: metadataLines) {
      sendData(line);
    }

    return columnCount;
  }

  private void executeQuery() throws IOException {
    String query = readData();

    Statement stmt = null;
    ResultSet rs = null;

    try {
      stmt = _cnx.createStatement();
      stmt.execute(query);

      rs = stmt.getResultSet();
      int affected = stmt.getUpdateCount();

      if (rs == null) {
        sendRaw("AFFECTED");
        sendRaw("" + affected);
        return;
      }

      int columnCount = sendMetadata(rs.getMetaData());
      String[] rowdataLines = new String[columnCount];

      int pageLeft = 25;
      while (rs.next()) {
        if (pageLeft == 0) {
          sendRaw("PAGE");
          String response = readRaw();
          if (response.equals("MORE")) {
            pageLeft = 25;
          } else if (response.equals("ABORT")) {
            break;
          }
        }

        // Same as before, row errors can only be emitted on a per-row basis
        pageLeft--;
        for (int i = 0; i < columnCount; i++) {
          Object value = rs.getObject(i + 1);

          if (value == null) {
            rowdataLines[i] = "<null>";
          } else if (value instanceof byte[]) {
            rowdataLines[i] = Base64.getEncoder().encodeToString((byte[]) value);
          } else {
            rowdataLines[i] = value.toString();
          }
        }

        sendRaw("ROW");
        for (String line: rowdataLines) {
          sendData(line);
        }
      }

      sendRaw("END");
    } catch (SQLException err) {
      sendRaw("ERROR");
      sendData(err.getMessage());
    } finally {
      if (stmt != null) try { stmt.close(); } catch (SQLException ignore) { }
      if (rs != null) try { rs.close(); } catch (SQLException ignore) { }
    }
  }

  private void prepareQuery() throws IOException {
    String query = readData();

    PreparedStatement stmt = null;
    try {
      stmt = _cnx.prepareStatement(query);
      sendMetadata(stmt.getMetaData());
    } catch (SQLException err) {
      sendRaw("ERROR");
      sendData("Could not prepare: " + err.getMessage());
    } finally {
      if (stmt != null) try { stmt.close(); } catch (SQLException ignore) { }
    }
  }

  public void start() throws IOException, SQLException {
    Socket peer = null;
    try {
      peer = new Socket(_host, _port);
      _peerRecv = new Scanner(new InputStreamReader(peer.getInputStream(), StandardCharsets.US_ASCII));
      _peerSend = new BufferedWriter(new OutputStreamWriter(peer.getOutputStream(), StandardCharsets.US_ASCII));

      DatabaseMetaData dbmd = _cnx.getMetaData();
      String identity = dbmd.getDriverName() + " " + dbmd.getDriverVersion();
      sendRaw("HELLO");
      sendData(identity);
      _peerSend.flush();

      while (true) {
        String command;
        try {
          command = readRaw();

          if (command.equals("EXECUTE")) {
            executeQuery();
          } else if (command.equals("PREPARE")) {
            prepareQuery();
          } else {
            break;
          }
        } catch (NoSuchElementException err) {
          break;
        }
      }
    } finally {
      if (peer != null) peer.close();
    }
  }
}
