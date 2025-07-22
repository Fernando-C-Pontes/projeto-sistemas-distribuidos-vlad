import java.io.*;
import java.net.*;
import java.util.*;

public class Servidor {
    private Map<String, Pair<String, Integer>> hashTable;
    private Map<String, Pair<Mensagem, Integer>> pendentes;
    private Map<String, List<ClienteInfo>> waiters;
    private InetSocketAddress enderecoLider;
    private InetSocketAddress meuEndereco;
    private boolean souLider;
    private int porta;
    private ServerSocket serverSocket;

    private static class Pair<V, T> {
        public V value;
        public T timestamp;
        public Pair(V v, T t) {
            this.value = v;
            this.timestamp = t;
        }
    }

    private static class ClienteInfo {
        public String ip;
        public int porta;

        public ClienteInfo(String ip, int porta) {
            this.ip = ip;
            this.porta = porta;
        }
    }

    private void encaminharParaLider(Mensagem msg) {
        try (
            Socket socket = new Socket(enderecoLider.getAddress(), enderecoLider.getPort());
            ObjectOutputStream outLider = new ObjectOutputStream(socket.getOutputStream())
        ) {
            outLider.writeObject(msg);
            outLider.flush();
            Thread.sleep(100);
            socket.close();
        } catch (Exception e) {}
    }

    private void enviarParaSeguidores(Mensagem replicationMsg) {
        for (int i = 0; i < 3; i++) {
            int portaServidor = 10097 + i;
            if (portaServidor != porta) {
                try (Socket s = new Socket("127.0.0.1", portaServidor);
                    ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {
                    out.writeObject(replicationMsg);
                    out.flush();
                    Mensagem ack = (Mensagem) in.readObject(); 
                } catch (IOException | ClassNotFoundException e) {}
            }
        }
    }

    private void tratarPUT(Mensagem msg, ObjectOutputStream out) throws IOException {
        String key = msg.getKey();
        String value = msg.getValue();

        if (!souLider) {
            encaminharParaLider(msg);
            Mensagem forwarded = new Mensagem("FORWARDED", key, value, 0, porta);
            forwarded.setIp(meuEndereco.getAddress().getHostAddress());
            out.writeObject(forwarded);
            out.flush();
            return;
        }

        int novoTimestamp;
        synchronized (hashTable) {
            if (hashTable.containsKey(key)) {
                novoTimestamp = hashTable.get(key).timestamp + 1;
            } else {
                novoTimestamp = 1;
            }
            hashTable.put(key, new Pair<>(value, novoTimestamp));
        }

        Mensagem replicationMsg = new Mensagem("REPLICATION", key, value, novoTimestamp, porta);
        replicationMsg.setIp(meuEndereco.getAddress().getHostAddress());
        enviarParaSeguidores(replicationMsg);

        pendentes.put(key, new Pair<>(msg, 2));

        Mensagem resposta = new Mensagem("PUT_OK", key, value, novoTimestamp, porta);
        resposta.setIp(meuEndereco.getAddress().getHostAddress());
        out.writeObject(resposta);
        out.flush();
    }

    private void tratarGET(Mensagem msg, ObjectOutputStream out) throws IOException {
        String key = msg.getKey();
        int tsCliente = msg.getTimestamp();

        int tsServidor;
        String value;
        synchronized (hashTable) {
            if (hashTable.containsKey(key)) {
                Pair<String, Integer> par = hashTable.get(key);
                tsServidor = par.timestamp;
                value = par.value;
            } else {
                tsServidor = 0;
                value = null;
            }
        }

        if (value != null && tsServidor >= tsCliente) {
            Mensagem resposta = new Mensagem("GET_RETURN", key, value, tsServidor, porta);
            resposta.setIp(meuEndereco.getAddress().getHostAddress());
            out.writeObject(resposta);
            out.flush();
        } else {
            Mensagem resposta = new Mensagem("WAIT_FOR_RESPONSE", key, null, tsServidor, porta);
            resposta.setIp(meuEndereco.getAddress().getHostAddress());
            out.writeObject(resposta);
            out.flush();
            synchronized (waiters) {
                waiters.putIfAbsent(key, new ArrayList<>());
                waiters.get(key).add(new ClienteInfo(msg.getIp(), msg.getPorta()));
            }
        }
    }

    private void tratarREPLICATION(Mensagem msg, ObjectOutputStream out) throws IOException {
        String key = msg.getKey();
        String value = msg.getValue();
        int timestamp = msg.getTimestamp();

        synchronized (hashTable) {
            hashTable.put(key, new Pair<>(value, timestamp));
        }

        Mensagem ack = new Mensagem("REPLICATION_OK", key, null, timestamp, porta);
        ack.setIp(meuEndereco.getAddress().getHostAddress());
        out.writeObject(ack);
        out.flush();

        synchronized (waiters) {
            if (waiters.containsKey(key)) {
                List<ClienteInfo> clientes = waiters.get(key);
                Iterator<ClienteInfo> it = clientes.iterator();
                while (it.hasNext()) {
                    ClienteInfo cliente = it.next();
                    String clienteIp = cliente.ip;
                    int clientePorta = cliente.porta;
                    try {
                        Socket s = new Socket(clienteIp, clientePorta);
                        ObjectOutputStream outCliente = new ObjectOutputStream(s.getOutputStream());
                        Mensagem resposta = new Mensagem("GET_RETURN", key, value, timestamp, porta);
                        resposta.setIp(meuEndereco.getAddress().getHostAddress());
                        outCliente.writeObject(resposta);
                        outCliente.flush();
                        outCliente.close();
                        s.close();
                        it.remove();
                    } catch (IOException e) {}
                }
                if (clientes.isEmpty()) {
                    waiters.remove(key);
                }
            }
        }
    }

    private synchronized void tratarREPLICATION_OK(Mensagem msg) throws IOException {
        String key = msg.getKey();
        if (pendentes.containsKey(key)) {
            Pair<Mensagem, Integer> par = pendentes.get(key);
            int restante = par.timestamp - 1;
            if (restante <= 0) {
                Mensagem originalPUT = par.value;
                Mensagem resposta = new Mensagem("PUT_OK", key, hashTable.get(key).value, hashTable.get(key).timestamp, porta);
                resposta.setIp(meuEndereco.getAddress().getHostAddress());
                Socket socket = new Socket(originalPUT.getIp(), originalPUT.getPorta());
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                out.writeObject(resposta);
                out.flush();
                out.close();
                socket.close();
                pendentes.remove(key);
            } else {
                pendentes.put(key, new Pair<>(par.value, restante));
            }
        }
    }

    public void init() {
        try {
            @SuppressWarnings("resource")
            Scanner scanner = new Scanner(System.in);

            System.out.println("Digite o IP deste servidor:");
            String meuIp = scanner.nextLine();
            System.out.println("Digite a porta deste servidor:");
            porta = Integer.parseInt(scanner.nextLine());
            meuEndereco = new InetSocketAddress(meuIp, porta);

            System.out.println("Digite o IP do líder:");
            String ipLider = scanner.nextLine();
            System.out.println("Digite a porta do líder:");
            int portaLider = Integer.parseInt(scanner.nextLine());
            enderecoLider = new InetSocketAddress(ipLider, portaLider);

            souLider = meuEndereco.equals(enderecoLider);
            hashTable = new HashMap<>();
            pendentes = new HashMap<>();
            waiters = new HashMap<>();

            serverSocket = new ServerSocket(porta);
        } catch (IOException e) {
            System.exit(1);
        }
    }

    public void receiveRequests() {
        new Thread(() -> {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    new Thread(new Handler(socket)).start();
                } catch (IOException e) {}
            }
        }).start();
    }

    private class Handler implements Runnable {
        private Socket socket;

        public Handler(Socket socket) {
            this.socket = socket;
        }

        public void run() {
            try {
                ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
                Mensagem msg = (Mensagem) in.readObject();

                switch (msg.getType()) {
                    case "PUT":
                        tratarPUT(msg, out);
                        break;
                    case "GET":
                        tratarGET(msg, out);
                        break;
                    case "REPLICATION":
                        tratarREPLICATION(msg, out);
                        break;
                    case "REPLICATION_OK":
                        tratarREPLICATION_OK(msg);
                        break;
                }
                in.close();
                out.close();
                Thread.sleep(100);
                socket.close();
            } catch (Exception e) {}
        }
    }

    public void execRequests() {
        System.out.println("Servidor pronto para receber requisições...");
    }

    public static void main(String[] args) {
        Servidor servidor = new Servidor();
        servidor.init();
        servidor.receiveRequests();
        servidor.execRequests();
    }
}