import java.io.*;
import java.net.*;
import java.util.*;

public class Servidor {
    private Map<String, Pair<String, Integer>> hashTable; // Tabela chave-valor com timestamp
    private Map<String, Pair<Mensagem, Integer>> pendentes; // Controla PUTs pendentes de confirmação
    private Map<String, List<Pair<String, Integer>>> waiters; // Controla clientes esperando resposta do GET
    private InetSocketAddress enderecoLider; // Endereço do líder
    private InetSocketAddress meuEndereco; // Endereço deste servidor
    private boolean souLider; // Flag se sou o líder
    private int porta; // Porta do servidor
    private ServerSocket serverSocket; // Socket para aceitar conexões

    private static class Pair<V, T> {
        public V value;
        public T timestamp;
        public Pair(V v, T t) {
            this.value = v;
            this.timestamp = t;
        }
    }

    // Encaminhar PUT para líder
    private void encaminharParaLider(Mensagem msg) {
        try (
            Socket socket = new Socket(enderecoLider.getAddress(), enderecoLider.getPort());
            ObjectOutputStream outLider = new ObjectOutputStream(socket.getOutputStream())
        ) {
            outLider.writeObject(msg);
            outLider.flush();
            Thread.sleep(100); // Pequena pausa para garantir que o líder receba a mensagem
            socket.close();
            System.out.println("PUT encaminhado ao líder, resposta será enviada pelo próprio líder ao cliente.");
        } catch (Exception e) {
            System.out.println("Erro ao encaminhar PUT para líder: " + e.getMessage());
        }
    }

    // Envia mensagem de replicação para os seguidores e aguarda REPLICATION_OK
    private void enviarParaSeguidores(Mensagem replicationMsg) {
        for (int i = 0; i < 3; i++) {
            int portaServidor = 10097 + i;
            if (portaServidor != porta) { // não envia para si mesmo
                try (Socket s = new Socket("127.0.0.1", portaServidor);
                    ObjectOutputStream out = new ObjectOutputStream(s.getOutputStream());
                    ObjectInputStream in = new ObjectInputStream(s.getInputStream())) {

                    out.writeObject(replicationMsg);
                    out.flush();

                    // Lê REPLICATION_OK
                    Mensagem ack = (Mensagem) in.readObject();
                    if ("REPLICATION_OK".equals(ack.getType())) {
                        System.out.println("Recebido REPLICATION_OK do servidor " + portaServidor + " para key:[" + replicationMsg.getKey() + "]");
                    } else {
                        System.out.println("Resposta inesperada do servidor " + portaServidor);
                    }

                } catch (IOException | ClassNotFoundException e) {
                    System.out.println("Erro ao enviar REPLICATION para porta " + portaServidor + ": " + e.getMessage());
                }
            }
        }
        System.out.println("REPLICATION key:[" + replicationMsg.getKey() + "] value:[" + replicationMsg.getValue() + "] ts:[" + replicationMsg.getTimestamp() + "]");
    }

    // Trata requisição PUT
    // Se não for líder, encaminha para o líder; se for líder, processa e replica
    private void tratarPUT(Mensagem msg, ObjectOutputStream out) throws IOException {
        String key = msg.getKey();
        String value = msg.getValue();

        if (!souLider) {
            System.out.println("Encaminhando PUT key:[" + key + "] value:[" + value + "]");
            encaminharParaLider(msg);
            Mensagem forwarded = new Mensagem("FORWARDED", key, value, 0, porta);
            forwarded.setIp(meuEndereco.getAddress().getHostAddress());
            out.writeObject(forwarded);
            out.flush();
            System.out.println("Informando cliente que PUT foi encaminhado ao líder.");
            return;
        }

        System.out.println("Cliente [" + msg.getIp() + "]:" + msg.getPorta() + " PUT key:[" + key + "] value:[" + value + "].");

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
        System.out.println("Enviando PUT_OK ao Cliente [" + msg.getIp() + "]:" + msg.getPorta() + " da key:[" + key + "] ts:[" + novoTimestamp + "]");
    }


    // Trata requisição GET
    // Verifica se a chave existe e se o timestamp é válido
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

        System.out.println("Cliente [" + msg.getIp() + "]:" + msg.getPorta() + " GET key:[" + key + "] ts:[" + tsCliente + "]. Meu ts é [" + tsServidor + "]");

        if (tsServidor >= tsCliente) {
            Mensagem resposta = new Mensagem("GET_RETURN", key, value, tsServidor, porta);
            resposta.setIp(meuEndereco.getAddress().getHostAddress());
            out.writeObject(resposta);
            out.flush();
            System.out.println("Devolvendo valor:[" + value + "]");
        } else {
            Mensagem resposta = new Mensagem("WAIT_FOR_RESPONSE", key, null, tsServidor, porta);
            resposta.setIp(meuEndereco.getAddress().getHostAddress());
            out.writeObject(resposta);
            out.flush();
            System.out.println("Timestamp menor, devolvendo WAIT_FOR_RESPONSE.");
            synchronized (waiters) {
                waiters.putIfAbsent(key, new ArrayList<>());
                waiters.get(key).add(new Pair<>(msg.getIp(), msg.getPorta()));
            }
        }
    }

    // Trata REPLICATION recebida
    // Atualiza a tabela hash e notifica clientes esperando por GET
    private void tratarREPLICATION(Mensagem msg, ObjectOutputStream out) throws IOException {
        String key = msg.getKey();
        String value = msg.getValue();
        int timestamp = msg.getTimestamp();

        synchronized (hashTable) {
            hashTable.put(key, new Pair<>(value, timestamp));
        }

        System.out.println("REPLICATION key:[" + key + "] value:[" + value + "] ts:[" + timestamp + "].");

        Mensagem ack = new Mensagem("REPLICATION_OK", key, null, timestamp, porta);
        ack.setIp(meuEndereco.getAddress().getHostAddress());
        out.writeObject(ack);
        out.flush();

        // Verifica se há clientes esperando por esta chave
        synchronized (waiters) { 
            if (waiters.containsKey(key)) {
                List<Pair<String, Integer>> clientes = waiters.get(key);
                Iterator<Pair<String, Integer>> it = clientes.iterator();
                while (it.hasNext()) {
                    Pair<String, Integer> cliente = it.next();
                    String clienteIp = cliente.value;
                    int clientePorta = cliente.timestamp;
                    try {
                        Socket s = new Socket(clienteIp, clientePorta);
                        ObjectOutputStream outCliente = new ObjectOutputStream(s.getOutputStream());
                        Mensagem resposta = new Mensagem("GET", key, value, timestamp, porta);
                        resposta.setIp(meuEndereco.getAddress().getHostAddress());
                        outCliente.writeObject(resposta);
                        outCliente.flush();
                        outCliente.close();
                        s.close();
                        System.out.println("Cliente [" + clienteIp + "]:" + clientePorta + " GET key:[" + key + "] ts:[" + timestamp + "] atualizado, enviando resposta.");
                        it.remove();
                    } catch (IOException e) {
                        System.out.println("Erro ao enviar resposta atrasada para cliente [" + clienteIp + "]:" + clientePorta);
                    }
                }
                if (clientes.isEmpty()) {
                    waiters.remove(key);
                }
            }
        }
    }

    // Tratar REPLICATION_OK
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
                System.out.println("Enviando PUT_OK ao Cliente [" + originalPUT.getIp() + "]:" + originalPUT.getPorta() + " da key:[" + key + "] ts:[" + hashTable.get(key).timestamp + "]");
                pendentes.remove(key);
            } else {
                pendentes.put(key, new Pair<>(par.value, restante));
            }
        }
    }
    
    // Inicialização: captura IP e porta deste servidor e do líder
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
            System.out.println("Servidor iniciado na porta " + porta + (souLider ? " (Líder)" : ""));

        } catch (IOException e) {
            System.out.println("Erro na inicialização: " + e.getMessage());
            System.exit(1);
        }
    }

    // Recebe requisições concorrentes de clientes e servidores
    public void receiveRequests() {
        new Thread(() -> {
            while (true) {
                try {
                    Socket socket = serverSocket.accept();
                    new Thread(new Handler(socket)).start();
                } catch (IOException e) {
                    System.out.println("Erro ao aceitar conexão: " + e.getMessage());
                }
            }
        }).start();
    }

    // Thread que trata uma requisição recebida
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
                Thread.sleep(100); // Pequena pausa para evitar sobrecarga
                socket.close();
            } catch (Exception e) {
                System.out.println("Erro ao tratar requisição: " + e.getMessage());
            }
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