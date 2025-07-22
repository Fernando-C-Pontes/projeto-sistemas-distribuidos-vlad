import java.net.*;
import java.util.*;
import java.io.*;
 
public class Cliente {
    private List<InetSocketAddress> servidores;
    private Map<String, Pair<String, Integer>> hashTable;
    private boolean inicializado;
    private Random random;
    private int portaCliente;

    private class Pair<F, S> {
        public F first;
        public S second;
        public Pair(F first, S second) {
            this.first = first;
            this.second = second;
        }
    }

    private class ListenerThread extends Thread {
        private int porta;

        public ListenerThread(int porta) {
            this.porta = porta;
        }

        @Override 
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(porta)) {
                while (true) { 
                    Socket socket = serverSocket.accept();
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    Mensagem msg = (Mensagem) in.readObject();

                    String key = msg.getKey();
                    int tsServidor = msg.getTimestamp();

                    switch (msg.getType()) { 
                        case "PUT_OK":
                            hashTable.put(key, new Pair<>(msg.getValue(), tsServidor));
                            System.out.println("PUT_OK key: [" + key + "] value [" +
                                    hashTable.get(key).first + "] timestamp [" + tsServidor +
                                    "] realizada no servidor [" + msg.getIp() + ":" + msg.getPorta() + "]");
                            break;

                        case "GET":
                            hashTable.put(key, new Pair<>(msg.getValue(), tsServidor));
                            System.out.println("GET key: [" + key + "] value: [" + msg.getValue() +
                                    "] obtido do servidor [" + msg.getIp() + ":" + msg.getPorta() +
                                    "], meu timestamp [" + hashTable.get(key).second +
                                    "] e do servidor [" + tsServidor + "]");
                            break;

                        case "WAIT_FOR_RESPONSE":
                            System.out.println("GET key: [" + key + "] value: [WAIT_FOR_RESPONSE] obtido do servidor [" +
                                    msg.getIp() + ":" + msg.getPorta() + "], meu timestamp [" +
                                    hashTable.getOrDefault(key, new Pair<>(null, 0)).second + "] e do servidor [" + tsServidor + "]");
                            break;
                    }

                    in.close();
                    socket.close();
                }
            } catch (IOException | ClassNotFoundException e) {}
        }
    }

    public Cliente(int portaCliente) {
        servidores = new ArrayList<>();
        hashTable = new HashMap<>();
        inicializado = false;
        random = new Random();
        this.portaCliente = portaCliente;
    }
    
    public void init() {
        @SuppressWarnings("resource")
        Scanner scanner = new Scanner(System.in);
        System.out.println("Selecione:\n1 - Usar IPs/portas padrão\n2 - Inserir manualmente");
        int opcao = Integer.parseInt(scanner.nextLine());

        servidores.clear();

        if (opcao == 1) {
            for (int i = 0; i < 3; i++) {
                servidores.add(new InetSocketAddress("127.0.0.1", 10097 + i));
            }
        } else if (opcao == 2) {
            for (int i = 1; i <= 3; i++) {
                System.out.println("Digite o IP do servidor " + i + ": ");
                String ip = scanner.nextLine();
                System.out.println("Digite a porta do servidor " + i + ": ");
                int porta = Integer.parseInt(scanner.nextLine());
                servidores.add(new InetSocketAddress(ip, porta));
            }
        } else {
            System.out.println("Opção inválida.");
            return;
        }

        inicializado = true; 
    }

    public void put(String key, String value) throws IOException, ClassNotFoundException {
        hashTable.put(key, new Pair<>(value, 0));
        Mensagem mensagem = new Mensagem("PUT", key, value, 0, portaCliente);
        mensagem.setIp("127.0.0.1");
        mensagem.setPorta(portaCliente);
        
        InetSocketAddress servidor = servidores.get(random.nextInt(servidores.size()));
        try (Socket socket = new Socket(servidor.getAddress(), servidor.getPort());
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeObject(mensagem);
            out.flush();

            Mensagem resposta = (Mensagem) in.readObject();
            if ("PUT_OK".equals(resposta.getType())) {
                hashTable.put(key, new Pair<>(value, resposta.getTimestamp()));
                System.out.println("PUT_OK key: [" + key + "] value [" + value + "] timestamp [" + resposta.getTimestamp() + "] realizada no servidor [" + resposta.getIp() + ":" + resposta.getPorta() + "]");
            }
        }
    }

    public void get(String key) throws IOException, ClassNotFoundException {
        int timestamp = hashTable.getOrDefault(key, new Pair<>(null, 0)).second;
        Mensagem mensagem = new Mensagem("GET", key, null, timestamp, portaCliente);
        mensagem.setIp("127.0.0.1");

        // Forçar o GET sempre no líder (servidor[0])
        InetSocketAddress servidor = servidores.get(0);

        try (Socket socket = new Socket(servidor.getAddress(), servidor.getPort());
            ObjectOutputStream out = new ObjectOutputStream(socket.getOutputStream());
            ObjectInputStream in = new ObjectInputStream(socket.getInputStream())) {

            out.writeObject(mensagem);
            out.flush();

            Mensagem resposta = (Mensagem) in.readObject();
            if ("GET_RETURN".equals(resposta.getType())) {
                hashTable.put(key, new Pair<>(resposta.getValue(), resposta.getTimestamp()));
                System.out.println("GET key: [" + key + "] value: [" + resposta.getValue() + "] obtido do servidor [" + resposta.getIp() + ":" + resposta.getPorta() + "], meu timestamp [" + hashTable.get(key).second + "] e do servidor [" + resposta.getTimestamp() + "]");
            } else if ("WAIT_FOR_RESPONSE".equals(resposta.getType())) {
                System.out.println("GET key: [" + key + "] value: [WAIT_FOR_RESPONSE] obtido do servidor [" + resposta.getIp() + ":" + resposta.getPorta() + "], meu timestamp [" + timestamp + "] e do servidor [" + resposta.getTimestamp() + "]");
            }
        }
    }

    public static void main(String[] args) throws IOException {
        @SuppressWarnings("resource")
        Scanner scanner = new Scanner(System.in);

        System.out.println("Qual porta você deseja usar para receber as respostas?");
        int portaCliente = Integer.parseInt(scanner.nextLine()); 

        Cliente cliente = new Cliente(portaCliente); 
        ListenerThread listener = cliente.new ListenerThread(portaCliente);
        listener.start(); 
        
        while (true) {
            try {
                System.out.println("O que deseja fazer?\n1 - INIT\n2 - PUT\n3 - GET");
                int opcao = Integer.parseInt(scanner.nextLine());

                if (opcao == 1) {
                    cliente.init();
                } else if (opcao == 2) {
                    if (!cliente.inicializado) {
                        System.out.println("Necessário inicializar o cliente para realizar PUT.");
                        continue;
                    }
                    System.out.println("Digite a key:");
                    String key = scanner.nextLine();
                    System.out.println("Digite o value:");
                    String value = scanner.nextLine();
                    cliente.put(key, value);
                } else if (opcao == 3) {
                    if (!cliente.inicializado) {
                        System.out.println("Necessário inicializar o cliente para realizar GET.");
                        continue;
                    }
                    System.out.println("Digite a key:");
                    String key = scanner.nextLine();
                    cliente.get(key);
                } else {
                    System.out.println("Opção inválida.");
                }
            } catch (Exception e) {
                System.out.println("Erro: " + e.getMessage());
            }
        }
    }
}