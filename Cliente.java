import java.net.*;
import java.util.*;
import java.io.*;
 
public class Cliente {
    private List<InetSocketAddress> servidores; // Lista de servidores
    private Map<String, Pair<String, Integer>> hashTable; // HashMap para armazenar chaves e seus valores com timestamp
    private boolean inicializado; // Indica se o cliente foi inicializado
    private Random random; // Para escolher servidores aleatórios
    private int portaCliente; // Porta fixa do cliente para escutar respostas

    // Armazenar par de chave e timestamp
    private class Pair<F, S> {
        public F first; // Valor da chave
        public S second; // Timestamp da chave

        // Construtor para inicializar o par
        public Pair(F first, S second) {
            this.first = first;
            this.second = second;
        }
    }

    // LISTENER: Thread que escuta respostas dos servidores
    private class ListenerThread extends Thread {
        private int porta;

        public ListenerThread(int porta) {
            this.porta = porta;
        }

        @Override 
        // Método run que executa a lógica de escuta
        public void run() {
            try (ServerSocket serverSocket = new ServerSocket(porta)) {
                System.out.println("Listener do cliente rodando na porta " + porta);

                while (true) { 
                    Socket socket = serverSocket.accept();
                    ObjectInputStream in = new ObjectInputStream(socket.getInputStream());
                    Mensagem msg = (Mensagem) in.readObject();

                    String key = msg.getKey();
                    int tsServidor = msg.getTimestamp();
                    
                    switch (msg.getType()) { 
                        case "PUT_OK":
                            // Atualiza timestamp local
                            if (hashTable.containsKey(key)) {
                                Pair<String, Integer> entry = hashTable.get(key);
                                hashTable.put(key, new Pair<>(entry.first, tsServidor));
                            }
                            System.out.println("PUT_OK key: [" + key + "] value [" +
                                    hashTable.get(key).first + "] timestamp [" + tsServidor +
                                    "] realizada no servidor [" + msg.getIp() + ":" + msg.getPorta() + "]");
                            break;

                        case "GET":
                            // Atualiza value e timestamp
                            hashTable.put(key, new Pair<>(msg.getValue(), tsServidor));
                            System.out.println("GET key: [" + key + "] value: [" + msg.getValue() +
                                    "] obtido do servidor [" + msg.getIp() + ":" + msg.getPorta() +
                                    "], meu timestamp [" + hashTable.get(key).second +
                                    "] e do servidor [" + tsServidor + "]");
                            break;

                        case "WAIT_FOR_RESPONSE":
                            System.out.println("GET key: [" + key + "] value: [WAIT_FOR_RESPONSE] obtido do servidor [" +
                                    msg.getIp() + ":" + msg.getPorta() + "], meu timestamp [" +
                                    hashTable.get(key).second + "] e do servidor [" + tsServidor + "]");
                            break;
                    }

                    in.close();
                    socket.close();
                }
            }
            catch (IOException | ClassNotFoundException e) {
                System.out.println("Erro no listener do cliente: " + e.getMessage());
            }
        }
    }

    // Construtor do Cliente: Inicializa a lista de servidores, hashTable, estado, random e recebe a porta do cliente para escutar respostas
    public Cliente(int portaCliente) {
        servidores = new ArrayList<>(); // Lista de servidores
        hashTable = new HashMap<>(); // Armazenar value + timestamp 
        inicializado = false; // Indica se o cliente foi inicializado
        random = new Random(); // Para escolher servidores aleatórios
        this.portaCliente = portaCliente; // Porta fixa do cliente para escutar respostas
    }
    
    // Método para inicializar o cliente
    public void init() {
        @SuppressWarnings("resource")
        Scanner scanner = new Scanner(System.in);

        // Permite escolher entre IPs/portas padrão ou inserção manual
        System.out.println("Selecione:\n1 - Usar IPs/portas padrão\n2 - Inserir manualmente");
        int opcao = Integer.parseInt(scanner.nextLine());

        // Limpa a lista de servidores
        servidores.clear();

        // Inicializa a lista de servidores com IPs e portas
        if (opcao == 1) { // Usar IPs/portas padrão
            for (int i = 0; i < 3; i++) {
                servidores.add(new InetSocketAddress("127.0.0.1", 10097 + i));
            }
        } 
        else if (opcao == 2) { // Inserção manual de IPs e portas
            for (int i = 1; i <= 3; i++) {
                System.out.println("Digite o IP do servidor " + i + ": ");
                String ip = scanner.nextLine();
                System.out.println("Digite a porta do servidor " + i + ": ");
                int porta = Integer.parseInt(scanner.nextLine());
                servidores.add(new InetSocketAddress(ip, porta));
            }
        } 
        else { // Opção inválida
            System.out.println("Opção inválida.");
            return;
        }

        inicializado = true; 
        System.out.println("Cliente inicializado com sucesso.");
    }

    // PUT
    // PUT corrigido:
    public void put(String key, String value) throws IOException, ClassNotFoundException {
        hashTable.put(key, new Pair<>(value, 0));
        Mensagem mensagem = new Mensagem("PUT", key, value, 0, portaCliente);
        mensagem.setIp("127.0.0.1");

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
            } else if ("FORWARDED".equals(resposta.getType())) {
                System.out.println("PUT encaminhado ao líder. Aguardando PUT_OK via ListenerThread...");
            }
        }
    }

    // GET
    public void get(String key) throws IOException, ClassNotFoundException {
        int timestamp = hashTable.getOrDefault(key, new Pair<>(null, 0)).second;
        Mensagem mensagem = new Mensagem("GET", key, null, timestamp, portaCliente);
        mensagem.setIp("127.0.0.1");

        InetSocketAddress servidor = servidores.get(random.nextInt(servidores.size()));
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

    // Método main para executar o cliente
    public static void main(String[] args) throws IOException {
        @SuppressWarnings("resource")
        Scanner scanner = new Scanner(System.in);

        System.out.println("Qual porta você deseja usar para receber as respostas?");
        int portaCliente = Integer.parseInt(scanner.nextLine()); 

        Cliente cliente = new Cliente(portaCliente); 
        ListenerThread listener = cliente.new ListenerThread(portaCliente);
        listener.start(); 
        
        // Loop para interagir com o usuário
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
                e.printStackTrace();
            }
        }
    }
}